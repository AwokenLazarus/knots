// Copyright (c) 2026 The Eddy developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#include <swiftsync.h>

#include <chain.h>
#include <crypto/common.h>
#include <hash.h>
#include <logging.h>
#include <node/blockstorage.h>
#include <primitives/block.h>
#include <random.h>
#include <serialize.h>
#include <streams.h>
#include <tinyformat.h>
#include <util/fs_helpers.h>
#include <util/signalinterrupt.h>
#include <util/time.h>
#include <validation.h>

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <unordered_map>

namespace swiftsync {

std::unique_ptr<SwiftSync> g_swiftsync;

// ---- hints file ----------------------------------------------------------------------------
//
// magic "XBTSWFT1" | uint32 version | int32 terminal height H | uint256 block hash at H |
// for each height 0..H: CompactSize(count) then count CompactSize gaps, where the first gap is the
// first unspent output index and each later gap is (index - previous index - 1). Output indices
// run over every output of every transaction of the block, in order.

std::optional<std::string> Hints::Load(const fs::path& path)
{
    AutoFile file{fsbridge::fopen(path, "rb")};
    if (file.IsNull()) return strprintf("cannot open hints file %s", fs::PathToString(path));
    try {
        char magic[8];
        file.read(MakeWritableByteSpan(magic));
        if (std::memcmp(magic, HINTS_MAGIC, sizeof(magic)) != 0) return "not a SwiftSync hints file";
        uint32_t version;
        file >> version;
        if (version != HINTS_VERSION) return strprintf("unsupported hints file version %u", version);
        int32_t height;
        file >> height >> m_terminal_hash;
        if (height < 1) return "hints file has no blocks";
        // Every height takes at least one byte, so a height larger than the file is a lie; checking
        // first keeps a crafted header from making us allocate gigabytes.
        const int64_t size{int64_t(fs::file_size(path))};
        if (height > size) return "hints file claims more blocks than it holds";
        m_offsets.resize(height + 1);
        for (int h = 0; h <= height; ++h) {
            m_offsets[h] = file.tell();
            const uint64_t count = ReadCompactSize(file, /*range_check=*/false);
            for (uint64_t i = 0; i < count; ++i) ReadCompactSize(file, /*range_check=*/false);
        }
        m_height = height;
    } catch (const std::exception& e) {
        return strprintf("hints file is truncated or corrupt (%s)", e.what());
    }
    m_path = path;
    return std::nullopt;
}

std::optional<std::vector<uint32_t>> Hints::Unspent(int height, uint32_t outputs) const
{
    std::vector<uint32_t> out;
    if (height < 0 || height > m_height) return out;
    if (!m_file) {
        m_file = std::make_shared<AutoFile>(fsbridge::fopen(m_path, "rb"));
        if (m_file->IsNull()) {
            m_file.reset();
            throw std::runtime_error("SwiftSync hints file disappeared");
        }
    }
    AutoFile& file{*m_file};
    try {
        file.seek(m_offsets[height], SEEK_SET);
        const uint64_t count = ReadCompactSize(file, /*range_check=*/false);
        if (count > outputs) return std::nullopt;
        out.reserve(count);
        int64_t prev = -1;
        for (uint64_t i = 0; i < count; ++i) {
            const uint64_t gap{ReadCompactSize(file, /*range_check=*/false)};
            if (gap >= outputs) return std::nullopt;
            prev += 1 + int64_t(gap);
            if (prev >= int64_t(outputs)) return std::nullopt;
            out.push_back(uint32_t(prev));
        }
    } catch (const std::exception&) {
        return std::nullopt;
    }
    return out;
}

// ---- aggregate -----------------------------------------------------------------------------

arith_uint256 Aggregate::Hash(const COutPoint& outpoint, const Coin& coin) const
{
    HashWriter hw{};
    hw << salt << outpoint << coin;
    return UintToArith256(hw.GetSHA256());
}

// ---- undo store ----------------------------------------------------------------------------

// Undo data waiting to be connected is held in memory: a few hundred small blocks early in the
// chain, a few dozen large ones later (requests stop at MAX_BYTES). Losing it on a crash costs only
// a re-request, so it never touches the disk.

bool UndoStore::Have(const uint256& block_hash) const { return m_undo.contains(block_hash) || m_packed.contains(block_hash); }

bool UndoStore::Read(const uint256& block_hash, CBlockUndo& undo) const
{
    if (auto it{m_undo.find(block_hash)}; it != m_undo.end()) {
        undo = it->second.undo;
        return true;
    }
    auto it{m_packed.find(block_hash)};
    if (it == m_packed.end()) return false;
    std::FILE* f{fsbridge::fopen(*it->second.file, "rb")};
    if (!f) {
        LogWarning("SwiftSync: cannot open undo pack %s\n", fs::PathToString(*it->second.file));
        return false;
    }
    std::vector<std::byte> buf(it->second.len);
    const bool ok{std::fseek(f, int64_t(it->second.offset), SEEK_SET) == 0 &&
                  std::fread(buf.data(), 1, buf.size(), f) == buf.size()};
    std::fclose(f);
    if (!ok) return false;
    try {
        DataStream ds{buf};
        ds >> undo;
        return ds.empty();
    } catch (const std::exception&) {
        return false;
    }
}

bool UndoStore::Write(const uint256& block_hash, const CBlockUndo& undo, int64_t source)
{
    const size_t bytes{GetSerializeSize(undo)};
    if (m_undo.emplace(block_hash, Entry{undo, bytes, source}).second) m_bytes += bytes;
    return true;
}

std::optional<int64_t> UndoStore::Source(const uint256& block_hash) const
{
    auto it{m_undo.find(block_hash)};
    if (it != m_undo.end()) return it->second.source;
    if (m_packed.contains(block_hash)) return SOURCE_RPC;
    return std::nullopt;
}

static constexpr char PACK_MAGIC[8]{'X', 'B', 'T', 'S', 'W', 'U', 'P', '1'};

util::Result<std::vector<UndoStore::PackEntry>> UndoStore::ScanPack(const fs::path& path, int& first, int& last)
{
    std::FILE* f{fsbridge::fopen(path, "rb")};
    if (!f) return util::Error{Untranslated("cannot open the pack file")};
    std::vector<PackEntry> out;
    auto fail{[&](const std::string& why) { std::fclose(f); return util::Error{Untranslated(why)}; }};
    if (std::fseek(f, 0, SEEK_END) != 0) return fail("cannot read the pack file");
    const int64_t size{std::ftell(f)};
    std::rewind(f);
    unsigned char head[24];
    if (size < 24 || std::fread(head, 1, 24, f) != 24 || std::memcmp(head, PACK_MAGIC, 8) != 0) return fail("not an undo pack");
    if (ReadLE32(head + 8) != 1) return fail("unknown undo pack version");
    first = int32_t(ReadLE32(head + 12));
    last = int32_t(ReadLE32(head + 16));
    const uint32_t count{ReadLE32(head + 20)};
    if (first < 0 || last < first) return fail("bad undo pack heights");
    // Each entry takes at least 36 bytes, so a count the file can't hold is refused before reserving.
    if (uint64_t(count) * 36 > uint64_t(size - 24)) return fail("undo pack claims more entries than it holds");
    out.reserve(count);
    uint64_t pos{24};
    for (uint32_t i = 0; i < count; ++i) {
        unsigned char eh[36];
        if (std::fread(eh, 1, 36, f) != 36) return fail("truncated undo pack");
        PackEntry e;
        std::memcpy(e.hash.begin(), eh, 32);
        e.len = ReadLE32(eh + 32);
        e.offset = pos + 36;
        if (e.offset + e.len > uint64_t(size)) return fail("truncated undo pack");
        if (std::fseek(f, int64_t(e.len), SEEK_CUR) != 0) return fail("truncated undo pack");
        pos = e.offset + e.len;
        out.push_back(e);
    }
    std::fclose(f);
    return out;
}

size_t UndoStore::IndexPack(const fs::path& path, const std::vector<PackEntry>& entries, const std::function<bool(const uint256&)>& wanted)
{
    auto file{std::make_shared<const fs::path>(path)};
    size_t n{0};
    for (const auto& e : entries) {
        if (Have(e.hash) || !wanted(e.hash)) continue;
        m_packed.emplace(e.hash, PackRef{file, e.offset, e.len});
        ++n;
    }
    return n;
}

util::Result<size_t> UndoStore::AddPack(const fs::path& path, const std::function<bool(const uint256&)>& wanted)
{
    int first, last;
    auto entries{ScanPack(path, first, last)};
    if (!entries) return util::Error{util::ErrorString(entries)};
    return IndexPack(path, *entries, wanted);
}

void UndoStore::Erase(const uint256& block_hash)
{
    m_packed.erase(block_hash);
    auto it{m_undo.find(block_hash)};
    if (it == m_undo.end()) return;
    m_bytes -= it->second.bytes;
    m_undo.erase(it);
}

size_t UndoStore::EraseIf(const std::function<bool(const uint256&)>& stale)
{
    size_t n{0};
    for (auto it{m_packed.begin()}; it != m_packed.end();) {
        if (stale(it->first)) {
            it = m_packed.erase(it);
            ++n;
        } else {
            ++it;
        }
    }
    for (auto it{m_undo.begin()}; it != m_undo.end();) {
        if (stale(it->first)) {
            m_bytes -= it->second.bytes;
            it = m_undo.erase(it);
            ++n;
        } else {
            ++it;
        }
    }
    return n;
}

void UndoStore::Clear()
{
    m_undo.clear();
    m_packed.clear();
    m_bytes = 0;
}

size_t UndoStore::Count() const { return m_undo.size() + m_packed.size(); }

void SwiftSync::BlameRpc(const uint256& hash) { m_bad_rpc_blocks[hash] = GetTime(); }

bool SwiftSync::RpcBlamed(const uint256& hash) const
{
    auto it{m_bad_rpc_blocks.find(hash)};
    return it != m_bad_rpc_blocks.end() && GetTime() - it->second < RPC_BLAME_SECONDS;
}

// ---- coins view over a block's undo data ---------------------------------------------------

UndoCoinsView::UndoCoinsView(CCoinsView* base, const CBlock& block, const CBlockUndo& undo)
    : CCoinsViewBacked(base)
{
    if (undo.vtxundo.size() + 1 != block.vtx.size()) {
        m_matches = false;
        return;
    }
    // Outputs created in this block and spent later in it are served by the connecting cache, so
    // they stay out of this view (else the BIP30 check would see them as already existing).
    std::unordered_set<uint256, SaltedTxidHasher> created;
    created.reserve(block.vtx.size());
    size_t inputs{0};
    for (const auto& tx : block.vtx) {
        created.insert(tx->GetHash().ToUint256());
        inputs += tx->vin.size();
    }
    m_spent.reserve(inputs);
    for (size_t i = 1; i < block.vtx.size(); ++i) {
        const CTransaction& tx{*block.vtx[i]};
        const CTxUndo& txundo{undo.vtxundo[i - 1]};
        if (txundo.vprevout.size() != tx.vin.size()) {
            m_matches = false;
            return;
        }
        for (size_t j = 0; j < tx.vin.size(); ++j) {
            if (created.contains(tx.vin[j].prevout.hash.ToUint256())) continue;
            // A block spending the same outpoint twice is invalid; let CheckTxInputs see the second
            // spend as missing by keeping only the first.
            m_spent.emplace(tx.vin[j].prevout, txundo.vprevout[j]);
        }
    }
}

std::optional<Coin> UndoCoinsView::GetCoin(const COutPoint& outpoint) const
{
    if (auto it{m_spent.find(outpoint)}; it != m_spent.end()) return it->second;
    return CCoinsViewBacked::GetCoin(outpoint);
}

bool UndoCoinsView::HaveCoin(const COutPoint& outpoint) const
{
    return m_spent.contains(outpoint) || CCoinsViewBacked::HaveCoin(outpoint);
}

// ---- script pre-validation -----------------------------------------------------------------

uint256 UndoHash(const CBlockUndo& undo)
{
    HashWriter hw{};
    hw << undo;
    return hw.GetSHA256();
}

bool Prevalidated::Claim(const uint256& block)
{
    LOCK(m_mutex);
    return m_entries.emplace(block, Entry{State::CHECKING, {}}).second;
}

void Prevalidated::Finish(const uint256& block, bool ok, const uint256& undo)
{
    {
        LOCK(m_mutex);
        auto it{m_entries.find(block)};
        if (it == m_entries.end() || it->second.state != State::CHECKING) return;
        it->second = Entry{ok ? State::PASSED : State::FAILED, undo};
    }
    m_cv.notify_all();
}

bool Prevalidated::Connecting(const uint256& block, const uint256& undo)
{
    WAIT_LOCK(m_mutex, lock);
    auto it{m_entries.find(block)};
    if (it != m_entries.end() && it->second.state == State::CHECKING) {
        ++m_waited;
        // A worker checks one block in well under a minute even on a phone; the bound only keeps a
        // stuck worker from stalling the chain.
        m_cv.wait_for(lock, std::chrono::seconds{60}, [&]() EXCLUSIVE_LOCKS_REQUIRED(m_mutex) {
            auto e{m_entries.find(block)};
            return e == m_entries.end() || e->second.state != State::CHECKING;
        });
        it = m_entries.find(block);
    }
    const bool passed{it != m_entries.end() && it->second.state == State::PASSED && it->second.undo == undo};
    m_entries.insert_or_assign(block, Entry{State::CONNECTING, {}});
    return passed;
}

bool Prevalidated::Wanted(const uint256& block) const
{
    LOCK(m_mutex);
    auto it{m_entries.find(block)};
    return it != m_entries.end() && it->second.state == State::CHECKING;
}

void Prevalidated::Forget(const uint256& block)
{
    LOCK(m_mutex);
    m_entries.erase(block);
}

size_t Prevalidated::Size() const
{
    LOCK(m_mutex);
    return m_entries.size();
}

void SwiftSync::StopPrevalidation()
{
    m_pre_stop = true;
    for (auto& t : m_pre_threads) {
        if (t.joinable()) t.join();
    }
    m_pre_threads.clear();
}

// ---- state ---------------------------------------------------------------------------------

std::vector<unsigned char> SwiftSync::Serialize() const
{
    DataStream ds{};
    ds << uint8_t{1} << m_hints.TerminalHash() << int32_t(m_hints.Height()) << m_agg.salt
       << ArithToUint256(m_agg.sum) << uint8_t(m_done);
    return {UCharCast(ds.data()), UCharCast(ds.data() + ds.size())};
}

bool SwiftSync::Deserialize(const std::vector<unsigned char>& data)
{
    try {
        DataStream ds{data};
        uint8_t version, done;
        uint256 terminal, sum;
        int32_t height;
        ds >> version >> terminal >> height >> m_agg.salt >> sum >> done;
        if (version != 1 || terminal != m_hints.TerminalHash() || height != m_hints.Height()) return false;
        m_agg.sum = UintToArith256(sum);
        m_done = done;
    } catch (const std::exception&) {
        return false;
    }
    return true;
}

bool StateActive(const std::vector<unsigned char>& saved)
{
    // Serialize() ends with the done flag.
    return !saved.empty() && saved.back() == 0;
}

void SwiftSync::NewSalt()
{
    m_agg.salt = GetRandHash();
    m_agg.sum = 0;
}

// ---- hints generation ----------------------------------------------------------------------

std::optional<std::string> WriteHints(Chainstate& chainstate, const fs::path& path, int& height_out, uint256& hash_out)
{
    // 1. A consistent snapshot of the UTXO set at the tip, grouped by the height that created each coin.
    chainstate.ForceFlushStateToDisk();
    std::unique_ptr<CCoinsViewCursor> cursor;
    const CBlockIndex* tip{nullptr};
    {
        LOCK(cs_main);
        cursor = chainstate.CoinsDB().Cursor();
        tip = chainstate.m_chain.Tip();
        if (!tip || tip->GetBlockHash() != cursor->GetBestBlock()) return "the UTXO set is not at the tip; try again";
    }
    const int height{tip->nHeight};
    std::vector<std::vector<COutPoint>> by_height(height + 1);
    size_t coins{0};
    for (; cursor->Valid(); cursor->Next()) {
        COutPoint key;
        Coin coin;
        if (!cursor->GetKey(key) || !cursor->GetValue(coin)) return "UTXO set read failed";
        if (coin.nHeight > uint32_t(height)) return "UTXO set changed during the snapshot";
        by_height[coin.nHeight].push_back(key);
        if (++coins % 10'000'000 == 0) {
            LogInfo("SwiftSync hints: read %u coins\n", coins);
            if (static_cast<bool>(chainstate.m_chainman.m_interrupt)) return "stopped: the node is shutting down";
        }
    }
    cursor.reset();
    LogInfo("SwiftSync hints: %u unspent coins at height %d\n", coins, height);

    // 2. Every block in order: where in its output list each unspent coin sits.
    fs::path tmp{path};
    tmp += ".incomplete";
    AutoFile file{fsbridge::fopen(tmp, "wb")};
    if (file.IsNull()) return strprintf("cannot write %s", fs::PathToString(tmp));
    file.write(std::as_bytes(std::span{HINTS_MAGIC}));
    file << HINTS_VERSION << int32_t(height) << tip->GetBlockHash();
    for (int h = 0; h <= height; ++h) {
        // The node can't finish shutting down while this RPC runs, so give way at once.
        if (h % 1000 == 0 && static_cast<bool>(chainstate.m_chainman.m_interrupt)) {
            file.fclose();
            fs::remove(tmp);
            return "stopped: the node is shutting down";
        }
        std::vector<uint32_t> idx;
        if (!by_height[h].empty()) {
            const CBlockIndex* pindex{tip->GetAncestor(h)};
            CBlock block;
            if (!chainstate.m_blockman.ReadBlock(block, *pindex)) return strprintf("cannot read block %d", h);
            std::unordered_map<Txid, uint32_t, SaltedTxidHasher> first_output;
            uint32_t n{0};
            for (const auto& tx : block.vtx) {
                first_output.emplace(tx->GetHash(), n);
                n += tx->vout.size();
            }
            for (const COutPoint& op : by_height[h]) {
                auto it{first_output.find(op.hash)};
                if (it == first_output.end()) return strprintf("coin %s not in block %d", op.ToString(), h);
                idx.push_back(it->second + op.n);
            }
            std::sort(idx.begin(), idx.end());
            std::vector<COutPoint>().swap(by_height[h]);
        }
        WriteCompactSize(file, idx.size());
        int64_t prev{-1};
        for (uint32_t i : idx) {
            WriteCompactSize(file, uint64_t(int64_t(i) - prev - 1));
            prev = i;
        }
        if (h % 100'000 == 0) LogInfo("SwiftSync hints: block %d of %d\n", h, height);
    }
    if (file.fclose() != 0) return "write failed";
    if (!RenameOver(tmp, path)) return "rename failed";
    height_out = height;
    hash_out = tip->GetBlockHash();
    return std::nullopt;
}

} // namespace swiftsync
