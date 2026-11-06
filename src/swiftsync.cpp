// Copyright (c) 2026 The Eddy developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#include <swiftsync.h>

#include <chain.h>
#include <hash.h>
#include <logging.h>
#include <node/blockstorage.h>
#include <primitives/block.h>
#include <random.h>
#include <serialize.h>
#include <streams.h>
#include <tinyformat.h>
#include <util/fs_helpers.h>
#include <validation.h>

#include <algorithm>
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

std::vector<uint32_t> Hints::Unspent(int height) const
{
    std::vector<uint32_t> out;
    if (height < 0 || height > m_height) return out;
    AutoFile file{fsbridge::fopen(m_path, "rb")};
    if (file.IsNull()) throw std::runtime_error("SwiftSync hints file disappeared");
    file.seek(m_offsets[height], SEEK_SET);
    const uint64_t count = ReadCompactSize(file, /*range_check=*/false);
    out.reserve(count);
    int64_t prev = -1;
    for (uint64_t i = 0; i < count; ++i) {
        prev += 1 + int64_t(ReadCompactSize(file, /*range_check=*/false));
        out.push_back(uint32_t(prev));
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

UndoStore::UndoStore(fs::path dir) : m_dir(std::move(dir))
{
    fs::create_directories(m_dir);
    std::error_code ec;
    for (const auto& e : fs::directory_iterator(m_dir, ec)) {
        const std::string name{fs::PathToString(e.path().filename())};
        if (name.size() != 64 + 5 || !name.ends_with(".undo")) continue;
        if (auto hash{uint256::FromHex(name.substr(0, 64))}) m_have.insert(*hash);
    }
}

fs::path UndoStore::File(const uint256& block_hash) const
{
    return m_dir / fs::u8path(block_hash.GetHex() + ".undo");
}

bool UndoStore::Have(const uint256& block_hash) const { return m_have.contains(block_hash); }

bool UndoStore::Read(const uint256& block_hash, CBlockUndo& undo) const
{
    AutoFile file{fsbridge::fopen(File(block_hash), "rb")};
    if (file.IsNull()) return false;
    try {
        file >> undo;
    } catch (const std::exception&) {
        return false;
    }
    return true;
}

bool UndoStore::Write(const uint256& block_hash, const CBlockUndo& undo)
{
    const fs::path final_path{File(block_hash)};
    fs::path tmp{final_path};
    tmp += ".tmp";
    {
        AutoFile file{fsbridge::fopen(tmp, "wb")};
        if (file.IsNull()) return false;
        file << undo;
        if (file.fclose() != 0) return false;
    }
    if (!RenameOver(tmp, final_path)) return false;
    m_have.insert(block_hash);
    return true;
}

void UndoStore::Erase(const uint256& block_hash)
{
    std::error_code ec;
    fs::remove(File(block_hash), ec);
    m_have.erase(block_hash);
}

void UndoStore::Clear()
{
    std::error_code ec;
    fs::remove_all(m_dir, ec);
    fs::create_directories(m_dir);
    m_have.clear();
}

size_t UndoStore::Count() const { return m_have.size(); }

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
    std::set<Txid> created;
    for (const auto& tx : block.vtx) created.insert(tx->GetHash());
    for (size_t i = 1; i < block.vtx.size(); ++i) {
        const CTransaction& tx{*block.vtx[i]};
        const CTxUndo& txundo{undo.vtxundo[i - 1]};
        if (txundo.vprevout.size() != tx.vin.size()) {
            m_matches = false;
            return;
        }
        for (size_t j = 0; j < tx.vin.size(); ++j) {
            if (created.contains(tx.vin[j].prevout.hash)) continue;
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
        if (++coins % 10'000'000 == 0) LogInfo("SwiftSync hints: read %u coins\n", coins);
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
