// Copyright (c) 2026 The Eddy developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#ifndef BITCOIN_SWIFTSYNC_H
#define BITCOIN_SWIFTSYNC_H

// SwiftSync: a faster first sync that still validates everything.
//
// A hints file says, for every block up to a terminal height H, which of its outputs are still
// unspent at H. Below H the node adds only those outputs to its UTXO set. Every other output is
// folded into a salted aggregate (added when created), and every input is folded out of it
// (subtracted when spent). The coins an input spends come from the block's undo data, which a
// peer serves ("swundo"). Scripts, amounts, maturity, sequence locks and sigops are all checked
// against those coins exactly as usual, so signature checking is unchanged.
//
// At H the aggregate must be zero. That proves every served coin was really created and spent
// exactly once, and that every hint was right. A wrong hints file or bad undo data can only make
// the sync fail and restart; it cannot make the node accept an invalid chain.
//
// What it saves: below H the UTXO database never sees the ~95% of outputs that are spent before
// H, so there are no lookups, writes or cache flushes for them. That was the slow, disk-bound
// part of a sync on a phone.
//
// Design after Ruben Somsen's SwiftSync write-up and the draft BIPs 455-457. The hints and undo
// wire formats here are this prototype's own, not those drafts'.

#include <arith_uint256.h>
#include <coins.h>
#include <primitives/transaction.h>
#include <streams.h>
#include <sync.h>
#include <uint256.h>
#include <undo.h>
#include <util/fs.h>
#include <util/hasher.h>
#include <util/result.h>
#include <util/time.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

class CBlock;
class CBlockIndex;
class Chainstate;

namespace swiftsync {

static constexpr char HINTS_MAGIC[8]{'X', 'B', 'T', 'S', 'W', 'F', 'T', '1'};
static constexpr uint32_t HINTS_VERSION{1};

/** The unspent-output hints, read once to index each block's position in the file. */
class Hints
{
public:
    /** Open and index a hints file. Returns an error message on failure. */
    std::optional<std::string> Load(const fs::path& path);

    int Height() const { return m_height; }
    const uint256& TerminalHash() const { return m_terminal_hash; }
    const fs::path& Path() const { return m_path; }

    /**
     * Indices (in block output order) of the outputs of block `height` unspent at the terminal height,
     * or nullopt if the entry is malformed or names more than `outputs` outputs (the block's count).
     */
    std::optional<std::vector<uint32_t>> Unspent(int height, uint32_t outputs) const;

private:
    fs::path m_path;
    int m_height{-1};
    uint256 m_terminal_hash;
    std::vector<uint64_t> m_offsets; //!< file offset of each height's entry
    mutable std::shared_ptr<AutoFile> m_file; //!< kept open: reopening per block is slow on phones
};

/** Salted sum (mod 2^256) of created-minus-spent coins below the terminal height. */
struct Aggregate {
    uint256 salt;
    arith_uint256 sum;

    void Add(const COutPoint& outpoint, const Coin& coin) { sum += Hash(outpoint, coin); }
    void Spend(const COutPoint& outpoint, const Coin& coin) { sum -= Hash(outpoint, coin); }
    arith_uint256 Hash(const COutPoint& outpoint, const Coin& coin) const;
};

/**
 * Undo data waiting for its block: received from peers or RPC and held in memory, or indexed in
 * undo pack files on disk (from file hosting) and read when the block connects.
 *
 * Pack file (all integers little endian): "XBTSWUP1", uint32 version (1), int32 first height,
 * int32 last height, uint32 entry count; then per entry a 32-byte block hash, a uint32 length and
 * that many bytes of serialized CBlockUndo. Entries are blocks between first and last height that
 * spend something, in height order.
 */
/** Where a block's undo data came from: a peer's id, or RPC (an app fetching from file hosting). */
static constexpr int64_t SOURCE_RPC{-1};

class UndoStore
{
public:
    static constexpr size_t MAX_BYTES{128 << 20};
    explicit UndoStore(fs::path) {}
    bool Have(const uint256& block_hash) const;
    bool Read(const uint256& block_hash, CBlockUndo& undo) const;
    bool Write(const uint256& block_hash, const CBlockUndo& undo, int64_t source = SOURCE_RPC);
    /** The source of stored undo data, or nullopt. */
    std::optional<int64_t> Source(const uint256& block_hash) const;
    /** Index the entries of a pack file that [wanted] accepts. Returns how many, or an error. */
    util::Result<size_t> AddPack(const fs::path& path, const std::function<bool(const uint256&)>& wanted);
    /** Read a pack file's entries (hash, offset, length) without touching the store. */
    struct PackEntry {
        uint256 hash;
        uint64_t offset;
        uint32_t len;
    };
    static util::Result<std::vector<PackEntry>> ScanPack(const fs::path& path, int& first, int& last);
    /** Index already-scanned entries of [path] (see ScanPack). */
    size_t IndexPack(const fs::path& path, const std::vector<PackEntry>& entries, const std::function<bool(const uint256&)>& wanted);
    size_t PackedCount() const { return m_packed.size(); }
    void Erase(const uint256& block_hash);
    /** Drop every entry [stale] says is no longer needed; returns how many. */
    size_t EraseIf(const std::function<bool(const uint256&)>& stale);
    void Clear();
    size_t Count() const;
    size_t Bytes() const { return m_bytes; }
    bool Full() const { return m_bytes >= MAX_BYTES; }
    /** Blocks just above the tip that are asked for even when the store is full, so one lost
     *  reply can't leave the store full of later blocks while the next one never comes. */
    static constexpr int RESERVE{8};

private:
    struct Entry {
        CBlockUndo undo;
        size_t bytes;
        int64_t source;
    };
    std::map<uint256, Entry> m_undo;
    size_t m_bytes{0};
    struct PackRef {
        std::shared_ptr<const fs::path> file;
        uint64_t offset;
        uint32_t len;
    };
    std::map<uint256, PackRef> m_packed;
};

/**
 * A coins view for connecting one block below the terminal height: the coins its inputs spend
 * come from the block's undo data; anything else (the BIP30 overwrite check) falls through to the
 * real UTXO set.
 */
class UndoCoinsView : public CCoinsViewBacked
{
public:
    UndoCoinsView(CCoinsView* base, const CBlock& block, const CBlockUndo& undo);
    /** False when the undo data doesn't have exactly one coin per input of the block. */
    bool Matches() const { return m_matches; }
    std::optional<Coin> GetCoin(const COutPoint& outpoint) const override;
    bool HaveCoin(const COutPoint& outpoint) const override;
    const std::unordered_map<COutPoint, Coin, SaltedOutpointHasher>& Spent() const { return m_spent; }

private:
    // Hash maps, not trees: built and probed once per input of every block (measured: the tree
    // version was most of the coin plumbing's cost on a phone).
    std::unordered_map<COutPoint, Coin, SaltedOutpointHasher> m_spent;
    bool m_matches{true};
};

/** SHA-256 of a block's serialized undo data: names the exact data a script pre-validation used. */
uint256 UndoHash(const CBlockUndo& undo);

/**
 * Script pre-validation: worker threads check every script of blocks above the tip, each block
 * on its own, from the block, its undo data and the header chain (see
 * StartSwiftSyncPrevalidation). A block that passed is noted with the hash of the undo data it
 * was checked against; ConnectBlock then skips its script checks only when it connects the block
 * with exactly that undo data, and checks everything else as always. The results are this node's
 * own, so nothing is trusted that wasn't verified here; wrong undo data is still caught by the
 * aggregate at the terminal height, as without pre-validation.
 */
class Prevalidated
{
public:
    /** A worker takes `block` to check; false if it is taken, done, or already being connected. */
    bool Claim(const uint256& block) EXCLUSIVE_LOCKS_REQUIRED(!m_mutex);
    /** A worker's result for a block it claimed. */
    void Finish(const uint256& block, bool ok, const uint256& undo) EXCLUSIVE_LOCKS_REQUIRED(!m_mutex);
    /**
     * The block is about to connect: waits for a worker still checking it (so no block is checked
     * twice), then answers whether it passed with exactly this undo data. Afterwards no worker takes
     * it; Forget() drops the entry once the block is connected.
     */
    bool Connecting(const uint256& block, const uint256& undo) EXCLUSIVE_LOCKS_REQUIRED(!m_mutex);
    void Forget(const uint256& block) EXCLUSIVE_LOCKS_REQUIRED(!m_mutex);
    /** Whether a worker should still check `block` (claimed and not yet reached by the tip). */
    bool Wanted(const uint256& block) const EXCLUSIVE_LOCKS_REQUIRED(!m_mutex);
    size_t Size() const EXCLUSIVE_LOCKS_REQUIRED(!m_mutex);

    std::atomic<uint64_t> m_checked{0}; //!< blocks whose scripts all passed
    std::atomic<uint64_t> m_failed{0};  //!< blocks a script failed in (checked again when they connect)
    std::atomic<uint64_t> m_hits{0};    //!< blocks connected without checking their scripts again
    std::atomic<uint64_t> m_misses{0};  //!< blocks connected with full script checks
    std::atomic<uint64_t> m_waited{0};  //!< connects that waited for a worker to finish the block

private:
    enum class State { CHECKING, PASSED, FAILED, CONNECTING };
    struct Entry {
        State state;
        uint256 undo;
    };
    mutable Mutex m_mutex;
    std::condition_variable m_cv;
    std::map<uint256, Entry> m_entries GUARDED_BY(m_mutex);
};

/** The node's SwiftSync state: active from startup with -swiftsync until the terminal height. */
class SwiftSync
{
public:
    SwiftSync(Hints hints, fs::path undo_dir) : m_hints(std::move(hints)), m_undo(std::move(undo_dir)) {}
    ~SwiftSync() { StopPrevalidation(); }

    const Hints& GetHints() const { return m_hints; }
    UndoStore& Undo() { return m_undo; }
    Aggregate& Agg() { return m_agg; }
    Prevalidated& Pre() { return m_pre; }

    /** Pre-validation threads (started by StartSwiftSyncPrevalidation); stopped before shutdown. */
    std::vector<std::thread> m_pre_threads;
    std::atomic<bool> m_pre_stop{false};
    /**
     * Share of the time (10-100%) each pre-validation worker spends checking: after each
     * transaction it rests in proportion to the work just done. On a phone the cores of a cluster
     * share one clock, set by the busiest core; fewer cores at full load push that clock (and the
     * energy per signature) up, while all cores at part load let it drop. setswiftsyncduty sets it.
     */
    std::atomic<int> m_pre_duty{100};
    void StopPrevalidation();

    /** Whether block `height` must be connected the SwiftSync way. */
    bool Covers(int height) const { return !m_done && height > 0 && height <= m_hints.Height(); }
    bool Done() const { return m_done; }
    void SetDone() { m_done = true; }

    /** State saved atomically with the UTXO set (txdb key 'W'). */
    std::vector<unsigned char> Serialize() const;
    bool Deserialize(const std::vector<unsigned char>& data);
    /** A fresh random salt, for a sync that starts from genesis. */
    void NewSalt();

    /** Times the undo for a block failed to validate (bad data from a peer, re-requested). */
    std::map<uint256, int> m_undo_failures;
    /** Peers whose undo data failed a block: no longer asked. */
    std::set<int64_t> m_bad_peers;
    /** Blocks whose undo data from file hosting (RPC) failed, and when: fetched from peers for a
     *  while, then from file hosting again (there may be no peer serving it). */
    std::map<uint256, int64_t> m_bad_rpc_blocks;
    static constexpr int64_t RPC_BLAME_SECONDS{60};
    void BlameRpc(const uint256& hash);
    bool RpcBlamed(const uint256& hash) const;
    /** Set when SwiftSync can't go on (e.g. the hints are for a block not on this chain); the app falls back. */
    std::string m_error;

    /**
     * Block packs (loadblockpack) bring blocks ahead of the tip. While they do, peers are asked only
     * for the next PACK_PEER_WINDOW blocks above the tip (the gaps a late pack leaves), not the
     * whole download window: else most blocks arrive twice, and each copy costs radio, decryption,
     * parsing and a check before it's dropped.
     */
    static constexpr int PACK_PEER_WINDOW{64};
    static constexpr int64_t PACK_FEED_SECONDS{120};
    void NotePackStored() { m_pack_stored_at = GetTime(); }
    bool PacksFeeding() const { return GetTime() - m_pack_stored_at.load() < PACK_FEED_SECONDS; }

private:
    Hints m_hints;
    UndoStore m_undo;
    Aggregate m_agg;
    Prevalidated m_pre;
    bool m_done{false};
    std::atomic<int64_t> m_pack_stored_at{0};
};

/** Whether saved SwiftSync state (txdb key 'W') belongs to a sync that hasn't reached its terminal height. */
bool StateActive(const std::vector<unsigned char>& saved);

/** Set by init when -swiftsync is given; null otherwise. Guarded by cs_main. */
extern std::unique_ptr<SwiftSync> g_swiftsync;

/**
 * Write a hints file for the chainstate's current tip. Reads a snapshot of the UTXO set, then
 * every block from genesis. Returns an error message on failure.
 */
std::optional<std::string> WriteHints(Chainstate& chainstate, const fs::path& path, int& height_out, uint256& hash_out);

} // namespace swiftsync

#endif // BITCOIN_SWIFTSYNC_H
