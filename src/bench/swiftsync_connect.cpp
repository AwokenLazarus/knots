// Copyright (c) 2026 The Eddy developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

// Per-block CPU of connecting a real mainnet block the SwiftSync way, split into its parts.
// Needs EDDY_SWIFTSYNC_BENCH=<dir> holding blk-<height>.bin (serialized block with witness) and
// undo-<height>.bin (its serialized CBlockUndo); every pair found there is used. Without it the
// benchmarks do nothing.

#include <bench/bench.h>
#include <chainparams.h>
#include <common/args.h>
#include <coins.h>
#include <consensus/amount.h>
#include <consensus/tx_verify.h>
#include <consensus/validation.h>
#include <hash.h>
#include <key.h>
#include <primitives/block.h>
#include <script/interpreter.h>
#include <script/sigcache.h>
#include <script/solver.h>
#include <streams.h>
#include <swiftsync.h>
#include <undo.h>
#include <util/chaintype.h>
#include <validation.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <string>
#include <vector>

namespace {

struct BlockCase {
    int height;
    CBlock block;
    CBlockUndo undo;
    std::vector<unsigned char> block_bytes;
    std::vector<unsigned char> undo_bytes;
};

std::vector<unsigned char> ReadAll(const std::filesystem::path& p)
{
    std::ifstream f{p, std::ios::binary};
    return {std::istreambuf_iterator<char>{f}, {}};
}

const std::vector<BlockCase>& Cases()
{
    static const std::vector<BlockCase> cases{[] {
        std::vector<BlockCase> out;
        const char* dir{std::getenv("EDDY_SWIFTSYNC_BENCH")};
        if (!dir) return out;
        for (const auto& e : std::filesystem::directory_iterator{dir}) {
            const std::string name{e.path().filename().string()};
            if (name.rfind("blk-", 0) != 0 || e.path().extension() != ".bin") continue;
            const std::string h{name.substr(4, name.size() - 8)};
            const auto undo_path{e.path().parent_path() / ("undo-" + h + ".bin")};
            if (!std::filesystem::exists(undo_path)) continue;
            BlockCase c;
            c.height = std::stoi(h);
            c.block_bytes = ReadAll(e.path());
            c.undo_bytes = ReadAll(undo_path);
            DataStream{c.block_bytes} >> TX_WITH_WITNESS(c.block);
            DataStream{c.undo_bytes} >> c.undo;
            out.push_back(std::move(c));
        }
        return out;
    }()};
    return cases;
}

const Consensus::Params& Mainnet()
{
    static const auto params{CreateChainParams(ArgsManager{}, ChainType::MAIN)};
    return params->GetConsensus();
}

constexpr unsigned int FLAGS{SCRIPT_VERIFY_P2SH | SCRIPT_VERIFY_DERSIG | SCRIPT_VERIFY_CHECKLOCKTIMEVERIFY |
                             SCRIPT_VERIFY_CHECKSEQUENCEVERIFY | SCRIPT_VERIFY_WITNESS | SCRIPT_VERIFY_NULLDUMMY |
                             SCRIPT_VERIFY_TAPROOT};

std::vector<CTxOut> SpentOutputs(const CTxUndo& txundo)
{
    std::vector<CTxOut> spent;
    spent.reserve(txundo.vprevout.size());
    for (const auto& c : txundo.vprevout) spent.push_back(c.out);
    return spent;
}

} // namespace

/** Deserializing the block (done again when it is read back from disk to connect). */
static void SwiftSyncParseBlock(benchmark::Bench& bench)
{
    bench.run([&] {
        for (const auto& c : Cases()) {
            CBlock b;
            DataStream{c.block_bytes} >> TX_WITH_WITNESS(b);
            ankerl::nanobench::doNotOptimizeAway(b.vtx.size());
        }
    });
}

/** Deserializing the block's undo data from file hosting or a peer. */
static void SwiftSyncParseUndo(benchmark::Bench& bench)
{
    bench.run([&] {
        for (const auto& c : Cases()) {
            CBlockUndo u;
            DataStream{c.undo_bytes} >> u;
            ankerl::nanobench::doNotOptimizeAway(u.vtxundo.size());
        }
    });
}

/**
 * What ConnectBlock does per transaction today, minus scripts: an undo-backed coins view, a
 * coins cache over it, CheckTxInputs, BIP68 prevheights, sigop cost, spending inputs into new undo
 * data and adding every output to the cache.
 */
static void SwiftSyncCoinPath(benchmark::Bench& bench)
{
    const auto& params{Mainnet()};
    CCoinsView empty;
    bench.run([&] {
        for (const auto& c : Cases()) {
            swiftsync::UndoCoinsView uview{&empty, c.block, c.undo};
            CCoinsViewCache view{&uview};
            CBlockUndo blockundo;
            std::vector<int> prevheights;
            CAmount fees{0};
            int64_t sigops{0};
            for (size_t i = 0; i < c.block.vtx.size(); ++i) {
                const CTransaction& tx{*c.block.vtx[i]};
                if (!tx.IsCoinBase()) {
                    CAmount txfee{0};
                    TxValidationState st;
                    bool ok{Consensus::CheckTxInputs(tx, st, view, c.height, txfee, CheckTxInputsRules::None, params)};
                    assert(ok);
                    fees += txfee;
                    prevheights.resize(tx.vin.size());
                    for (size_t j = 0; j < tx.vin.size(); ++j) prevheights[j] = view.AccessCoin(tx.vin[j].prevout).nHeight;
                }
                sigops += GetTransactionSigOpCost(tx, view, FLAGS);
                if (i > 0) {
                    blockundo.vtxundo.emplace_back();
                    for (const auto& in : tx.vin) {
                        blockundo.vtxundo.back().vprevout.emplace_back();
                        view.SpendCoin(in.prevout, &blockundo.vtxundo.back().vprevout.back());
                    }
                }
                AddCoins(view, tx, c.height);
            }
            ankerl::nanobench::doNotOptimizeAway(fees + sigops);
        }
    });
}

/** Building each transaction's sighash precomputation (hashPrevouts, spent amounts and scripts...). */
static void SwiftSyncTxdata(benchmark::Bench& bench)
{
    bench.run([&] {
        for (const auto& c : Cases()) {
            for (size_t i = 1; i < c.block.vtx.size(); ++i) {
                PrecomputedTransactionData txdata;
                txdata.Init(*c.block.vtx[i], SpentOutputs(c.undo.vtxundo[i - 1]));
                ankerl::nanobench::doNotOptimizeAway(txdata.m_bip143_segwit_ready);
            }
        }
    });
}

/** Every script and signature of the block, on one thread (the work the check queue spreads). */
static void SwiftSyncScripts(benchmark::Bench& bench)
{
    ECC_Context ecc{};
    SignatureCache cache{1 << 20};
    bench.run([&] {
        for (const auto& c : Cases()) {
            for (size_t i = 1; i < c.block.vtx.size(); ++i) {
                const CTransaction& tx{*c.block.vtx[i]};
                const CTxUndo& txundo{c.undo.vtxundo[i - 1]};
                PrecomputedTransactionData txdata;
                txdata.Init(tx, SpentOutputs(txundo));
                for (unsigned int j = 0; j < tx.vin.size(); ++j) {
                    CScriptCheck check{txundo.vprevout[j].out, tx, cache, j, FLAGS, /*cacheIn=*/false, &txdata};
                    const auto err{check()};
                    assert(!err);
                }
            }
        }
    });
}

/** The SwiftSync aggregate: a salted SHA-256 per created output and per spent input. */
static void SwiftSyncAggregate(benchmark::Bench& bench)
{
    swiftsync::Aggregate agg;
    bench.run([&] {
        for (const auto& c : Cases()) {
            for (size_t i = 0; i < c.block.vtx.size(); ++i) {
                const CTransaction& tx{*c.block.vtx[i]};
                for (uint32_t o = 0; o < tx.vout.size(); ++o) agg.Add(COutPoint{tx.GetHash(), o}, Coin{tx.vout[o], c.height, i == 0});
                if (i == 0) continue;
                const CTxUndo& txundo{c.undo.vtxundo[i - 1]};
                for (size_t j = 0; j < tx.vin.size(); ++j) agg.Spend(tx.vin[j].prevout, txundo.vprevout[j]);
            }
        }
        ankerl::nanobench::doNotOptimizeAway(agg.sum);
    });
}

/** Writing undo data again (serialize and checksum, as WriteBlockUndo does; no disk). */
static void SwiftSyncUndoWrite(benchmark::Bench& bench)
{
    bench.run([&] {
        for (const auto& c : Cases()) {
            DataStream ds;
            ds << c.undo;
            HashWriter hasher{};
            hasher << c.block.hashPrevBlock << c.undo;
            ankerl::nanobench::doNotOptimizeAway(hasher.GetHash());
            ankerl::nanobench::doNotOptimizeAway(ds.size());
        }
    });
}

/**
 * Script time by the kind of output spent, printed once: where signature work goes, to choose
 * between optimizations (Schnorr batching, legacy sighash, ...). Each input is timed alone.
 */
static void SwiftSyncScriptsByType(benchmark::Bench& bench)
{
    ECC_Context ecc{};
    SignatureCache cache{1 << 20};
    static bool printed{false};
    if (!printed) {
        printed = true;
        std::map<std::string, std::pair<int, double>> by;
        for (const auto& c : Cases()) {
            for (size_t i = 1; i < c.block.vtx.size(); ++i) {
                const CTransaction& tx{*c.block.vtx[i]};
                const CTxUndo& txundo{c.undo.vtxundo[i - 1]};
                PrecomputedTransactionData txdata;
                txdata.Init(tx, SpentOutputs(txundo));
                for (unsigned int j = 0; j < tx.vin.size(); ++j) {
                    std::vector<std::vector<unsigned char>> sol;
                    std::string kind{GetTxnOutputType(Solver(txundo.vprevout[j].out.scriptPubKey, sol))};
                    if (kind == "witness_v1_taproot") {
                        const auto& st{tx.vin[j].scriptWitness.stack};
                        const bool annex{st.size() >= 2 && !st.back().empty() && st.back()[0] == 0x50};
                        kind += st.size() - (annex ? 1 : 0) == 1 ? "_keypath" : "_scriptpath";
                    }
                    const auto t0{std::chrono::steady_clock::now()};
                    for (int r = 0; r < 5; ++r) {
                        CScriptCheck check{txundo.vprevout[j].out, tx, cache, j, FLAGS, false, &txdata};
                        assert(!check());
                    }
                    const double us{std::chrono::duration<double, std::micro>(std::chrono::steady_clock::now() - t0).count() / 5};
                    by[kind].first++;
                    by[kind].second += us;
                }
            }
        }
        double total{0};
        for (const auto& [k, v] : by) total += v.second;
        for (const auto& [k, v] : by) {
            std::fprintf(stderr, "scripts-by-type %-30s inputs %6d  total %9.1f ms  %5.1f%%  %7.1f us/input\n",
                         k.c_str(), v.first, v.second / 1000, 100 * v.second / total, v.second / v.first);
        }
    }
    bench.run([] {});
}

BENCHMARK(SwiftSyncParseBlock, benchmark::PriorityLevel::LOW);
BENCHMARK(SwiftSyncScriptsByType, benchmark::PriorityLevel::LOW);
BENCHMARK(SwiftSyncParseUndo, benchmark::PriorityLevel::LOW);
BENCHMARK(SwiftSyncCoinPath, benchmark::PriorityLevel::LOW);
BENCHMARK(SwiftSyncTxdata, benchmark::PriorityLevel::LOW);
BENCHMARK(SwiftSyncScripts, benchmark::PriorityLevel::LOW);
BENCHMARK(SwiftSyncAggregate, benchmark::PriorityLevel::LOW);
BENCHMARK(SwiftSyncUndoWrite, benchmark::PriorityLevel::LOW);
