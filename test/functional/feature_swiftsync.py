#!/usr/bin/env python3
# Copyright (c) 2026 The Eddy developers
# Distributed under the MIT software license, see the accompanying
# file COPYING or http://www.opensource.org/licenses/mit-license.php.
"""SwiftSync: sync from genesis with a hints file and peer-served undo data.

node0 mines a chain with many transactions (some outputs spent later, some left unspent, some
spent in the block that creates them), writes a hints file, then mines past it.
node1 syncs from genesis with -swiftsync, is restarted partway, and must end with exactly the
same UTXO set as node0. node2 gets a hints file with one hint flipped and must stop with an error
instead of accepting the chain.
"""
import shutil

from test_framework.test_framework import BitcoinTestFramework
from test_framework.util import assert_equal
from test_framework.wallet import MiniWallet, MiniWalletMode


class SwiftSyncTest(BitcoinTestFramework):
    def set_test_params(self):
        self.num_nodes = 3
        self.setup_clean_chain = True
        self.extra_args = [["-coinstatsindex=0"], [], []]

    def setup_network(self):
        self.setup_nodes()  # no connections: each node joins node0 when the test says

    def make_history(self, blocks):
        node = self.nodes[0]
        wallet = self.wallet
        for i in range(blocks):
            # Signed spends too, so signature checks run against the coins from served undo data.
            self.signer.send_self_transfer(from_node=node)
            # Spend a few coins (creating new ones), chain one spend within the block, leave the rest.
            for _ in range(3):
                wallet.send_self_transfer(from_node=node)
            if i % 3 == 0:
                parent = wallet.send_self_transfer(from_node=node)
                wallet.send_self_transfer(from_node=node, utxo_to_spend=parent["new_utxo"])
            if i % 5 == 0:
                wallet.send_self_transfer_multi(from_node=node, num_outputs=4)
            self.generate(node, 1, sync_fun=self.no_op)

    def run_test(self):
        node0 = self.nodes[0]
        self.wallet = MiniWallet(node0)
        self.signer = MiniWallet(node0, mode=MiniWalletMode.RAW_P2PK)
        self.generate(self.signer, 30, sync_fun=self.no_op)
        self.generate(self.wallet, 120, sync_fun=self.no_op)
        self.make_history(150)

        hints = node0.generateswiftsynchints("swiftsync.hints")
        self.log.info(f"Hints to height {hints['height']}")
        assert_equal(hints["height"], node0.getblockcount())
        self.make_history(20)  # the chain goes on past the hints; those blocks are checked normally

        self.log.info("node1 syncs from genesis with SwiftSync")
        node1_hints = self.nodes[1].chain_path / "swiftsync.hints"
        shutil.copy(hints["path"], node1_hints)
        self.restart_node(1, extra_args=["-swiftsync=swiftsync.hints", "-stopatheight=200"])
        self.connect_nodes(1, 0)
        self.nodes[1].wait_until_stopped(timeout=120)

        self.log.info("node1 restarts mid-sync and continues")
        self.start_node(1, extra_args=["-swiftsync=swiftsync.hints"])
        info = self.nodes[1].getswiftsyncinfo()
        assert_equal(info["terminal_height"], hints["height"])
        self.connect_nodes(1, 0)
        self.sync_blocks([node0, self.nodes[1]], timeout=120)
        assert_equal(self.nodes[1].getswiftsyncinfo()["active"], False)
        assert self.nodes[1].debug_log_path.read_text().count("SwiftSync complete") == 1

        self.log.info("node1's UTXO set equals node0's")
        a = node0.gettxoutsetinfo("hash_serialized_3")
        b = self.nodes[1].gettxoutsetinfo("hash_serialized_3")
        assert_equal(a["hash_serialized_3"], b["hash_serialized_3"])
        assert_equal(a["txouts"], b["txouts"])
        assert_equal(a["total_amount"], b["total_amount"])

        self.log.info("the undo export writes one file per block that spends something")
        res = node0.exportswiftsyncundo("undo-export", hints["height"])
        spending = sum(1 for h in range(1, hints["height"] + 1)
                       if node0.getblock(node0.getblockhash(h))["nTx"] > 1)
        assert_equal(res["files"], spending)
        assert_equal(res["files"] + res["skipped"], hints["height"])
        again = node0.exportswiftsyncundo("undo-export", hints["height"])
        assert_equal(again["files"], 0)  # resumable: existing files are kept

        self.log.info("undo data can also arrive by RPC, as an app fetching from file hosting does it")
        node2 = self.nodes[2]
        shutil.copy(hints["path"], node2.chain_path / "rpc.hints")
        self.restart_node(2, extra_args=["-swiftsync=rpc.hints", "-noconnect"])
        # Headers and blocks from node0, but not as a manual peer, so no swundo comes over P2P.
        self.connect_nodes(0, 2)  # inbound to node2: not a manual connection on its side
        export = node0.chain_path / "undo-export"
        def feed():
            for want in node2.getswiftsyncwants(512):
                f = export / want["hash"][:2] / want["hash"]
                # No file means the block spends nothing (coinbase only): the node needs no undo data
                # for it once the block itself arrives. File hosting answers 404 here.
                if f.exists():
                    node2.submitswiftsyncundo(want["hash"], f.read_bytes().hex())
            return node2.getblockcount() >= node0.getblockcount()
        self.wait_until(feed, timeout=120)
        assert "SwiftSync complete" in node2.debug_log_path.read_text()
        assert_equal(node2.getbestblockhash(), node0.getbestblockhash())
        self.stop_node(2)
        shutil.rmtree(node2.chain_path / "blocks")
        shutil.rmtree(node2.chain_path / "chainstate")

        self.log.info("a wrong hint stops the sync instead of being accepted")
        data = bytearray(open(hints["path"], "rb").read())
        # Header is 8 + 4 + 4 + 32 bytes; flip the low bit of the last byte (a gap in the last block).
        data[-1] ^= 1
        bad = self.nodes[2].chain_path / "bad.hints"
        open(bad, "wb").write(bytes(data))
        log_start = len(self.nodes[2].debug_log_path.read_text())
        self.restart_node(2, extra_args=["-swiftsync=bad.hints"])
        self.connect_nodes(2, 0)
        # The node stops itself with an error at the terminal height (or earlier, if the flipped
        # hint names an output that doesn't exist), and never gets past it.
        self.wait_until(lambda: self.nodes[2].process.poll() is not None, timeout=120)
        log = self.nodes[2].debug_log_path.read_text()[log_start:]
        assert "hints file is wrong" in log or "SwiftSync failed" in log, "no SwiftSync error in the log"
        assert "SwiftSync complete" not in log
        self.nodes[2].running = False
        self.nodes[2].process = None
        self.nodes[2].rpc_connected = False
        self.nodes[2].rpc = None

if __name__ == "__main__":
    SwiftSyncTest(__file__).main()
