#!/usr/bin/env python3
# Copyright (c) 2026 The Eddy developers
# Distributed under the MIT software license, see the accompanying
# file COPYING or http://www.opensource.org/licenses/mit-license.php.
"""Write gzipped block packs (many consecutive blocks per file) from a node's RPC.

A phone syncing from P2P peers got at most about 12 MB/s of blocks; plain file hosting reached
60 MB/s. Blocks from a pack are as untrusted as blocks from a peer: `loadblockpack` takes only
blocks matching headers on the best header chain, and every block is validated when it connects.

The output directory gets:

  block/<first>-<last>.gz   gzip of a block pack (format in `loadblockpack` help), about --target bytes before compression
  blocks.json               {"version": 1, "height", "hash", "packs": [{"first", "last", "blocks", "bytes", "gz_bytes", "sha256"}]}

Heights 1..--height are split into --segments ranges of about equal size (from block sizes
sampled every 1000 blocks); each range is packed in order by one of --workers processes. A rerun
with the same arguments writes the same packs and skips those already written.

usage: make-block-packs.py --out DIR --height H --cookie FILE [--rpc http://127.0.0.1:8332]
"""
import argparse
import base64
import gzip
import hashlib
import json
import os
import struct
import sys
import urllib.request
from multiprocessing import Pool

MAGIC = b"XBTBLKP1"
A = None


def rpc(calls):
    auth = base64.b64encode(open(A.cookie, "rb").read().strip()).decode()
    body = json.dumps([{"jsonrpc": "1.0", "id": i, "method": m, "params": p} for i, (m, p) in enumerate(calls)]).encode()
    req = urllib.request.Request(A.rpc, body, {"Authorization": "Basic " + auth, "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=600) as r:
        out = sorted(json.load(r), key=lambda x: x["id"])
    for x in out:
        if x.get("error"):
            raise SystemExit(f"rpc error: {x['error']}")
    return [x["result"] for x in out]


def write_pack(first, last, blocks):
    name = os.path.join(A.out, "block", f"{first}-{last}.gz")
    raw = bytearray(MAGIC + struct.pack("<IiiI", 1, first, last, len(blocks)))
    for b in blocks:
        raw += struct.pack("<I", len(b)) + b
    gz = gzip.compress(bytes(raw), 6, mtime=0)
    if not (os.path.exists(name) and os.path.getsize(name) == len(gz)):
        with open(name + ".tmp", "wb") as f:
            f.write(gz)
        os.replace(name + ".tmp", name)
    return {"first": first, "last": last, "blocks": len(blocks), "bytes": len(raw),
            "gz_bytes": len(gz), "sha256": hashlib.sha256(gz).hexdigest()}


def segment(rng):
    lo, hi = rng
    packs, blocks, size, first = [], [], 0, lo
    for start in range(lo, hi + 1, 64):
        hs = list(range(start, min(hi, start + 63) + 1))
        hashes = rpc([("getblockhash", [h]) for h in hs])
        for h, hexblock in zip(hs, rpc([("getblock", [x, 0]) for x in hashes])):
            b = bytes.fromhex(hexblock)
            blocks.append(b)
            size += 4 + len(b)
            if size >= A.target or h == hi:
                packs.append(write_pack(first, h, blocks))
                blocks, size, first = [], 0, h + 1
    print(f"segment {lo}-{hi}: {len(packs)} packs", file=sys.stderr, flush=True)
    return packs


def init(a):
    global A
    A = a


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--height", type=int, required=True)
    ap.add_argument("--cookie", required=True)
    ap.add_argument("--rpc", default="http://127.0.0.1:8332")
    ap.add_argument("--target", type=int, default=32 << 20)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--segments", type=int, default=96)
    a = ap.parse_args()
    init(a)
    os.makedirs(os.path.join(a.out, "block"), exist_ok=True)

    # Balance segments by bytes: sample block sizes every 1000 blocks.
    sample = list(range(1, a.height + 1, 1000))
    sizes = []
    for i in range(0, len(sample), 200):
        sizes += [s["total_size"] for s in rpc([("getblockstats", [h, ["total_size"]]) for h in sample[i:i + 200]])]
    total = sum(sizes)
    bounds, acc, lo = [], 0, 1
    for h, s in zip(sample, sizes):
        acc += s
        if acc >= total / a.segments and h > lo:
            bounds.append((lo, h + 999 if h + 999 < a.height else a.height))
            lo, acc = bounds[-1][1] + 1, 0
    if lo <= a.height:
        bounds.append((lo, a.height))
    print(f"{len(bounds)} segments", file=sys.stderr, flush=True)

    with Pool(a.workers, initializer=init, initargs=(a,)) as pool:
        packs = [p for seg in pool.map(segment, bounds, chunksize=1) for p in seg]
    index = {"version": 1, "height": a.height, "hash": rpc([("getblockhash", [a.height])])[0], "packs": packs}
    with open(os.path.join(a.out, "blocks.json.tmp"), "w") as f:
        json.dump(index, f, separators=(",", ":"))
    os.replace(os.path.join(a.out, "blocks.json.tmp"), os.path.join(a.out, "blocks.json"))
    print(f"done: {len(packs)} packs, {sum(p['blocks'] for p in packs)} blocks, "
          f"{sum(p['bytes'] for p in packs)} bytes, {sum(p['gz_bytes'] for p in packs)} gzipped", flush=True)


if __name__ == "__main__":
    main()
