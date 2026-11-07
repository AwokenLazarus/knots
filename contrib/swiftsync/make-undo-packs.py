#!/usr/bin/env python3
# Copyright (c) 2026 The Eddy developers
# Distributed under the MIT software license, see the accompanying
# file COPYING or http://www.opensource.org/licenses/mit-license.php.
"""Bundle per-block SwiftSync undo files (from `exportswiftsyncundo`) into gzipped packs.

File hosting serves one file per block that spends something; fetching those one at a time is
bound by request latency on a phone. Packs hold many consecutive blocks each, so a client
streams them instead. The output directory gets:

  pack/<first>-<last>.gz   gzip of an undo pack (format in src/swiftsync.h), about --target bytes before compression
  packs.json               {"version": 1, "terminal_height", "terminal_hash", "packs": [{"first", "last",
                           "entries", "bytes", "gz_bytes", "sha256" (of the .gz)}]}

Pack boundaries depend only on the export and --target, so a rerun writes the same packs (and
skips those already written). Block hashes by height come from the node's RPC (cookie auth).

usage: make-undo-packs.py --export DIR --out DIR --height H --cookie FILE [--rpc http://127.0.0.1:8332]
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
from concurrent.futures import ThreadPoolExecutor

MAGIC = b"XBTSWUP1"


def rpc_batch(url, auth, calls):
    body = json.dumps([{"jsonrpc": "1.0", "id": i, "method": m, "params": p} for i, (m, p) in enumerate(calls)]).encode()
    req = urllib.request.Request(url, body, {"Authorization": "Basic " + auth, "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=300) as r:
        out = sorted(json.load(r), key=lambda x: x["id"])
    for x in out:
        if x.get("error"):
            raise SystemExit(f"rpc error: {x['error']}")
    return [x["result"] for x in out]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--export", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--height", type=int, required=True, help="the hints' terminal height")
    ap.add_argument("--cookie", required=True)
    ap.add_argument("--rpc", default="http://127.0.0.1:8332")
    ap.add_argument("--target", type=int, default=16 << 20)
    ap.add_argument("--threads", type=int, default=8)
    a = ap.parse_args()

    auth = base64.b64encode(open(a.cookie, "rb").read().strip()).decode()
    hashes = [None]
    for start in range(1, a.height + 1, 5000):
        hs = rpc_batch(a.rpc, auth, [("getblockhash", [h]) for h in range(start, min(a.height, start + 4999) + 1)])
        hashes.extend(hs)
        print(f"hashes {len(hashes) - 1}/{a.height}", file=sys.stderr, flush=True)
    terminal_hash = hashes[a.height]
    os.makedirs(os.path.join(a.out, "pack"), exist_ok=True)

    def write(first, last, files):
        name = os.path.join(a.out, "pack", f"{first}-{last}.gz")
        raw = bytearray(MAGIC + struct.pack("<IiiI", 1, first, last, len(files)))
        for bh, path in files:
            data = open(path, "rb").read()
            raw += bytes.fromhex(bh)[::-1] + struct.pack("<I", len(data)) + data
        gz = gzip.compress(bytes(raw), 6, mtime=0)
        if not (os.path.exists(name) and os.path.getsize(name) == len(gz)):
            with open(name + ".tmp", "wb") as f:
                f.write(gz)
            os.replace(name + ".tmp", name)
        return {"first": first, "last": last, "entries": len(files), "bytes": len(raw),
                "gz_bytes": len(gz), "sha256": hashlib.sha256(gz).hexdigest()}

    jobs = []
    files, size, first = [], 0, 1
    with ThreadPoolExecutor(a.threads) as ex:
        for h in range(1, a.height + 1):
            bh = hashes[h]
            path = os.path.join(a.export, bh[:2], bh)
            try:
                n = os.path.getsize(path)
            except FileNotFoundError:
                n = None  # spends nothing (coinbase only)
            if n is not None:
                files.append((bh, path))
                size += 36 + n
            if size >= a.target or h == a.height:
                jobs.append(ex.submit(write, first, h, files))
                files, size, first = [], 0, h + 1
        packs = []
        for i, j in enumerate(jobs):
            packs.append(j.result())
            if i % 200 == 0:
                print(f"packs {i + 1}/{len(jobs)} (to height {packs[-1]['last']})", file=sys.stderr, flush=True)
    index = {"version": 1, "terminal_height": a.height, "terminal_hash": terminal_hash, "packs": packs}
    with open(os.path.join(a.out, "packs.json.tmp"), "w") as f:
        json.dump(index, f, separators=(",", ":"))
    os.replace(os.path.join(a.out, "packs.json.tmp"), os.path.join(a.out, "packs.json"))
    print(f"done: {len(packs)} packs, {sum(p['entries'] for p in packs)} entries, "
          f"{sum(p['bytes'] for p in packs)} bytes, {sum(p['gz_bytes'] for p in packs)} gzipped", flush=True)


if __name__ == "__main__":
    main()
