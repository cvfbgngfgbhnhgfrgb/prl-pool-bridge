#!/usr/bin/env python3
"""
pool_connector.py  --  Kryptex Pearl (PRL) pool <-> GitHub bridge.

What it does, in one loop:

  1. Opens a stratum connection to  prl.kryptex.network  and authorizes the wallet.
  2. Every `mining.notify` job the pool pushes is rewritten into  jobs.txt  on GitHub
     (newest job first), together with a per-PC nonce-range split so that N mining
     PCs never scan the same nonces.
  3. In a second thread it watches  shares.txt  on GitHub. The moment that file has
     content, it is consumed atomically (read + clear in one commit), and every share
     inside is submitted to the pool with `mining.submit`. Then it goes back to
     waiting for the next share.

The pool's dialect (verified live against prl.kryptex.network:7048):
    -> {"id":1,"method":"mining.subscribe","params":["krig/1.2.0"]}
    -> {"id":2,"method":"mining.authorize","params":["krxYRPV4WQ.0x","x"]}
    <- {"id":2,"result":true,"error":null}
    <- {"id":null,"method":"mining.notify","params":{"header":"...","height":100107,
        "job_id":"f7188fd3_2097152","target":"0000...ffff","cert_version":3}}
    -> {"id":N,"method":"mining.submit","params":{"job_id":"...","nonce":"...", ...}}
  Note that params are JSON *objects*, not the positional arrays of classic stratum;
  the pool answers `[20,"Unsupported submit format"]` to array-style submits.

Usage:
    python3 pool_connector.py                # asks how many PCs are joining
    python3 pool_connector.py --pcs 4        # non-interactive
    python3 pool_connector.py --ssl          # use stratum+ssl://...:8048
"""

import argparse
import json
import os
import queue
import re
import socket
import ssl
import sys
import threading
import time

from gh import load_config, store_from_config

STOP = threading.Event()
NONCE_SPACE = 1 << 64


def log(tag, msg):
    print("[%s] %-9s %s" % (time.strftime("%H:%M:%S"), tag, msg), flush=True)


def parse_stratum_url(url):
    m = re.match(r"^(?:stratum\+)?(tcp|ssl|tls)://([^:/]+):(\d+)", url.strip())
    if not m:
        raise ValueError("bad stratum url: %r" % url)
    scheme, host, port = m.group(1), m.group(2), int(m.group(3))
    return host, port, scheme in ("ssl", "tls")


def nonce_ranges(pcs):
    """Split the 64-bit nonce space into `pcs` contiguous slices."""
    step = NONCE_SPACE // pcs
    out = []
    for i in range(pcs):
        start = i * step
        end = (start + step - 1) if i < pcs - 1 else (NONCE_SPACE - 1)
        out.append({"pc": i, "nonce_start": "%016x" % start, "nonce_end": "%016x" % end})
    return out


class PoolClient:
    """Line-delimited JSON-RPC stratum client with auto-reconnect."""

    def __init__(self, host, port, use_ssl, wallet, password="x", agent="krig/1.2.0"):
        self.host, self.port, self.use_ssl = host, port, use_ssl
        self.wallet, self.password, self.agent = wallet, password, agent
        self.sock = None
        self.fh = None
        self._id = 10
        self._lock = threading.Lock()
        self.connected = threading.Event()

    def connect(self):
        self.close()
        sock = socket.create_connection((self.host, self.port), timeout=30)
        sock.settimeout(None)
        if self.use_ssl:
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            sock = ctx.wrap_socket(sock, server_hostname=self.host)
        self.sock = sock
        self.fh = sock.makefile("rwb")
        self.send({"id": 1, "method": "mining.subscribe", "params": [self.agent]})
        self.send({"id": 2, "method": "mining.authorize", "params": [self.wallet, self.password]})
        self.connected.set()
        log("pool", "connected %s://%s:%d as %s" %
            ("ssl" if self.use_ssl else "tcp", self.host, self.port, self.wallet))

    def close(self):
        self.connected.clear()
        for obj in (self.fh, self.sock):
            try:
                obj and obj.close()
            except Exception:
                pass
        self.fh = self.sock = None

    def send(self, obj):
        with self._lock:
            if not self.fh:
                raise IOError("not connected")
            self.fh.write(json.dumps(obj).encode() + b"\n")
            self.fh.flush()

    def next_id(self):
        with self._lock:
            self._id += 1
            return self._id

    def submit(self, params):
        rid = self.next_id()
        self.send({"id": rid, "method": "mining.submit", "params": params})
        return rid

    def readline(self):
        line = self.fh.readline()
        if not line:
            raise IOError("pool closed the connection")
        return json.loads(line.decode("utf-8", "replace"))


def render_jobs_file(job, meta, history):
    """jobs.txt layout: line 1 = meta JSON, then one JSON job per line, newest first."""
    lines = [json.dumps(meta, separators=(",", ":"))]
    for j in history:
        lines.append(json.dumps(j, separators=(",", ":")))
    return "\n".join(lines) + "\n"


def jobs_writer(store, cfg, job_q, pcs):
    """Thread: rewrite jobs.txt on GitHub whenever a new job arrives."""
    path = cfg["github"]["jobs_file"]
    keep = 5
    history = []
    ranges = nonce_ranges(pcs)
    last_push = 0.0
    min_interval = float(cfg["timing"].get("jobs_poll_seconds", 3))
    while not STOP.is_set():
        try:
            job = job_q.get(timeout=1)
        except queue.Empty:
            continue
        # coalesce: if the pool spammed several jobs, only publish the newest
        while True:
            try:
                job = job_q.get_nowait()
            except queue.Empty:
                break
        wait = min_interval - (time.time() - last_push)
        if wait > 0:
            time.sleep(wait)
        job = dict(job)
        job["received_at"] = int(time.time())
        history = ([job] + history)[:keep]
        meta = {
            "type": "meta",
            "updated_at": int(time.time()),
            "pool": cfg["pool"]["url"],
            "wallet": cfg["pool"]["wallet"],
            "pcs": pcs,
            "nonce_ranges": ranges,
            "current_job_id": job.get("job_id"),
        }
        try:
            store.overwrite(path, render_jobs_file(job, meta, history),
                            "job %s height %s" % (job.get("job_id"), job.get("height")))
            last_push = time.time()
            log("jobs", "-> %s  job_id=%s height=%s target=%s..." %
                (path, job.get("job_id"), job.get("height"), str(job.get("target"))[:14]))
        except Exception as e:
            log("jobs", "push failed: %s" % e)


def shares_watcher(store, cfg, pool, pending):
    """Thread: drain shares.txt on GitHub and submit each share to the pool."""
    path = cfg["github"]["shares_file"]
    interval = float(cfg["timing"].get("shares_poll_seconds", 2))
    wallet = cfg["pool"]["wallet"]
    while not STOP.is_set():
        try:
            if not pool.connected.is_set():
                time.sleep(1)
                continue
            # NOTE: raw.githubusercontent.com is CDN-cached and can lag by minutes,
            # which silently strands shares. Always peek through the API instead.
            peek, _ = store.get_file(path)
            if not peek or not peek.strip():
                time.sleep(interval)
                continue
            lines = store.take_and_clear(path, "consume shares")
            if not lines:
                time.sleep(interval)
                continue
            log("shares", "consumed %d share(s) from %s (file cleared)" % (len(lines), path))
            for raw in lines:
                try:
                    share = json.loads(raw)
                except Exception:
                    log("shares", "skip unparsable line: %.80s" % raw)
                    continue
                params = {k: v for k, v in share.items()
                          if k in ("job_id", "nonce", "result", "hash", "cert",
                                   "cert_version", "height", "worker")}
                params.setdefault("worker", wallet)
                if "job_id" not in params or "nonce" not in params:
                    log("shares", "skip share without job_id/nonce: %.80s" % raw)
                    continue
                try:
                    rid = pool.submit(params)
                    pending[rid] = {"job_id": params["job_id"], "nonce": params["nonce"],
                                    "pc": share.get("pc"), "sent_at": time.time()}
                    log("shares", "submit id=%d job=%s nonce=%s pc=%s" %
                        (rid, params["job_id"], params["nonce"], share.get("pc")))
                except Exception as e:
                    log("shares", "submit failed (%s) - re-queueing share" % e)
                    try:
                        store.append_lines(path, [raw], "requeue share")
                    except Exception as e2:
                        log("shares", "re-queue failed too: %s" % e2)
        except Exception as e:
            log("shares", "watcher error: %s" % e)
            time.sleep(3)


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    ap = argparse.ArgumentParser(description="Pearl pool <-> GitHub connector")
    ap.add_argument("--config", default=os.path.join(here, "config.json"))
    ap.add_argument("--pcs", type=int, default=None, help="number of mining PCs joining")
    ap.add_argument("--ssl", action="store_true", help="use the stratum+ssl endpoint")
    args = ap.parse_args()

    cfg = load_config(args.config)

    pcs = args.pcs
    if pcs is None:
        pcs = cfg.get("cluster", {}).get("pcs")
        try:
            answer = input("How many PCs are joining the pool? [%s]: " % pcs).strip()
            if answer:
                pcs = int(answer)
        except (EOFError, KeyboardInterrupt):
            print()
    pcs = max(1, int(pcs or 1))
    log("setup", "%d PC(s) joining -> nonce space split into %d slice(s)" % (pcs, pcs))
    cfg.setdefault("cluster", {})["pcs"] = pcs
    # Persist the PC count, but re-read the file from disk first and only touch that
    # one field. Writing the in-memory cfg would bake the token (merged in from
    # config.local.json / $GH_TOKEN) into the tracked config and trip GitHub's
    # push protection on the next commit.
    try:
        with open(args.config, "r", encoding="utf-8") as fh:
            on_disk = json.load(fh)
        on_disk.setdefault("cluster", {})["pcs"] = pcs
        on_disk.setdefault("github", {})["token"] = ""
        with open(args.config, "w", encoding="utf-8") as fh:
            json.dump(on_disk, fh, indent=2)
    except Exception as e:
        log("setup", "could not save pcs to config: %s" % e)

    store = store_from_config(cfg)
    store.ensure_file(cfg["github"]["jobs_file"], "")
    store.ensure_file(cfg["github"]["shares_file"], "")
    log("setup", "github %s/%s branch %s" % (cfg["github"]["owner"], cfg["github"]["repo"],
                                             cfg["github"].get("branch", "main")))

    url = cfg["pool"]["ssl_url"] if args.ssl else cfg["pool"]["url"]
    host, port, use_ssl = parse_stratum_url(url)
    pool = PoolClient(host, port, use_ssl, cfg["pool"]["wallet"])

    job_q = queue.Queue()
    pending = {}

    threading.Thread(target=jobs_writer, args=(store, cfg, job_q, pcs), daemon=True).start()
    threading.Thread(target=shares_watcher, args=(store, cfg, pool, pending), daemon=True).start()

    backoff = 2
    while not STOP.is_set():
        try:
            pool.connect()
            backoff = 2
            while not STOP.is_set():
                msg = pool.readline()
                method = msg.get("method")
                if method == "mining.notify":
                    job_q.put(msg.get("params") or {})
                elif method == "mining.set_difficulty":
                    log("pool", "set_difficulty %s" % (msg.get("params"),))
                elif msg.get("id") in pending:
                    info = pending.pop(msg["id"])
                    if msg.get("error"):
                        log("pool", "share REJECTED job=%s nonce=%s -> %s" %
                            (info["job_id"], info["nonce"], msg["error"]))
                    else:
                        log("pool", "share ACCEPTED job=%s nonce=%s pc=%s" %
                            (info["job_id"], info["nonce"], info.get("pc")))
                elif msg.get("id") == 2:
                    log("pool", "authorize -> %s" % msg.get("result"))
                else:
                    log("pool", "<- %.160s" % json.dumps(msg))
        except KeyboardInterrupt:
            break
        except Exception as e:
            log("pool", "disconnected (%s); reconnecting in %ds" % (e, backoff))
            pool.close()
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)

    STOP.set()
    pool.close()
    log("setup", "bye")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        STOP.set()
        print()
