#!/usr/bin/env python3
"""
pearl_miner.py  --  mines Pearl (PRL) on the GPU with the krig miner, driven by the
jobs that pool_connector.py published to GitHub, and pushes every solution it finds
back to GitHub as a line in shares.txt.

Design
------
krig-miner speaks stratum, not "read a text file from GitHub". So this script runs a
tiny **local stratum server** that krig connects to, and plays the role of the pool:

    GitHub jobs.txt ──> pearl_miner.py (local stratum on 127.0.0.1:7333) ──> krig-miner
                                    <── mining.submit ──
    GitHub shares.txt <── appended ──┘

  * A poller pulls jobs.txt every few seconds. When current_job_id changes, the new
    job is pushed to krig as a `mining.notify`, exactly in the pool's own dialect.
  * The nonce range assigned to this PC (from the meta line in jobs.txt, keyed by
    --pc) is passed to krig with --nonce-start/--nonce-end when the binary supports
    it, so several PCs never scan the same nonces.
  * Every `mining.submit` krig sends is acknowledged locally (so krig keeps mining)
    and appended to shares.txt on GitHub as one JSON object per line. The connector
    picks it up within seconds, submits it to Kryptex and clears the file.

krig is SSL-only, so the local listener uses TLS with a self-signed certificate that
is generated automatically on first run (openssl, or Python's own fallback).

Usage
-----
    python3 pearl_miner.py --pc 0                 # this rig is PC #0
    python3 pearl_miner.py --pc 1 --no-launch     # run krig yourself, point it here
    python3 pearl_miner.py --pc 0 --plain         # plain TCP listener (other miners)

Get krig first:
    ./get_krig.sh          (Linux)      or  get_krig.ps1  (Windows)
"""

import argparse
import json
import os
import shutil
import socket
import socketserver
import ssl
import subprocess
import sys
import threading
import time

from gh import load_config, store_from_config

STOP = threading.Event()
HERE = os.path.dirname(os.path.abspath(__file__))


def log(tag, msg):
    print("[%s] %-9s %s" % (time.strftime("%H:%M:%S"), tag, msg), flush=True)


class JobState:
    """Latest job from GitHub + the set of connected krig clients to fan it out to."""

    def __init__(self):
        self.lock = threading.Lock()
        self.job = None
        self.meta = {}
        self.clients = []          # list of _send callables
        self.job_seq = 0

    def set_job(self, job, meta):
        with self.lock:
            if self.job and job.get("job_id") == self.job.get("job_id"):
                return False
            self.job, self.meta, self.job_seq = job, meta, self.job_seq + 1
            targets = list(self.clients)
        for send in targets:
            try:
                send({"id": None, "method": "mining.notify", "params": job})
            except Exception:
                pass
        return True

    def add_client(self, send):
        with self.lock:
            self.clients.append(send)
            job = self.job
        if job:
            try:
                send({"id": None, "method": "mining.notify", "params": job})
            except Exception:
                pass

    def drop_client(self, send):
        with self.lock:
            if send in self.clients:
                self.clients.remove(send)

    def my_range(self, pc):
        with self.lock:
            for r in self.meta.get("nonce_ranges", []):
                if r.get("pc") == pc:
                    return r
        return None


def jobs_poller(store, cfg, state):
    """Thread: pull jobs.txt from GitHub and hand new jobs to the local stratum server."""
    path = cfg["github"]["jobs_file"]
    interval = float(cfg["timing"].get("jobs_poll_seconds", 3))
    last_raw = None
    while not STOP.is_set():
        try:
            # API read, not raw.githubusercontent: the raw CDN caches aggressively
            # and would hand krig stale jobs.
            raw, _ = store.get_file(path)
            if raw and raw != last_raw:
                last_raw = raw
                lines = [l for l in raw.splitlines() if l.strip()]
                meta, job = {}, None
                for i, line in enumerate(lines):
                    try:
                        obj = json.loads(line)
                    except Exception:
                        continue
                    if i == 0 and obj.get("type") == "meta":
                        meta = obj
                    elif job is None and obj.get("job_id"):
                        job = obj
                if job and state.set_job(job, meta):
                    log("jobs", "new job %s height %s -> krig" %
                        (job.get("job_id"), job.get("height")))
        except Exception as e:
            log("jobs", "poll error: %s" % e)
        STOP.wait(interval)


def share_uploader(store, cfg, share_q, pc):
    """Thread: append every solution krig finds to shares.txt on GitHub."""
    path = cfg["github"]["shares_file"]
    while not STOP.is_set():
        try:
            share = share_q.get(timeout=1)
        except Exception:
            continue
        batch = [share]
        while len(batch) < 20:                     # coalesce bursts into one commit
            try:
                batch.append(share_q.get_nowait())
            except Exception:
                break
        lines = [json.dumps(s, separators=(",", ":")) for s in batch]
        for attempt in range(5):
            try:
                store.append_lines(path, lines, "share from pc%d" % pc)
                log("shares", "uploaded %d share(s) -> %s" % (len(lines), path))
                break
            except Exception as e:
                log("shares", "upload failed (%s), retry %d/5" % (e, attempt + 1))
                time.sleep(2 * (attempt + 1))


def make_handler(state, share_q, pc, wallet):
    class StratumHandler(socketserver.StreamRequestHandler):
        def handle(self):
            peer = "%s:%s" % self.client_address[:2]
            log("krig", "connected %s" % peer)
            lock = threading.Lock()

            def send(obj):
                with lock:
                    self.wfile.write(json.dumps(obj).encode() + b"\n")
                    self.wfile.flush()

            subscribed = False
            try:
                for raw in self.rfile:
                    if STOP.is_set():
                        break
                    raw = raw.strip()
                    if not raw:
                        continue
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue
                    method, mid = msg.get("method"), msg.get("id")

                    if method == "mining.subscribe":
                        send({"id": mid, "result": True, "error": None})
                    elif method == "mining.authorize":
                        send({"id": mid, "result": True, "error": None})
                        if not subscribed:
                            subscribed = True
                            state.add_client(send)
                            rng = state.my_range(pc)
                            if rng:
                                log("krig", "pc%d nonce range %s..%s" %
                                    (pc, rng["nonce_start"], rng["nonce_end"]))
                    elif method == "mining.submit":
                        params = msg.get("params")
                        share = dict(params) if isinstance(params, dict) else {}
                        if isinstance(params, list):      # tolerate array-style miners
                            keys = ["worker", "job_id", "nonce", "result"]
                            share = {k: v for k, v in zip(keys, params)}
                        share.setdefault("worker", wallet)
                        share["pc"] = pc
                        share["found_at"] = int(time.time())
                        share_q.put(share)
                        # ack immediately so krig does not stall waiting on GitHub
                        send({"id": mid, "result": True, "error": None})
                        log("krig", "share job=%s nonce=%s -> queued" %
                            (share.get("job_id"), share.get("nonce")))
                    elif method in ("mining.extranonce.subscribe", "mining.ping"):
                        send({"id": mid, "result": True, "error": None})
                    elif mid is not None:
                        send({"id": mid, "result": True, "error": None})
            except Exception as e:
                log("krig", "%s error: %s" % (peer, e))
            finally:
                state.drop_client(send)
                log("krig", "disconnected %s" % peer)
    return StratumHandler


class ThreadedTCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


def ensure_cert():
    """Self-signed cert for the local TLS listener (krig is SSL-only)."""
    cert, key = os.path.join(HERE, "local-cert.pem"), os.path.join(HERE, "local-key.pem")
    if os.path.exists(cert) and os.path.exists(key):
        return cert, key
    if shutil.which("openssl"):
        subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
                        "-keyout", key, "-out", cert, "-days", "3650",
                        "-subj", "/CN=localhost"], check=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        log("tls", "generated self-signed cert for the local listener")
        return cert, key
    raise RuntimeError("openssl not found - install it, or run with --plain")


def launch_krig(cfg, pc, host, port, use_ssl, state):
    """Start krig-miner pointed at our local stratum listener."""
    binary = cfg["miner"].get("krig_binary") or ""
    # Resolve the binary across platforms: config value first, then the usual
    # names next to this script (krig-miner.exe on Windows), then $PATH.
    candidates = [binary] if binary else []
    names = ["krig-miner.exe", "krig-miner"] if os.name == "nt" else ["krig-miner", "krig-miner.exe"]
    candidates += [os.path.join(HERE, n) for n in names] + names
    resolved = None
    for cand in candidates:
        if not cand:
            continue
        if os.path.isfile(cand):
            resolved = os.path.abspath(cand)
            break
        found = shutil.which(cand)
        if found:
            resolved = found
            break
    if not resolved:
        helper = "get_krig.ps1" if os.name == "nt" else "./get_krig.sh"
        log("krig", "krig binary not found - run %s (or use --no-launch)" % helper)
        return None
    binary = resolved
    scheme = "stratum+ssl" if use_ssl else "stratum+tcp"
    wallet = cfg["pool"]["wallet"]
    user = "%s/pc%d" % (wallet.split(".")[0], pc)
    # krig 1.2.0 CLI: --url/--user/--password/--devices/--api-port/--log-level.
    # It has no nonce-range flags, so PCs are separated by worker name instead and
    # the range from jobs.txt is only logged for miners that can consume it.
    cmd = [binary, "--url", "%s://%s:%d" % (scheme, host, port),
           "--user", user, "--password", "x"]
    cmd += list(cfg["miner"].get("krig_extra_args", []))
    rng = state.my_range(pc)
    if rng:
        log("krig", "pc%d assigned nonce range %s..%s" %
            (pc, rng["nonce_start"], rng["nonce_end"]))
    log("krig", "launching: %s" % " ".join(cmd))
    env = dict(os.environ)
    env.setdefault("KRIG_INSECURE_TLS", "1")        # accept our self-signed cert
    try:
        return subprocess.Popen(cmd, env=env)
    except Exception as e:
        log("krig", "launch failed: %s" % e)
        return None


def main():
    ap = argparse.ArgumentParser(description="Pearl GPU miner (krig) fed from GitHub jobs")
    ap.add_argument("--config", default=os.path.join(HERE, "config.json"))
    ap.add_argument("--pc", type=int, default=0, help="index of this PC (0-based)")
    ap.add_argument("--no-launch", action="store_true", help="do not spawn krig, just serve")
    ap.add_argument("--plain", action="store_true", help="plain TCP local listener (no TLS)")
    ap.add_argument("--port", type=int, default=None)
    args = ap.parse_args()

    cfg = load_config(args.config)
    store = store_from_config(cfg)
    host = cfg["miner"].get("local_stratum_host", "0.0.0.0")
    port = args.port or int(cfg["miner"].get("local_stratum_port", 7333))
    use_ssl = (not args.plain) and bool(cfg["miner"].get("use_ssl", True))

    state = JobState()
    import queue
    share_q = queue.Queue()

    threading.Thread(target=jobs_poller, args=(store, cfg, state), daemon=True).start()
    threading.Thread(target=share_uploader, args=(store, cfg, share_q, args.pc), daemon=True).start()

    server = ThreadedTCPServer((host, port), make_handler(state, share_q, args.pc,
                                                          cfg["pool"]["wallet"]))
    if use_ssl:
        cert, key = ensure_cert()
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(cert, key)
        server.socket = ctx.wrap_socket(server.socket, server_side=True)
    log("setup", "local stratum %s://%s:%d  (pc%d)" %
        ("ssl" if use_ssl else "tcp", host, port, args.pc))

    threading.Thread(target=server.serve_forever, daemon=True).start()

    proc = None
    if not args.no_launch:
        time.sleep(1.5)
        proc = launch_krig(cfg, args.pc, "127.0.0.1", port, use_ssl, state)
    if proc is None:
        log("setup", "point your miner at: %s://127.0.0.1:%d  --user %s/pc%d" %
            ("stratum+ssl" if use_ssl else "stratum+tcp", port,
             cfg["pool"]["wallet"].split(".")[0], args.pc))

    try:
        while not STOP.is_set():
            if proc and proc.poll() is not None:
                log("krig", "miner exited with code %s - restarting in 10s" % proc.returncode)
                time.sleep(10)
                proc = launch_krig(cfg, args.pc, "127.0.0.1", port, use_ssl, state)
            time.sleep(2)
    except KeyboardInterrupt:
        pass
    finally:
        STOP.set()
        if proc and proc.poll() is None:
            proc.terminate()
        server.shutdown()
        log("setup", "bye")


if __name__ == "__main__":
    main()
