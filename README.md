# prl-pool-bridge

Mine **Pearl (PRL)** on Kryptex with the **krig** GPU miner, with GitHub sitting in the middle as the message bus.

```
 Kryptex pool                      GitHub repo                        mining PC(s)
 prl.kryptex.network:7048          jobs.txt   ──────────────►  pearl_miner.py ──► krig-miner (GPU)
        ▲   │  mining.notify          ▲                                │
        │   └──────────────────► pool_connector.py                     │ mining.submit
        └──────── mining.submit ◄──  shares.txt  ◄─────────────────────┘
```

* **`pool_connector.py`** — the only machine that talks to Kryptex. It receives jobs, rewrites them into `jobs.txt`, and watches `shares.txt`; the moment that file has content it consumes it (read + clear in one commit) and submits every share to the pool, then waits for the next one.
* **`pearl_miner.py`** — runs on each rig. It reads `jobs.txt`, feeds the jobs to `krig-miner` over a local stratum listener, and appends every solution krig finds to `shares.txt`.

---

## Quick start

```bash
git clone https://github.com/cvfbgngfgbhnhgfrgb/prl-pool-bridge
cd prl-pool-bridge
export GH_TOKEN=ghp_xxxxxxxx          # required — the token is never committed
```

Or drop it in `config.local.json` (gitignored) instead of exporting it:

```json
{ "github": { "token": "ghp_xxxxxxxx" } }
```

**On the connector machine** (any box with internet — no GPU needed):

```bash
python3 pool_connector.py             # asks: how many PCs are joining?
python3 pool_connector.py --pcs 4     # or pass it directly
python3 pool_connector.py --ssl       # use stratum+ssl://prl.kryptex.network:8048
```

**On each mining PC** (needs an NVIDIA Ampere/Ada/Blackwell or AMD RDNA2+ GPU):

```bash
./get_krig.sh                # downloads krig 1.2.0 (Linux x64)
python3 pearl_miner.py --pc 0    # second rig uses --pc 1, third --pc 2, ...
```

That's it. Watch `jobs.txt` and `shares.txt` update in the repo.

---

## File formats

**`jobs.txt`** — line 1 is a meta record, then the 5 most recent jobs, newest first:

```json
{"type":"meta","updated_at":1786792965,"pool":"stratum+tcp://prl.kryptex.network:7048","wallet":"krxYRPV4WQ.0x","pcs":4,"nonce_ranges":[{"pc":0,"nonce_start":"0000000000000000","nonce_end":"3fffffffffffffff"}],"current_job_id":"7e67bb63_2097152"}
{"header":"00004020...","height":100110,"job_id":"7e67bb63_2097152","target":"00000000000007ff...","cert_version":3,"received_at":1786792965}
```

**`shares.txt`** — one JSON share per line, appended by the miners, cleared by the connector:

```json
{"job_id":"7e67bb63_2097152","nonce":"00000000deadbeef","worker":"krxYRPV4WQ.0x","pc":0,"found_at":1786792875}
```

---

## Notes from building this against the live pool

* **The pool's stratum dialect is not classic stratum.** `mining.notify` and `mining.submit` carry JSON **objects**, not positional arrays. Array-style submits are answered with `[20,"Unsupported submit format"]`. Verified live; `pool_connector.py` speaks the object dialect.
* **`mining.subscribe` gets no reply** on this pool — only `mining.authorize` is answered, and jobs start flowing right after. The client does not block waiting on a subscribe response.
* **Never read state from `raw.githubusercontent.com`.** It is CDN-cached and served a stale empty `shares.txt` during testing, stranding shares indefinitely. Both pollers use the authenticated Contents API instead.
* **Concurrent writes are safe.** `shares.txt` is written with optimistic locking on the blob sha; if two rigs append at once GitHub rejects the stale write with a 409 and the client re-reads and retries, so no share is lost or consumed twice.
* **krig 1.2.0 is TLS-only and has no nonce-range flags** (`--url --user --password --devices --api-port --log-level` only). So `pearl_miner.py` serves TLS locally with a self-signed cert it generates on first run, and rigs are separated by worker name (`wallet/pc0`, `wallet/pc1`, …). The nonce ranges are still published in `jobs.txt` for miners that can consume them.

## Latency caveat, worth knowing before you rely on this

Routing shares through GitHub adds roughly **4–10 seconds** between krig finding a solution and Kryptex seeing it (commit + poll + commit). Pearl blocks arrive about every 305 s and the pool expires jobs quickly, so a share that arrives after its job rotates is rejected with `[21,"Job not found"]`. For actual earnings, point krig straight at `stratum+ssl://prl.kryptex.network:8048`. This bridge is the right tool when you specifically want the jobs/shares flow to be visible and auditable in a repo — not when you want maximum accepted-share rate.

## Config

`config.json` holds the GitHub target, pool URL, wallet, and timings — but **not** the token: GitHub's push protection rejects any commit containing a PAT, so the token comes from `$GH_TOKEN` or the gitignored `config.local.json`.

The token you supplied is already written to `config.local.json` on the machine this was built on; it is excluded from the repo by `.gitignore`. Since it was shared in plaintext, rotate it at <https://github.com/settings/tokens> when you're done testing.

## Requirements

Python 3.8+, standard library only — no `pip install`. `openssl` for the local TLS cert (or run `pearl_miner.py --plain`).
