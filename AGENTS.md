# AGENTS.md

## Current state: all-AWS pipeline on Python 3.14 Lambda + live web API

This repository tracks the **all-AWS pipeline, no new platform**. A full Cloudflare (Workers + R2) port was evaluated on 2026-10-29 and **parked** — the Lambda free tier already runs this workload at zero cost, and the curator's write volume would force Workers Paid (~$5/mo) on Cloudflare. The decision: stay on AWS (Lambda + S3). See "Parked evaluation" at the bottom.

Upgrade status (2026-10-29): the pipeline runs on the 3.14 Lambda runtime — `lambda/requirements.txt` bumped to 3.14-era pins, `make runtime` target added, Makefile made venv-self-sufficient, code+test scripts verified offline under Python 3.14 (parser ran real SEF data; MD5 verified), and the live functions updated via `make runtime && make deploy all` (runtime `python3.14`, ~16 MB zips). Next runtime generation: `make RUNTIME=<next> runtime` followed by `make deploy all`.

API status (2026-11-01): the **cc2asn-db web API is implemented and live** — `api/` (see `api/README.md` for endpoints, rate limits, deployment). Function `CC2ASN-api` (Event Function + adapter layer v30, python3.14) with public Function URL; latest-day lookups free + rate-limited. **Scope change (2026-11-10, deployed as version 15 + DynamoDB TTL on `expires_at` enabled, smoke-verified via api.cc2asn.com and the TCP 43 bridge): latest day only, no historic lookups, no tokens, no paid tier.** Date-prefixed paths answer 404 "Historic lookups are not available"; token code (`lib/tokens.py`, `scripts/issue_token.py`) removed (in git history). Rate limit: **max 100 queries per user/device (client IP, IPv6 per /64) per UTC day**, enforced across warm environments by an atomic DynamoDB counter in `cc2asn-tokens` (`rl#<date>#<hashed client>`, `expires_at` TTL attribute; fails open on DynamoDB errors), plus the in-memory 30/min burst limit. The Pro-tier spec + implementation plan are **parked** (kept for a possible revisit): `docs/superpowers/specs/2026-11-09-pro-tier-design.md`, `docs/superpowers/plans/2026-11-10-pro-tier.md`. Query logging to `/cc2asn/api/latest` (legacy whois format + ` src=http|tcp43` marker, UTC), 180-day retention; `/cc2asn/api/historic` is no longer written (ages out). Costs tracked within free tiers; design/history in `docs/2026-10-29-cc2asn-api-design.md`. **Stable public alias live (2026-11-03): `api.cc2asn.com`** — CloudFront distribution (placeholder-clean template `api/cloudfront-distribution.json`; gitignored live copy `.live.json` with real host/ARN/secret) in front of the Function URL, ACM cert us-east-1, origin header `X-CC2ASN-Proxy: <CC2ASN_PROXY_SECRET>` marks proxy-trusted requests so client IP = last XFF entry (same `_client_ip()` path as the bridge; verified: log shows user IP with `src=http`, CloudFront edge IP never leaks). Function URL stays public — old direct links unaffected. Unknown-CC lookups fixed (2026-11-03, deployed): S3 returns AccessDenied for nonexistent keys when the caller lacks s3:ListBucket; `_fetch` treats it as no-data → 404 `Not found: <path>` (also covers historic), banner reads "CC2ASN WEB API".

Bridge status (2026-11-03): the **whois-web API bridge is implemented (`bridge/`), deployed and verified live** — Python 3 stdlib port of `legacy/cc2asn-server.py`, TCP 43 → HTTPS API (ops record incl. gcloud provisioning in `bridge/README.md`). Running on a GCP e2-micro VM (`cc2asn-bridge`, us-east1-b, static IP `35.196.91.34`, behind firewall rule `allow-whois-43`); old rambase address `81.4.121.45` (TTL ~66 s as of 2026-11-04). `CC2ASN_BRIDGE_SECRET` is set on the live `CC2ASN-api` (env 2026-11-03, bridge-support code deployed same day — code deploy is what activates bridge-side headers; env alone is silently limp pre-deploy). Neutral smoke verified end-to-end (`/cc2asn/api/latest` shows client IP with `src=tcp43`). **DNS cutover done (verified 2026-11-14)**: the `cc2asn.com` apex A record (Cloudflare) points at `35.196.91.34`, **grey cloud mandatory** (orange can't carry TCP 43); rollback = flip the record back to rambase `81.4.121.45`, which stays running (idle fallback) until it is cancelled in **October 2027**. Apex web redirect (2026-11-14): Caddy on the VM answers 80/443 for `cc2asn.com` with a 301 to `https://www.cc2asn.com` (Cloudflare Worker `cc2asn-website`), Let's Encrypt cert auto-renewed; needs firewall rule `allow-http-redirect` (tcp:80,443) to stay open. `default-allow-rdp` deleted. GCP project ID is `project-64a38f38-315b-4bd8-92b` (display name CC2ASN; `cc2asn-bridge` is the VM name). Verification: `echo fi | nc cc2asn.com 43` + `aws logs tail /cc2asn/api/latest --region eu-west-1 --since 2m` showing real user IPs with `src=tcp43` (Apple macOS caveat: avoid `whois -h <IP> <single-arg>` — client-side segfault, see `bridge/README.md` smoke section). Rotation/retirement + monthly decay check: `bridge/README.md`. Bridge-plan: `/Users/toringe/.opencode/plan/cc2asn-whois-webapi-bridge.md` (decisions also summarized in the API design doc, open item 4).

Production reality check: the deployed Lambda code uses plain boto3 S3 clients (no custom `endpoint_url`), so the live system writes to **real AWS S3** (`cc2asn-data` / `cc2asn-db`, eu-west-1). Same-named R2 buckets exist from the evaluation, but nothing in the pipeline uses them — treat them as dormant copies.

**Skills:** in this repository, load the relevant `aws-*` skills (Lambda, S3, IAM, Serverless) before AWS-specific design or code. The repo's installed Cloudflare skills are not needed unless the parked port is reopened.

## Repo layout

- `lambda/` — the **deployed** AWS Lambda pipeline (downloader / parser / curator, Makefile, test scripts). This is live production code (see Build & deploy below). It used to live under `legacy/lambda/` and was moved back to the repo root.
- `bridge/` — the **whois-web API bridge** (TCP 43 → HTTPS API proxy, runs on a GCP free-tier VM in us-east1; deployed config lives on the VM, `/etc/cc2asn-bridge.conf`). Implemented and tested, deployment pending; ops record in `bridge/README.md`.
- `legacy/` — historical code only: the old Python 2 server (Upstart, configobj, naturalsort).
- `example-s3/` — **local only, gitignored**: small sample mirror of the two production S3 buckets (`cc2asn-data/` with `RIR-SEF/`, `parsed/`, `metrics/`; `cc2asn-db/` with date-pathed per-country files). Use for local testing/exploration instead of hitting live S3. Never commit or sync it. The real `cc2asn-db` bucket holds historical data back to 2019; the sample only contains a couple of recent dates. The folder also contains scratch files from manual exploration (test scripts, logs, charts) — treat them as throwaway.
- `misc/` — architecture diagrams (drawio + png). No code.

## Pipeline (lambda/)

Event-driven flow, one file per function:

1. **downloader.py** — downloads a RIR SEF delegation file (URL from event), verifies MD5, uploads to `s3://cc2asn-data/RIR-SEF/`. Invoked manually (e.g. scheduled) with `{"url": ..., "loglevel": ...}`.
2. **parser.py** — triggered by EventBridge (S3 ObjectCreated on `cc2asn-data`). Parses SEF (RIR Statistics Exchange Format) into per-country JSON, writes `parsed/<basename>.json` to the same bucket. Log level comes from the `LogLevel` env var (not the event).
3. **curator.py** — triggered by the parser's S3 write, but the trigger only signals "something changed": every run reads **all five** `parsed/delegated-<rir>-extended-latest.json` files, merges them per country (union, de-duplicated — a country can appear in several registries' files, e.g. US in ARIN/RIPE/APNIC/LACNIC), and syncs per-country files (`<CC>_<TYPE>` and `<CC>_ALL`) to `s3://cc2asn-db` under `yyyy/mm/dd/` (newest RIR generation date) and `latest/`. The sync skips files whose MD5 matches the existing ETag (bucket is SSE-S3, single-part PUTs) and deletes keys no longer in the merged set. It refuses to publish if any parsed file is missing or empty. (Before 2026-11-11 it wrote one registry's data per run, so the last registry to run overwrote the others' records for shared countries; dated snapshots written before then are affected.) Offline unit tests: `cd lambda && uv run python -m unittest discover -s test -t .`.

AWS config (`REGION`, `BUCKET`, `PREFIX`) is hardcoded at the top of each module — no environment config. Keep it consistent if changing regions/buckets.

## Build & deploy (lambda/)

- `make <function>` — build zip only. `make deploy <function>` — build and run `aws lambda update-function-code`. `make clean` removes `build/`.
- `make runtime` (or `make runtime-<function>`) — set the deployed functions' Lambda runtime to `RUNTIME = python3.14` (no code upload). Run it **once, before the first deploy after a runtime upgrade** — `update-function-configuration` and `update-function-code` are separate AWS calls. Future runtime generations: `make RUNTIME=<next> runtime`.
- The Makefile is **venv-self-sufficient**: it builds from the repo's uv venv by default (`VENV ?= ../.venv`; activate nothing, no `VIRTUAL_ENV` needed — override with `make VENV=/path`). The freeze is `uv pip freeze --python $(PY)` and site-packages is resolved via `sysconfig`, so builds are identical from any shell. It still **overwrites `requirements.txt`** on every build (backs up to `requirements.bkp`), and hard-fails with instructions if the venv interpreter is missing (build targets only; `clean`/`runtime` needs no venv).
- The deploy target runs **`pcregrep`** to read `REGION = "..."` from the function's source file, so `pcregrep` must be installed and the `REGION` line format in the `.py` must be preserved (`REGION = "xx-yy-N"`, double quotes).
- Deployed function names are `CC2ASN-{downloader,parser,curator}`; region is `eu-west-1` (from the sources). Zip = the single function file + venv `site-packages`; the 3.14-era build is ~16 MB (under the 50 MB direct-upload limit).

## Python environment

- `.python-version` pins `3.14`; the environment is uv-managed and the venv lives in `.venv/` (gitignored). Use `uv run python ...` or `source .venv/bin/activate`; `uv` auto-installs the pinned interpreter.
- Lambda runtime: **Python 3.14** (`python3.14`), the newest GA Lambda runtime (3.15 is public preview only; GA targeted ~November 2026 — functions on the preview identifier graduate automatically).
- Dev deps: `boto3`, `natsort` (installed via `uv pip install`). `lambda/requirements.txt` now pins **Python 3.14-era** versions (boto3/botocore 1.43.119, natsort 8.4.0, urllib3 2.8.0) — it documents what the built zip bundles; the Makefile re-freezes it from `.venv` on every build, so it needs no manual upkeep for the dev environment.
- No linter, formatter, typecheck, or CI configuration exists.

## Local testing

- `test_local_*.py` in `lambda/` are standalone scripts (no pytest): run them directly from `lambda/` (`uv run python test_local_parser.py`) since they import sibling modules. They execute the real handler with a sample event and log to stdout.
- They call **live AWS S3** (cc2asn-data / cc2asn-db) and real RIR FTP servers — valid AWS credentials are required. They are integration tests, not unit tests.
- Alternative RIR URLs for the downloader are commented out inside `test_local_downloader.py`.

## Parked evaluation: Cloudflare port (2026-10-29)

A complete, behavior-preserving Cloudflare implementation plan was produced and verified (three Python Workers + R2 + Queues replacing EventBridge; R2 `put(md5=…)` integrity option replacing local MD5 calc; forced **Workers Paid** because the curator writes ~2k R2 objects/RIPE-file vs the free tier's 50-subrequest cap). It was **not chosen**: the pipeline already runs free on AWS Lambda + S3, so the port would cost ~$5/mo purely for platform consolidation. If consolidation is ever wanted, the archived plan cuts the effort sharply (all docs' load-bearing facts are verified) — see `docs/2026-10-29-cloudflare-port-parked.md`.

## Open follow-ups (from `docs/2026-10-29-cc2asn-api-design.md`)

**Git state (2026-11-04): PR #3 (`dev` → `master`) MERGED** — master carries
the full 2026-11-03/04 work (bridge, `api.cc2asn.com` alias, unknown-CC fix,
`__pycache__` hygiene). Two later docs commits (`544d3e6`, `4e54675`) hit
`dev` after the merge; they will ride along on the next dev→master merge.
`dev` is pushed to origin (verify with `git status -sb` / `gh pr view 3`).

The cc2asn-db Web API is implemented and live (see "API status" above).
The 2026-11-10 scope change is deployed (version 15, TTL enabled). The one
leftover token item (`smoketest`, expired 2026-11-02) is inert and can be
deleted at leisure.
Remaining follow-ups: cancel the rambase VPS (legacy TCP whois server) in
October 2027 (DNS cutover to the bridge is done, 2026-11-14), and re-checking Lambda Web Functions as the form factor
whenever their free-tier status changes. Commercial/historic access is parked
(see the Pro-tier spec + plan).
