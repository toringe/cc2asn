# cc2asn-db Web API — design (saved for later implementation)

**Status: design agreed 2026-10-29 (same-day revisions: form factor switched from Web Functions to Event Function + Lambda Web Adapter; language switched from Node to Python once the Node-only constraint disappeared), NOT implemented.** See also `docs/2026-10-29-cloudflare-port-parked.md`.

## Goal

Serve the per-country data in `cc2asn-db` as an HTTP API, replacing the legacy whois server's *semantics* over time:

- Same functionality as the current whois server (`legacy/cc2asn-server.py`): query `CC` → ASN list; optional record type `ASN|IPV4|IPV6|ALL`. Responses stay plain-text for whois compatibility.
- **Latest-day lookups: free, anonymous, rate-limited.**
- **Historic lookups** (data back to 2019 in `cc2asn-db`): commercial potential — require an auth token with per-token quota.
- Migration path: a future **whois-web bridge** on a VM forwards incoming port-43 whois requests to this HTTP API; when whois traffic is sufficiently low, the old server retires.

## Decisions made

| Question | Decision |
|---|---|
| API surface | **HTTP(S) only.** Neither form factor can serve port 43; the whois server keeps running until the bridge retires it. |
| Historic missing dates | **Exact date or 404.** No nearest-earlier fallback. |
| Compute form factor | **Lambda Event Function (`python3.14`) behind the `aws-lambda-web-adapter` layer**, front door = **Function URL** (free, no API Gateway). Chosen over native Web Functions **because of the free-tier goal** — see "Why not Web Functions" below. |
| Implementation language | **Python** — chosen once the Web Adapter removed the Web-Functions Node-only constraint. Keeps the repo single-language: uv venv, Makefile conventions, boto3 patterns all match `lambda/`. Node's only edge (slightly faster cold start, ~200 ms) is noise at this traffic. |
| Application stack | **Python stdlib `http.server`** — no FastAPI/Flask/uvicorn. Each Event Function environment handles one request at a time anyway, so a framework adds cold-start weight for nothing; the API surface is 6 endpoints. Deps: `boto3` only. |
| Token/quota store | **DynamoDB in provisioned mode** (never on-demand) for tokens + per-token quota — this is what fits inside the always-free tier. |
| Rate limiting | No API Gateway in front, so limits live in the app: per-IP in-memory (approximate, per warm environment) for anonymous; per-token **precise** via DynamoDB atomic counters (doubles as billing usage data). |
| Token issuance | Manual, via CLI script. No payment integration yet — commerce hooks deferred. |

## Why not Web Functions (and how to reverse)

- AWS recommends Web Functions for new Node.js apps (active-CPU billing, in-environment concurrency, native streaming); Node is also their only launch runtime, so the original design defaulted to Node.
- **But Web Functions are excluded from the Lambda free tier** (verified on the Lambda pricing page, 2026-10-29), and they meter **memory for as long as a warm environment exists** (2 GB fixed) — an unbounded idle-time cost floor, worst case ~$19/mo per permanently warm environment.
- The adapter approach bills GB-seconds *per request* and sits inside the **always-free 1M requests + 400K GB-s/month**:
  - At 512 MB and ~60 ms/request (S3-wait dominated), the free 400K GB-s covers **~13 million requests/month** — effectively un-billable at this workload's scale.
- **Reversal path:** the app is a plain HTTP server either way; the adapter is a layer + `AWS_LWA_EXEC_WRAPPER=/opt/bootstrap` outside the code. If Web Functions later gain a free tier (or commercial scale makes active-CPU billing the winner), the app must be ported to Node.js (Web Functions remain Node-only) — re-check that constraint at switch time rather than assuming a free port. Re-checkpoint: when/if Web Functions get free-tier coverage.

## Cost analysis (verified 2026-10-29)

| Component | Cost mechanism | Free tier | Estimate at cc2asn scale |
|---|---|---|---|
| Function URL | Free HTTPS front door (buffered invoke; ~100 req/s avg soft limit — ample here) | ✅ | **$0** |
| Event Function (arm64, 512 MB) | $0.20/M req + $0.0000133334/GB-s billed duration | ✅ 1M req + 400K GB-s always-free | **$0** up to ~13M req/mo at 60 ms/req |
| DynamoDB | **Provisioned mode**: 25 GB + 25 RCU + 25 WCU always-free (on-demand mode is *not* free) | ✅ | **$0** if ≤ 25 units |
| S3 `GetObject` | $0.004/1k; no always-free allowance | ❌ | `latest/` cached in memory; historic direct reads → ~$0.20/mo at 50k historic lookups |
| Adapter layer | Managed AWS layer, free | ✅ | $0 |

Realistic total: **≈ $0.00–0.30/month** (S3 GET cents dominate). CloudWatch cost guard stays as a safety net.

## Architecture

One Event Function in `eu-west-1`, `python3.14`, zip-packaged:

- **Architecture:** arm64 by default (≈20% cheaper per GB-s beyond free tier; identical within it). Verify SnapStart architecture support *before* adopting SnapStart as a cold-start mitigation (support matrix may be x86_64-limited) — SnapStart is optional anyway given the app's trivial init.
- **Packaging:** AWS-managed layer (`arn:aws:lambda:${REGION}:753240598075:layer:LambdaAdapterLayerArm64:<ver>`), `AWS_LWA_EXEC_WRAPPER=/opt/bootstrap`, handler = startup script (`run.sh` → `python3 server.py`). App binds `AWS_LWA_PORT`/`PORT` (default 8080). Zip built from the repo's uv venv (`boto3` only), mirroring the `lambda/` Makefile conventions.
- **Configuration via env vars** (no hardcoded config): `PORT`, `CC2ASN_DB_BUCKET` (default `cc2asn-db`), `AWS_REGION` (runtime-provided), `CC2ASN_TOKENS_TABLE`, rate-limit knobs (`RATE_PER_MIN`, `RATE_PER_DAY`), cache TTL.
- An execution role with least-privilege read on `cc2asn-db` + read/write on the tokens table.
- Deploy via `api/Makefile` mirroring the `lambda/` conventions.

```
api/
├── server.py           # stdlib http.server + request routing
├── run.sh              # adapter startup script
├── lib/
│   ├── whois.py        # CC/type validation, response rendering
│   ├── s3data.py       # boto3 S3 reads + latest/ in-memory cache (TTL refresh)
│   ├── tokens.py       # DynamoDB token validation (hash lookup, quota)
│   └── ratelimit.py    # in-memory per-IP token bucket + DynamoDB per-token counter
├── scripts/issue-token.py   # CLI: create/revoke token (hash stored, plaintext printed once)
├── test/               # stdlib unittest (S3/DDB stubbed) + example-s3 fixture tests
├── Makefile
├── requirements.txt
└── README.md
```

## API surface (whois semantics)

- `GET /` → service banner text
- `GET /{CC}` → contents of `latest/{CC}_ASN` (plain text, whois parity)
- `GET /{TYPE}/{CC}`, TYPE ∈ {ASN, IPV4, IPV6, ALL} → `latest/{CC}_{TYPE}`
- **Historic:** same paths prefixed with `/YYYY-MM-DD/` → exact S3 key `yyyy/MM/dd/{CC}_{TYPE}`; no file → **404** (body names the missing key)
- `?format=json` optional on any lookup; plain text is the default
- Latest lookups: free + anonymous, `X-Data-Date` response headers
- Historic lookups: `Authorization: Bearer <token>`, daily quota enforced server-side

## Tokens & rate limits

- Single DynamoDB table `cc2asn-tokens`, PK = SHA-256(token); attributes: name, tier (`historic` / `historic-pro`), daily quota, remaining-today counter (date-partitioned with TTL — no cron), expiry.
- Validation = 1 GetItem per authorized request (comfortably inside 25 RCU); quota = atomic conditional `Update` on the remaining counter (also the billing usage data later).
- Free/anonymous: in-memory per-IP token bucket (defaults proposed: 30 req/min, 300/day — configurable via env), 429 + `Retry-After`. Approximate per warm environment; per-IP counters can move to DynamoDB later if abuse demands.

## Testing & ops

- Tests: stdlib `unittest` (repo has no pytest; standalone-script convention matches `test_local_*.py`), all AWS I/O stubbed; fixture tests render real files from `example-s3/`; one live smoke script post-deploy.
- Local dev: `python server.py` (the app is just a server — runs the same with or without the adapter).
- CloudWatch basic logging (low volume, free tier); alarms: 5xx rate + monthly cost guard.
- Relevant skills at implementation time: `aws-sdk-python-usage` (boto3 code), `aws-auth`-lite patterns excluded (custom token scheme, no Cognito).

## Open items for implementation time

1. Confirm the exact adapter invoke-mode env for Function URLs (buffered/streaming, Function-URL payload handling; README lists support — resolve remaining env nuance then), keeping the layer version pinned.
2. Verify SnapStart architecture support if a cold-start mitigation is ever wanted (optional; trivial init today).
3. Settle exact rate-limit defaults and `?format=json` response schema.
4. ~~Decide whois-web bridge design separately when the retirement plan firms up.~~ **Resolved 2026-11-02**: the bridge is implemented in `bridge/` (TCP 43 → HTTPS, Python 3 stdlib port of `legacy/cc2asn-server.py`). Bridge sends `Authorization: Bearer <CC2ASN_BRIDGE_SECRET>` + the user's IP in `X-Forwarded-For`; the API trusts the forwarded IP behind a constant-time match, rate-limits per user, and tags `/cc2asn/api/latest` lines ` src=tcp43` (direct HTTP = `src=http`; the historic group's `src=cache|s3` is untouched). VM ops/provisioning/cutover/retirement record: `bridge/README.md`. Deployment pending: GCP free-tier VM (us-east1) + `CC2ASN_BRIDGE_SECRET` on the function + DNS cutover.

## Implementation notes (2026-11-01, post-deploy)

- Open item 1 (Function-URL invoke mode) resolved: BUFFERED works with the adapter; the missing piece was the **two-statement resource policy** required for CLI-created URLs since Oct 2025 (`lambda:InvokeFunctionUrl` + `lambda:InvokeFunction` gated by `lambda:InvokedViaFunctionUrl`) — both documented in `api/README.md` step 4.
- Query logging added (per user decision 2026-11-01): three log groups — operational `/aws/lambda/CC2ASN-api`; query groups `/cc2asn/api/latest` (exact legacy whois format) and `/cc2asn/api/historic` (legacy prefix + `date status ms src tier tok`); UTC; 180-day retention; streams per day.
- Tier names finalized: `standard` and `pro` (DDB, CLI, logs).
- Ops finding (safe to re-verify on future runtimes): app-side PutLogEvents / post-response writes placed **after** the response send never completed in the adapter setup; query-log writes are ordered **before** the response send instead. Behavior verified live (lines land immediately under the pre-send order).
