# CC2ASN Cloudflare Port Implementation Plan

> **STATUS: PARKED (2026-10-29).** Evaluated and **not chosen** — the pipeline stays on AWS (Lambda + S3, already running free, see repo `AGENTS.md`). This plan is archived as a complete, verified reference: every platform fact below was checked against current docs on 2026-10-29 (R2 `put(md5=…)` integrity option, Python Workers handler surface, `pywrangler`/Wrangler config shapes, free-tier vs Paid limits). If the consolidation is ever revisited (~$5/mo Workers Paid), this plan cuts most of the design work.
> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rebuild the `legacy/lambda/` pipeline (downloader → parser → curator) as **Python** Cloudflare Workers over the migrated R2 buckets `cc2asn-data` and `cc2asn-db`, preserving behavior and output layout byte-for-byte.

**Architecture:** Same decoupled three-stage design as the Lambda pipeline, with Queues replacing EventBridge S3-ObjectCreated events. `cc2asn-downloader` (cron trigger for daily runs, `fetch` handler for manual `?url=` invocations) downloads a RIR SEF file, verifies its origin `<url>.md5` via R2's `put(md5=…)` integrity option, stores it under `RIR-SEF/`, then enqueues a `{bucket, key}` parse job. `cc2asn-parser` (queue consumer) parses SEF into per-country JSON and writes `parsed/<basename>.json`, then enqueues a curate job. `cc2asn-curator` (queue consumer) writes `<CC>_<TYPE>`/`<CC>_ALL` files to `cc2asn-db` under both `yyyy/mm/dd/` and `latest/`. This port buys a real language-parity win: the pure logic modules (`parse.py`, `dbstore.py`, download core) are ordinary Python, so they are unit-tested with pytest **natively, without the Workers runtime or any cloud credentials** — the same tests then guard the deployed code, since the Workers entrypoints are thin FFI glue around them.

**Tech Stack:** Python Workers (`from workers import WorkerEntrypoint, Response`; `python_workers` compatibility flag), **`pywrangler`** (`workers-py`) as the CLI — it wraps Wrangler, sets up the environment, bundles PyPI deps (`uv` + `pyproject.toml`), and supports all Wrangler commands; per-worker `wrangler.jsonc` configs. R2 bindings, Queues, Cron Triggers. `natsort` PyPI package — the *same package the legacy curator imported*, so sort parity is literal. pytest for pure-module tests; `pywrangler dev` (local Wrangler, simulated R2/Queues, no credentials) for wiring smoke.

**Spec:** the behavioral spec is the removed-from-production Lambda implementation — `legacy/lambda/{downloader,parser,curator}.py` (read them before starting; named quirks and sequencing matter) — plus the verified ground-truth output snapshots already in `example-r2/cc2asn-data/parsed/*.json` and `example-r2/cc2asn-db/` (produced by the running Lambda). This is a straight behavior-preserving port into Python; all five SEF files currently in production R2: `delegated-{afrinic,apnic,arin,lacnic,ripencc}-extended-latest`.

## Design: Lambda → Cloudflare mapping

| legacy/lambda | Cloudflare port | Notes |
|---|---|---|
| `downloader.py` (manual invoke w/ `{"url": …, "loglevel": …}`, externally scheduled) | `cc2asn-downloader`: `scheduled(controller, env, ctx)` iterates all five SEF URLs daily; `fetch(request)` handles one URL on demand (`GET /?url=…&loglevel=…`) | parity: same two invocation modes |
| S3 `put` + local `calc_md5()` vs `<url>.md5` | R2 `env.DATA.put(key, data, md5=<origin-md5>)` — R2 rejects on mismatch | no custom MD5 impl needed; see Review Focus |
| EventBridge (S3 ObjectCreated → parser) | Queue `cc2asn-parse`: downloader sends `{"bucket": "cc2asn-data", "key": "RIR-SEF/<file>"}` after a successful put | parser consumes |
| EventBridge (parser S3 write → curator) | Queue `cc2asn-curate`: parser sends `{"bucket": "cc2asn-data", "key": "parsed/<file>.json"}` after writing | curator consumes |
| `parser.py` `collections.OrderedDict` | `dict` in CPython 3.7+/Pyodide preserves insertion order → JSON output key order parity | nicer than the TS plan needed |
| `curator.py` `dbstore` (`natsort.natsorted`) | `natsort` PyPI package (same lib), pure `dbstore(parsed) -> list[WriteOp]` in `src/dbstore.py`; write list computed pure, then shell executes with `asyncio.Semaphore(6)` | write list computed pure; shell performs the puts |
| Python `logging` package | native Python `logging` with `logging.basicConfig(level=LOG_LEVEL)` — supported in Python Workers (`workers-runtime-sdk` docs) | closest possible parity to legacy logging |
| hardcoded `REGION/BUCKET/PREFIX` | bindings: `DATA` → `cc2asn-data` (all 3 workers), `DB` → `cc2asn-db` (curator), queues `cc2asn-parse`/`cc2asn-curate` | no hardcoded endpoints |
| `LogLevel` env var / event field | `LOG_LEVEL` var on parser + curator (`wrangler.jsonc` `vars`); `?loglevel=` on downloader fetch | same accepted names `debug/info/warning/error` |

**File structure (create all under `workers/` at repo root; `legacy/lambda/` is read-only reference):**

```
workers/
  downloader/            # each subdir: pyproject.toml + wrangler.jsonc + uv.lock (its own uv env)
    wrangler.jsonc       # python_workers flag, R2 + queue bindings, [triggers] cron: 0 3 * * *
    pyproject.toml       # requires-python >=3.13; dev: workers-py, workers-runtime-sdk, pytest
    src/entry.py         # Default(WorkerEntrypoint): scheduled + fetch handlers (FFI glue only)
    src/job.py           # process(url, content: bytes, md5_text: str | None, put_object) -> dict  (pure; put callable injected)
    src/config.py        # SEF_URLS constant (the five pinned URLs)
    tests/test_job.py
    tests/fakes.py       # FakeBucket (put enforces R2's md5-mismatch rejection), FakeQueue, FakeMessage
  parser/
    wrangler.jsonc       # R2 DATA + queue producers/consumers bindings, vars LOG_LEVEL
    pyproject.toml
    src/entry.py         # Default(WorkerEntrypoint): queue(self, batch) consumer
    src/parse.py         # parse_sef(lines: list[str]) -> dict  (pure, stdlib json/math)
    tests/test_parse.py
    tests/test_entry.py  # fake env objects, no Workers runtime
  curator/
    wrangler.jsonc
    pyproject.toml       # deps include natsort
    src/entry.py         # queue consumer; executes writes with asyncio.Semaphore(6)
    src/dbstore.py       # dbstore(parsed: dict) -> list[{"key": str, "body": str}]  (pure; WriteOp)
    tests/test_dbstore.py
    tests/test_entry.py
```

**Message contract (single source of truth; duplicated as dicts in each worker — any shape change must be made in all three test suites):** `{"bucket": str, "key": str}` on both queues. It is the JSON-serialized queue body (default `json` content type).

**Verified platform facts baked into this plan** (docs fetched 2026-10-29): Python Workers handlers live in an entrypoint class extending `WorkerEntrypoint` (`fetch`, `scheduled(controller, env, ctx)`, `queue(self, batch)` — `batch.messages`, per-message `ack()`/`retry()`); bindings expose R2/Queue on `self.env`; queue publish is `await self.env.<QUEUE>.send({"bucket": …, "key": …})`; `pywrangler` sets up uv env, bundles PyPI deps, and "also supports all commands supported by the `wrangler` tool"; Python Workers require the `python_workers` compatibility flag; Workers Paid = 10,000 subrequests/invocation (Free = 50, which cannot run the curator); cron/queue-consumer wall clock is 15 min; curator runs ~1,000-2,000 R2 puts per RIPE-scale message → cap concurrent puts at 6 (Workers allows 6 connections simultaneously waiting for response headers per invocation).

## Global Constraints

- **Workers Paid plan required** (curator's write volume >> free plan's 50 subrequests; Pyodide start-up CPU also far exceeds the 10 ms free-plan CPU). Confirm plan on the cf account before deploy.
- **CLI: use `uv run pywrangler …` inside each worker dir** (Python Workers are a Wrangler-config project: `wrangler.jsonc`. Per repo AGENTS.md + the wrangler skill, Wrangler-style config is the driver here; the cf-cli skill's routing says wrangler-config projects run wrangler — `pywrangler` is that runner for Python). Only Wrangler-gap commands if ever needed: `npx wrangler tail <name>`; but prefer `uv run pywrangler <cmd>` for `dev`, `deploy`, `types`, and all queue/R2 resource commands.
- Installing: `uv` and Node are prerequisites (pywrangler docs). Load required skills before touching each area per repo AGENTS.md: `wrangler` before any dev/deploy/queue command; `workers-best-practices` before writing Worker code.
- Each worker dir: `pyproject.toml` with `requires-python = ">=3.13"`, dev deps `workers-py` + `workers-runtime-sdk` + `pytest`; `src/` layout; keep the repo's existing uv-managed Python 3.14 `.python-version` at the root intact.
- Compatibility flags: `"python_workers"` in every `wrangler.jsonc`; `compatibility_date: "2026-10-29"`; `[triggers] crons = ["0 3 * * *"]` (downloader only); `[observability] enabled = true` on all three.
- Worker names `cc2asn-downloader/parser/curator`; queues `cc2asn-parse` (producer: downloader; consumer: parser, `max_batch_size = 1` for lambda-style one-file-per-invocation parity) and `cc2asn-curate` (producer: parser; consumer: curator, same size 1).
- The five production SEF URLs pinned (matching `legacy/lambda/test_local_downloader.py`'s URLs): `https://ftp.afrinic.net/pub/stats/afrinic/delegated-afrinic-extended-latest`, `https://ftp.apnic.net/stats/apnic/delegated-apnic-extended-latest`, `https://ftp.arin.net/pub/stats/arin/delegated-arin-extended-latest`, `https://ftp.lacnic.net/pub/stats/lacnic/delegated-lacnic-extended-latest`, `https://ftp.ripe.net/pub/stats/ripencc/delegated-ripencc-extended-latest`.
- Pure modules MUST NOT import `workers`/`js` — no runtime SDK in anything under `src/job.py`, `src/parse.py`, `src/dbstore.py`. This is what makes pytest credential-free and fast.
- **Behavior parity rule:** port legacy semantics literally, including quirks flagged in Review Focus. Where legacy is clearly a bug, still port it first, then raise it for the user — do not unilaterally "fix" it in this port.
- No secrets in git; no `.dev.vars` committed. Update root `.gitignore` for `workers/**/.venv/`, `uv.lock` artifacts? (check: `uv.lock` SHOULD be committed.) Add: `_build`/cache dirs under workers/.

## Review Focus

Failure modes the spec implies but normal-name tests might not catch; each is pinned by a specific test in a specific task:

1. **R2 `put(md5=…)` semantics from Python** — ① does the hex string form pass FFI conversion into the JS `R2PutOptions` object (kwargs→object conversion); ② does R2 actually reject a mismatching body; ③ accepted md5 encodings (hex string vs base64 vs 16-byte ArrayBuffer — docs say `ArrayBuffer | string`). Task 6 pins ①+② shape-wise in fakes, and the local `pywrangler dev` smoke (local R2 sim) re-verifies precisely — if local simulation doesn't enforce md5, the fallback plan switches to a pure-Python MD5 module (stdlib `hashlib` if available in Pyodide, else vendor a ~100-line md5 impl with RFC 1321 vectors) + explicit cmp before put. Note the deviation in Task 8 deploy notes.
2. **SEF version line handling** — version line is the line *starting with a digit*; `DATE` is field **5**. Legacy parses `summary`/`available`/`reserved` against the *original-case* line and uppercases only the `|`-split elements. Task 2 pins with a lowercased fixture line.
3. **IPv6 rows** — `value` is already the prefix length (`2001:600::|29` → `2001:600::/29` — checked against ground truth). IPv4 `4096` → `/20` (`32 - log2(4096)`). Task 2 pins both.
4. **Curator sort + `_ALL` assembly** — non-IPV6 types use `natsort.natsorted` (PyPI `natsort`, default options); IPV6 uses plain lexicographic `sorted()`; `_ALL` = concatenation of per-type sorted arrays in *type-name sorted* order (ASN before IPV4 before IPV6). Task 4 pins both sort flavors with rows deliberately picked where natural ≠ lexicographic.
5. **Curator write volume vs concurrent connections** — up to ~2,000 R2 puts per message with only 6 concurrent allowed. Task 5 pins that the writer never issues more than 6 concurrent puts (fake bucket records in-flight count), and Task 8's deploy smoke confirms a real RIPE-scale curate completes inside the 15-min queue-consumer wall clock.

## Tasks

### Task 1: Scaffold `workers/` Python monorepo

**Files:**
- Create: `workers/{downloader,parser,curator}/{pyproject.toml,wrangler.jsonc}` + minimal `src/entry.py` hello worker each
- Create: `workers/downloader/tests/fakes.py` (shared stubs used by 3 suites are simpler inside each suite; skip this here — see task note below)
- Modify: `.gitignore` (add per-worker `.venv/` and build/cache dirs; keep root `.python-version` alone)

**Interfaces:**
- Produces: three uv-managed Python Worker projects, runnable with `uv run pywrangler dev`, wrangler configs with names `cc2asn-{downloader,parser,curator}`, compat flag `python_workers`, date 2026-10-29 — but no bindings wired yet (bindings/triggers finalized in Task 8).

- [ ] **Step 1:** Confirm `uv --version` and Node ≥ 22 present; verify pywrangler: `uvx --from workers-py pywrangler --version` (or per-worker `uv run pywrangler --version` after init).
- [ ] **Step 2: Scaffold each subdir** — do NOT use `pywrangler init` blindly (it creates a JS-ish tree; instead check `uv run pywrangler init --help` and if the template is unusable, hand-write the minimal `pyproject.toml` (per packages docs: `[project]` + `[dependency-groups] dev = ["workers-py", "workers-runtime-sdk", "pytest"]`) plus a minimal `src/entry.py` hello worker) and a minimal `wrangler.jsonc` (schema from `./node_modules/wrangler/config-schema.json` not required; the python docs wrangler.toml example is the pattern) with `name`, `main = "src/entry.py"`, `compatibility_flags = ["python_workers"]`, `compatibility_date = "2026-10-29"`.
- [ ] **Step 3: Sanity check each project:** `uv run pytest --collect-only` (0 tests, config loads), `uv run pywrangler types` runs cleanly (also writes generated Env types — handy for IDE, not required in CI).
- [ ] **Step 4:** Write `workers/shared/jobs.py` as the **message contract doc** (not imported by workers): documents `{"bucket", "key"}` queue payload both ways, `SEF_URLS` list. The actual constant goes into `workers/downloader/src/config.py` in Task 6 (downloader is the only worker needing it).
- [ ] **Step 5: Commit** — `git add workers/ .gitignore && git commit -m "chore(workers): scaffold downloader/parser/curator Python cf projects"`

### Task 2: `parse_sef` — port of the SEF parser core

**Files:**
- Create: `workers/parser/src/parse.py`, `workers/parser/tests/test_parse.py`, `workers/parser/tests/fixtures/` (file `fixture_sef.txt` + `expected.json`)

**Interfaces:**
- Produces: `parse_sef(lines: list[str]) -> dict` — EXACT port of `legacy/lambda/parser.py::parser()`. quirks: version line = **line starting with a digit**, take field[5] as `DATE`; skip `#`-comments, `summary`-ending, `available`/`reserved`-ending rows (case-sensitive status checks on the raw line, matching legacy); upper-case the `|`-split elements *after* those checks; IPv4: `value = ceil(32 - log2(value))`; IPv6: value is already the prefix length; ASN: `"AS" + start`; NO dedupe, NO sorting (legacy preserves encounter order and duplicates; curator sorts later); a real allocated row with empty country code produces the literal key `""` (its omission is the curator's job, not the parser's).

- [ ] **Step 1: Build the test fixture** — copy these REAL lines from `example-r2/cc2asn-data/RIR-SEF/delegated-ripencc-extended-latest` into `tests/fixtures/fixture_sef.txt`, plus one synthetic-but-valid line to exercise sort divergence, plus one lowercased line, plus one real allocated empty-cc row:

```
# header comment
2|ripencc|1792969199|261085|19700101|20261025|+0100
ripencc|*|ipv4|*|101094|summary
ripencc|*|asn|*|48685|summary
ripencc|*|ipv6|*|111306|summary
ripencc|PS|ipv4|1.178.112.0|4096|20071126|allocated|ff94ef2c-a92c-479a-a3f5-ee2bd1626c76
ripencc|FR|ipv4|1.178.96.0|8192|20100625|allocated|9cd3fe87-31b4-4e2d-bf07-2404c8c368aa
ripencc|FR|ipv4|1.178.224.0|8192|20100625|allocated|9cd3fe87-31b4-4e2d-bf07-2404c8c368aa
ripencc|NL|ipv6|2001:600::|29|19990826|allocated|8efd2bb1-0dd7-4095-946a-6f73457a410f
ripencc|DE|ipv6|2001:608::|32|19990812|allocated|93cf112e-69c0-429a-8e00-418ab29b17bf
ripencc|EU|asn|7|1|19930901|allocated|7dbfcf37-c342-458e-9acd-83d569265893
ripencc|DE|asn|7|1|20000207|allocated|84c4e4c1-c81e-168b-8b08-fd8c6498e11a
ripencc|DE|asn|28|1|19930901|allocated|2849f34d-3e57-4e2a-8464-953adab9ef02
ripencc||ipv4|81.0.0.0|256|20050104|allocated|9a9a9a9a-0000-0000-0000-000000000000
ripencc||ipv4|85.8.248.0|2048||available
ripencc||ipv4|5.134.16.0|2048||reserved
```

(The `1.178.96.0` vs `1.178.224.0` and `AS7` vs `AS28` pairs are deliberate: natural sort and lexicographic sort disagree on both — that's what pins the curator's two sort flavors in Task 4. The 1.178.96/224 pair and the two DE ASN rows diverge; `1.178.96.0` with value 8192 is synthetic-but-valid SEF.)

Also add one lowercased-alternative line (e.g. all-lowercase `ripencc|gb|ipv4|1.186.0.0|65536|20100713|allocated`) to pin the "uppercase elements, not the raw line before the summary/status checks" behavior.

- [ ] **Step 2: Write the failing test — expected exact object:**

```python
def test_parse_sef_fixture():
    lines = (FIXTURE / "fixture_sef.txt").read_text().splitlines()
    assert parse_sef(lines) == {
        "PS": {"IPV4": ["1.178.112.0/20"]},                # 4096 → /20
        "FR": {"IPV4": ["1.178.96.0/19", "1.178.224.0/19"]},  # encounter order kept
        "NL": {"IPV6": ["2001:600::/29"]},                # IPv6 value kept as-is
        "DE": {"IPV6": ["2001:608::/32"], "ASN": ["AS7", "AS28"]},
        "EU": {"ASN": ["AS7"]},
        "GB": {"IPV4": ["1.186.0.0/16"]},                 # via the lowercased line, uppercased
        "":   {"IPV4": ["81.0.0.0/24"]},                  # empty-cc allocated row present in parser output
        "DATE": "20261025",                               # field 5 of the version line
    }

def test_parse_sef_skips_available_and_reserved():
    # same fixture, assert "85.8.248.0/21" and "5.134.16.0/21" appear nowhere in the result
```

- [ ] **Step 3: Run `uv run pytest tests/test_parse.py`; expect FAIL (`parse_sef` not importable).** Then implement `parse_sef` in `src/parse.py` (stdlib only: `math`, `json`, dict ops).
- [ ] **Step 4: Run again; expect PASS, then one full-size sanity test:** add `test_parse_real_ripencc_matches_snapshot()`: read the real `legacy/../example-r2/cc2asn-data/RIR-SEF/delegated-ripencc-extended-latest` (accessed via relative path up to repo root from the test file), `parse_sef(real_lines)`, and `json.dumps(parsed)` compared against the Lambda's real `example-r2/cc2asn-data/parsed/delegated-ripencc-extended-latest.json` via `json.loads` equality (NOT byte-equality — legacy `json.dumps` inserts spaces after separators, misses none in content). This is the strongest spec check we have.
- [ ] **Step 5: Commit** — `git commit -m "feat(parser): port SEF parser core, verified byte-parity against R2 snapshot"`

### Task 3: Parser Worker shell — queue consumer, R2 read/put, curate enqueue

**Files:**
- Create: `workers/parser/src/entry.py`, `workers/parser/tests/test_entry.py` (+ `tests/fakes.py` if useful here)
- Uses: `parse_sef` from Task 2; message contract `{"bucket", "key"}`

**Interfaces:**
- Consumes: `cc2asn-parse` messages `{"bucket": "cc2asn-data", "key": "RIR-SEF/<fname>"}` via `async def queue(self, batch)` on `Default(WorkerEntrypoint)`.
- Produces: R2 object at `parsed/<basename(key)>.json` in `env.DATA` (same bucket as the source — legacy writes into the source bucket), `application/json`; then `await self.env.CURATE_Q.send({"bucket": <same>, "key": "parsed/<basename>.json"})`; `msg.ack()` each; on error `msg.retry()`.

- [ ] **Step 1: Write the failing test** with fake env objects (real Python, no Workers runtime): a `FakeBucket` seeded with the SEF fixture content, a `FakeQueue` recording `send` calls, batch stub with `Message`-shaped stubs (`body`, `ack()`, `retry()` recorded). Assert: ① `parsed/delegated-testprefix-extended-latest.json` written with `json.dumps(ccdata)` content equality to expected; ② `FakeQueue.sent == [{"bucket": "cc2asn-data", "key": "parsed/delegated-testprefix-extended-latest.json"}]`; ③ all `ack()`ed, zero `retry()`; ④ error path: missing object → `retry()` called, no queue send.
- [ ] **Step 2: Run; expect FAIL.** Then implement `src/entry.py`:

```python
from workers import WorkerEntrypoint
from parse import parse_sef
import json

class Default(WorkerEntrypoint):
    async def queue(self, batch):
        for msg in batch.messages:
            body = msg.body                       # {"bucket": ..., "key": ...}
            obj = await self.env.DATA.get(body["key"])
            ccdata = parse_sef((await obj.text()).splitlines())  # legacy parity: .decode().splitlines() — never yields a trailing "" element
            parsed_path = "parsed/" + body["key"].split("/")[-1] + ".json"
            await self.env.DATA.put(parsed_path, json.dumps(ccdata), contentType="application/json")
            await self.env.CURATE_Q.send({"bucket": body["bucket"], "key": parsed_path})
            msg.ack()
```

Logging via native `logging` (per Python Workers examples) gated on `self.env.LOG_LEVEL` set by `wrangler.jsonc` `vars`. NOTE: `parse_sef` MUST be imported as a sibling module (relative plain import `from parse import parse_sef`, per the Python Workers modules docs — `src` dir on the import path), keeping the import untouched by runtime SDK coupling.
- [ ] **Step 3: Run; expect PASS.** Commit — `git commit -m "feat(parser): queue consumer reads SEF from R2, writes parsed JSON, enqueues curate job"`

### Task 4: `dbstore` — port of curator logic (pure)

**Files:**
- Create: `workers/curator/src/dbstore.py`, `workers/curator/tests/test_dbstore.py` (+ copies of the Task 2 fixture files into `workers/curator/tests/fixtures/`)
- Dep: add `natsort` to `workers/curator/pyproject.toml`

**Interfaces:**
- Consumes: the Task 2 output `dict` (CcData, order-preserving).
- Produces: `WriteOp = {"key": str, "body": str}`; `dbstore(parsed: dict) -> list[WriteOp]`. Behavior, per `legacy/lambda/curator.py::dbstore()` — exact: keys `latest/<CC>_<TYPE>` and, only if DATE is a real calendar date, also `<yyyy>/<mm>/<dd>/<CC>_<TYPE>`; invalid/absent DATE → date keys omitted; bodies are `"\n"`-joined records (no trailing newline); per-CC per-type sorted (IPV6 uses plain `sorted()`, others `natsort.natsorted`); `<CC>_ALL` = concatenation of per-type sorted arrays in *type-name sorted* order (ASN before IPV4 before IPV6); `DATE` is consumed and NOT written as a country; empty-cc (`""`) country is skipped (`if not cc: continue`). Additionally: defensively skip blank lines (`if not ln: continue` before the digit check) — legacy relies on `splitlines()` never producing a trailing blank, so this only formalizes the same behavior for `.split("\n")` inputs (no behavioral change on real SEF data, which contains no blank lines).

- [ ] **Step 1: Write the failing test** using Task 2's fixture output as input:

```python
writes = dbstore(parse_sef(fixture_lines))
latest = [w for w in writes if w["key"].startswith("latest/")]
# _ALL = type blocks in NAME-sorted order (ASN < IPV4 < IPV6), each block per-type sorted:
{"key": "latest/DE_ALL",  "body": "AS7\nAS28\n2001:608::/32"} in latest   # type blocks: ASN then IPV6
len(latest) == 13      # PS,FR,NL,DE,EU,GB × (per-type + _ALL); the "" cc record is DROPPED by dbstore
{"key": "latest/FR_IPV4", "body": "1.178.96.0/19\n1.178.224.0/19"} in latest  # natsort: 96 < 224; lexicographic would give 224 first
{"key": "latest/DE_ASN",  "body": "AS7\nAS28"} in latest    # natsort; lexicographic gives AS28 first
{"key": "latest/NL_IPV6", "body": "2001:600::/29"} in latest # plain sorted() branch for IPV6 (code-parity: no natsort here)
{"key": "2026/10/25/DE_ALL", "body": "AS7\nAS28\n2001:608::/32"} in writes  # dated path derived from DATE 20261025
len(writes) == 26      # 13 latest + 13 dated
```

- [ ] **Step 2: Run; FAIL, then implement `dbstore`.** Use the PyPI `natsort` package, default options (matches the Python package the legacy curator used — literal same-lib parity). Then plain `sorted()` for the IPV6 branch exactly as in legacy (`natsort.natsorted` vs `sorted`). If ever in doubt about a subtle ordering, the tests above are the contract; failing tests on the fixture define the needed deviation.
- [ ] **Step 3: Run; PASS.** Commit — `git commit -m "feat(curator): port dbstore logic, computes exact R2 write list from parsed JSON"`

### Task 5: Curator Worker shell — run writes against `cc2asn-db`

**Files:**
- Create: `workers/curator/src/entry.py`, `workers/curator/tests/test_entry.py`

**Interfaces:**
- Consumes: `cc2asn-curate` messages `{"bucket", "key"}` (parsed JSON in `env.DATA`).
- Produces: R2 objects in `env.DB` (cc2asn-db) at every key from `dbstore(parsed_json)`; nothing enqueued downstream.

- [ ] **Step 1: Failing test** (same fake-env pattern as Task 3): seed a `parsed/…` object in the fake DATA, run `queue(batch)`, assert the fake DB gains all expected keys from Task 4's WriteOps with exact `\n`-joined bodies, both under `latest/` and under the dated path from the fixture's `DATE`. Also assert a concurrency property: the fake `DB.put` records its in-flight counter and the max observed in-flight count is ≤ 6.
- [ ] **Step 2: FAIL → implement `src/entry.py`:** `parsed = await self.env.DATA.get(msg.body["key"]).json()` (returns a native dict — Pyodide FFI); `ops = dbstore(parsed)`; execute with `asyncio.Semaphore(6)` + `asyncio.gather`-chunged grouping (never more than 6 in-flight R2 puts — Workers allows 6 connections simultaneously waiting for response headers per invocation); bodies as Python `str`. Log via native `logging` gated on `env.LOG_LEVEL`: per-country `debug` lines and one `info` line with the write count (legacy `fc`) and RIR name extracted from the queue-message key via `basename(key).split("-")[1].upper()` (legacy `split("-")[1]` logic).
- [ ] **Step 3: PASS.** Commit — `git commit -m "feat(curator): queue consumer executes cc2asn-db writes with bounded concurrency"`

### Task 6: Downloader core — filename/md5 extraction + `process()` with injected put

**Files:**
- Create: `workers/downloader/src/job.py`, `workers/downloader/src/config.py`, `workers/downloader/tests/test_job.py`, `workers/downloader/tests/fakes.py`, `workers/downloader/tests/fixtures/` (tiny fake SEF file + its computed md5 text line)

**Interfaces:**
- Produces (pure, stdlib-only, no FFI):
  - `parse_filename(url: str) -> str` — last `/`-segment of the URL path, URL-decoded (legacy `unquote(urlparse(url).path.split("/")[-1])`).
  - `extract_md5(md5_text: str) -> str | None` — regex `[a-fA-F\d]{32}`, first match (legacy `.findall(...).pop()` semantics).
  - `RIR_SEF_KEY_FORMAT = "RIR-SEF/{filename}"`.
  - `async def process(url: str, content: bytes, md5_text: str | None, put_object) -> dict` — `put_object(key: str, data: bytes, md5_hex: str | None)` is injected by the caller (entry.py binds it to the real R2 binding; tests bind a fake). Behavior mirroring `legacy/lambda/downloader.py::handler`: if `md5_text` extraction fails or `put_object` raises (mismatch) → return `{"ok": False, "error": ...}`; on success return `{"ok": True, "key": "RIR-SEF/<fname>"}`.
- `FakeBucket.put` enforces the same contract R2 documents for `md5`: rejects (raises) when the supplied md5 doesn't match the body being stored. This is the shape-contract pin — the REAL R2-enforcement verification happens in the Task 8 local-dev smoke against the local R2 simulation.

- [ ] **Step 1: Failing tests:** ① `parse_filename`/`extract_md5` happy paths; ② `extract_md5` returning None on garbage; ③ `process` happy path (fake put succeeds) → `{"ok": True, "key": ...}`; ④ `process` with a *wrong* md5 → fake put raises → `{"ok": False}` returned; ⑤ `process` with missing `.md5` info → `{"ok": False}` with an extract-message (legacy parses md5 and errors via "Invalid checksum"; also pin download-side extract error logging if it's a log, keep it in the entry layer).
- [ ] **Step 2: FAIL → implement `src/job.py` + `src/config.py`.** `config.py` holds `SEF_URLS` (the five URLs from Global Constraints) — pure constant.
- [ ] **Step 3: PASS.** Commit — `git commit -m "feat(downloader): fetch+verify RIR file logic with injected put_object"`

### Task 7: Downloader Worker entrypoint — scheduled + manual fetch, real R2/queue FFI glue

**Files:**
- Create: `workers/downloader/src/entry.py`, `workers/downloader/tests/test_entry.py`

**Interfaces:**
- Consumes: `process`/`extract_md5`/`config.SEF_URLS` from Task 6.
- Produces: `ParseJob` = `{"bucket": "cc2asn-data", "key": "RIR-SEF/<fname>"}` per successful download, sent to `env.PARSE_Q` (replaces EventBridge). `scheduled(controller, env, ctx)`: iterate `env.SEED_URLS.split("\n")` (skip empty lines), call the real put path per URL (with the real `env.DATA`), send ParseJob on success — per-URL independence, errors logged and next URL continues. `fetch(request)`: `GET /?url=<required>&loglevel=<optional>` → same routine for one URL; no `url` param → `400`; missing `.md5` or checksum mismatch → error logged, response carries error string (legacy logs it).

**Python FFI glue details pinned here (implementation guidance, not code):**
- `fetch` from Python: import the JS global `fetch` (per FFI docs, all Runtime APIs are callable). Response bodies: `resp.text()` for the `.md5` sidecar; for the SEF body prefer passing the *body stream* (or `text()`) — max RIR file is a few tens of MB against a 128 MB isolate budget, both fit; the FFI conversion path for `bytes` → JS (via `to_js`) is pinned in the Task 8 smoke. Verification passes the origin *hex string* as `md5=` (docs allow `ArrayBuffer | string`); if FFI rejects that form, fall back to a 16-byte ArrayBuffer via `pyodide.ffi.to_js`.
- R2 put via binding: `await self.env.DATA.put(key, to_js(content_bytes), md5=md5_hex)` — where the preferred `md5` value is the *hex string* per docs (`ArrayBuffer | string`); if FFI rejects the form, fall back to 16-byte ArrayBuffer via `pyodide.ffi.to_js`.
- Queue send: the documented Python form `await env.<Q>.send({"bucket": …, "key": …})` — plain dicts convert to JS objects (bind `dict_converter` = `js.Object.fromEntries` via `pyodide.ffi.to_js` if a bare dict send fails); verify the actual conversion path on a real send in the Task 8 smoke.

- [ ] **Step 1: Failing tests** (fake env, and the real `process` from Task 6): ① `fetch` handler with `?url=` answered by the fixture (fake fetch injected via the same `put_object`-style seam OR monkeypatched `process`); expect exactly one `ParseJob` sent to a fake `PARSE_Q` with `{bucket, key}` and the object written in the fake DATA; ② `?url=` with mismatching `.md5`: object missing, no `PARSE_Q.send` call, error result; ③ `scheduled` with 3 fake URLs: 3 enqueued jobs; one URL's put rejects ⇒ others still send (per-URL independence); ④ missing `?url` ⇒ `400`; ⑤ `?loglevel=debug` handled without error.
- [ ] **Step 2: FAIL → implement `src/entry.py`.** Log via native `logging`, gated on the query param `loglevel` for manual runs (legacy parity: downloader's log level ridden from event; scheduled defaults to `info`).
- [ ] **Step 3: PASS.** Commit — `git commit -m "feat(downloader): scheduled + manual-fetch handlers enqueue parse jobs"`

### Task 8: Wrangler configs (bindings/triggers), local pipeline smoke, deploy

**Files:**
- Modify: the three `wrangler.jsonc` (final binding/trigger set)
- Create: none new

**Interfaces:**
- Produces: deployed `cc2asn-downloader`/`cc2asn-parser`/`cc2asn-curator`; queues `cc2asn-parse` (consumer: parser; `max_batch_size = 1`) and `cc2asn-curate` (consumer: curator; same); R2 bindings `DATA` (`cc2asn-data`) on all three, `DB` (`cc2asn-db`) on curator; `vars` `LOG_LEVEL = "info"` (parser+curator), `SEED_URLS` = the five URLs newline-joined (downloader, format: `wrangler.jsonc` `vars`); cron `[triggers] crons = ["0 3 * * *"]` (downloader only); `[observability] enabled = true`.

- [ ] **Step 1: Fill each `wrangler.jsonc`** using the wrangler-config shapes: `r2_buckets`, `queues.producers` (with `binding` names `PARSE_Q`/`CURATE_Q`), `queues.consumers` (with `queue`, `max_batch_size = 1`), `[triggers]`, `vars`, `r2_buckets`. Confirm flag applicability/limits/policy by checking current docs page (per cf skill's only-verified-details rule) before saving values.
- [ ] **Step 2: Local pipeline smoke** — per wrangler skill, run `uv run pywrangler dev` for the downloader with **local R2/Queues simulation** (no real credentials): manually `fetch` handler via `curl "http://localhost:8787/?url=<fixture-served-url>"` (or point the fetch shim at a local file server via a test-only override). Assert: ① the resulting SEF object gets written into local R2 sim (the miniflare-backed `RIR-SEF/…` namespace), ② the local parser consumes a queue message and writes `parsed/<basename>.json` into the local cc2asn-data sim, ③ the local curator writes `latest/` and dated keys to the local cc2asn-db sim. If local queue-consumer delivery from a local producer is not simulated by miniflare, run each worker's `dev` locally and inject a synthetic queue message via a dev-only HTTP route on the parser/curator (guarded by `env.NODE_ENV != "production"`) — do NOT commit to production.
- [ ] **Step 3: Verify md5 enforcement locally** (Review Focus): same smoke but corrupt the `.md5` first in the local R2 fetch; local-put must fail (or our put rejects). If the local simulator does not enforce md5, run the wrong/right `.md5` verification against the real R2 bucket with a **dev-only throwaway key** inside `RIR-SEF-smoke/..` and delete it after — no wildcard changes. **If R2 does not enforce mismatches at all**, switch Task 6's md5 path to: pure-Python MD5 fallback module (+ RFC 1321 vectors) + explicit compare before put; deploy notes record the deviation.
- [ ] **Step 4: Deploy:** with queue + r2 resources confirmed visible (pywrangler/wrangler list commands), `uv run pywrangler deploy` in each of the three dirs. Then smoke end-to-end: ① confirm the cron is registered on downloader; ② verify the objects (wrangler r2 object get commands / dashboard as needed): `RIR-SEF/delegated-…`, `parsed/delegated-…json`, `latest/<CC>_<TYPE>` and `<yyyy>/<mm>/<dd>/<CC>_<TYPE>` in cc2asn-db; ③ confirm object sizes are non-zero and the parsed JSON's per-country keys are present; byte-parity vs. the legacy output was pinned by unit tests against the (stable) checked-in fixture — the SEF files rotate daily, so don't byte-compare live production output against the Oct-2026 `example-r2` snapshot. Tail logs with `npx wrangler tail <name>` (the one documented wrangler-gap command per cf-cli skill), fix any FFI/kwarg issues surfaced there.
- [ ] **Step 5: Commit** any config-only changes — `git commit -m "chore(workers): wrap wrangler.jsonc config/queue/deploy wiring"`.

## Non-goals (explicitly out of scope)

- The consumer-facing lookup API (the cc2asn.com side) — this port covers only the three-stage pipeline; it can be planned separately later.
- The metrics producer (`metrics/curator.json`) is not part of this plan; if needed later it becomes its own plan (different trigger shapes).
- CDN serving of `latest/`, custom domains, branch previews, Terraform/Pulumi IaC, CI/CD, secrets rotation, free-tier compatibility (the plan requires Workers Paid), and any changes to `legacy/` — it stays read-only as the behavioral spec.
