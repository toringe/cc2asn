CC2ASN
======

A lookup service for AS-numbers and prefixes belonging to any given country in
the world. Simply provide country codes as input and you'll get all ASNs, IPv4
or IPv6 addresses registered to that country. For more detailed information
check out the website at [www.cc2asn.com][1].

This branch (`dev`) holds the newest state of the project: the pipeline runs on
an AWS Lambda python3.14 runtime, and the data is additionally served by a new
web API (Lambda Web Adapter + Function URL) with whois-compatible semantics,
free latest-day lookups, and token-gated historic lookups (data back to 2019).
For the legacy server-based version, see [legacy/README.md](legacy/README.md);
the old TCP whois server keeps running until the web API fully replaces it
(the whois-to-API bridge plan lives in `docs/2026-10-29-cc2asn-api-design.md`).

Repo layout
-----------

| Directory | Content |
|---|---|
| `lambda/` | The production pipeline: downloader, parser, curator (one file per function) + Makefile for build/deploy |
| `api/` | The web API: a Python HTTP server behind [aws-lambda-web-adapter][2], Function URL front door, DynamoDB tokens, query logs |
| `legacy/` | The original server-based implementation (TCP whois, Python 2) |
| `example-s3/` | Local-only mirror of the production buckets for testing (gitignored) |
| `docs/` | Design docs and plans (api design, parked platform evaluation) |
| `misc/` | Architecture diagrams |

Pipeline
--------

Event-driven, one file per function (see `lambda/README.md` for build/deploy):

1. **downloader** — fetches a RIR delegation file, verifies MD5, stores it in S3
2. **parser** — S3-triggered; parses the RIR SEF format into per-country JSON
3. **curator** — writes per-country files (`<CC>_<TYPE>`, `<CC>_ALL`) to the
   delivery bucket under `latest/` and `yyyy/mm/dd/`

Web API
-------

`GET /{CC}`, `GET /{TYPE}/{CC}` and date-prefixed variants serve the curated
data. Latest-day lookups are free (rate limited); historic lookups require a
token. Two dedicated query logs (legacy whois format / enriched) keep the same
usage record the old server produced. See `api/README.md` for endpoints,
tokens and deployment, and `docs/2026-10-29-cc2asn-api-design.md` for the
design and cost model (everything sized to stay in the AWS free tiers).

Architecture
------------
![Architecture Diagram](misc/CC2ASN-Arch-Dark.png#gh-dark-mode-only)
![Architecture Diagram](misc/CC2ASN-Arch-Light.png#gh-light-mode-only)

[1]: http://www.cc2asn.com
[2]: https://github.com/aws/aws-lambda-web-adapter
