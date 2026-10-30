CC2ASN web API: serves the per-country data in the `cc2asn-db` bucket over
HTTPS with whois-compatible semantics (see ../docs/2026-10-29-cc2asn-api-design.md).

## Usage

Local development (no Lambda, no adapter — the app is a plain HTTP server):

    cd api && uv run python server.py
    curl -s localhost:8080/NO

Endpoints (plain text by default, `?format=json` for structured output):

    GET /                      service banner
    GET /{CC}                  latest AS numbers (free, rate limited)
    GET /{TYPE}/{CC}           latest for TYPE in ASN|IPV4|IPV6|ALL
    GET /{YYYY-MM-DD}/{CC}     historic, exact date, requires token (404 if no data)
    GET /{YYYY-MM-DD}/{TYPE}/{CC}

Tokens: `uv run python scripts/issue_token.py --name ACME --quota 500 [--days 30]`.
Only the SHA-256 hash is stored; the plaintext is printed once. Revoke with
`--revoke ACME`.

Tests: `uv run python -m unittest discover -s test -t .`

## Build

    make build      # build/CC2ASN-api.zip (refreezes api/requirements.txt from .venv)
    make clean

## Deployment checklist (account changes need explicit approval)

AWS-side steps, in order. The Makefile gates all `aws` calls behind
`CONFIRM_DEPLOY=yes`; run them here only after the human partner has approved
in conversation. Region is `eu-west-1` (same as the bucket).

Read-only status checks are allowed without approval:

    # latest adapter layer version (update LAYER_VERSION in the Makefile):
    aws lambda list-layer-versions \
        --layer-name arn:aws:lambda:eu-west-1:753240598075:layer:LambdaAdapterLayerArm64 \
        --region eu-west-1 --query 'LayerVersions[0].Version'

1. DynamoDB table (provisioned mode, always-free sized):

    aws dynamodb create-table --table-name cc2asn-tokens \
        --attribute-definitions AttributeName=tk,AttributeType=S \
        --key-schema AttributeName=tk,KeyType=HASH \
        --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5 \
        --region eu-west-1

2. Execution role: `CC2ASN-api-role` with trust policy for
   `lambda.amazonaws.com`; inline policy granting `s3:GetObject` on
   `arn:aws:s3:::cc2asn-db` + `arn:aws:s3:::cc2asn-db/*` and read/write(+scan)
   on the `cc2asn-tokens` table ARN.

3. Function creation (first deploy):

    make CONFIRM_DEPLOY=yes deploy-create   # requires ROLE_ARN and LAYER_VERSION set

   Runtime python3.14, arm64, 512 MB, 10 s timeout, handler `run.sh`,
   layer attached, env vars: `AWS_LWA_EXEC_WRAPPER=/opt/bootstrap`,
   `PORT`, `CC2ASN_DB_BUCKET`, `CC2ASN_TOKENS_TABLE`.

4. Front door: Function URL `--auth-type NONE --invoke-mode BUFFERED`.
   Note (design open item): the adapter's Function-URL invoke payload handling
   is verified at first deploy; BUFFERED is the default invoke mode.

5. Code updates afterwards:

    make build && make CONFIRM_DEPLOY=yes deploy

6. Smoke test (`FUNCTION_URL` from step 4): banner, `/NO`, historic
   401 without token, historic 200 with token, malformed date 404.
   Then issue a real token with `scripts/issue_token.py` and verify quota
   via CloudWatch + DynamoDB usage.
