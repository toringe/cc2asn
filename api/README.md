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

Tokens: `uv run python scripts/issue_token.py --name ACME --tier standard --quota 500 [--days 30]`.
Tiers: `standard` (default) and `pro`. Only the SHA-256 hash is stored; the plaintext
is printed once. Revoke with `--revoke ACME`.

## Query logs (CloudWatch)

Successful lookups are written per request to two dedicated log groups
(`lib/querylog.py`), one **CloudWatch stream per day**, 180-day retention:

- `/cc2asn/api/latest` — free latest-day lookups, exact whois line format:
  `2026-11-01 13:04:51 141.101.104.69 IPV4 US`
- `/cc2asn/api/historic` — historic lookups, legacy prefix plus:
  `date=(the requested day) status=(200) ms=(duration) src=(cache|s3) tier=(standard|pro) tok=(sha256 8-char prefix)`

Failures (401/404/429/500), banners and lifecycle events stay in the
function's own `/aws/lambda/CC2ASN-api` operational group.

Tests: `uv run python -m unittest discover -s test -t .`

## Build

    make build      # build/CC2ASN-api.zip (refreezes api/requirements.txt from .venv)
    make clean

## Deployment checklist (account changes need explicit approval)

AWS-side steps, in order. The Makefile gates all `aws` calls behind
`CONFIRM_DEPLOY=yes`; run them here only after the human partner has approved
in conversation. Region is `eu-west-1` (same as the bucket).

Read-only status checks are allowed without approval. First, authenticate
(`aws login` — re-run if the session expired), then start from read-only
checks:

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
   `arn:aws:s3:::cc2asn-db` + `arn:aws:s3:::cc2asn-db/*`, read/write(+scan)
   on the `cc2asn-tokens` table ARN, and CloudWatch Logs actions
   `logs:CreateLogGroup`, `logs:CreateLogStream`, `logs:PutLogEvents`
   (without them the function emits no logs at all). Easier alternative:
   attach the managed `AWSLambdaBasicExecutionRole` plus the inline S3/DDB
   policy.

2b. Query log groups (they are NOT auto-created by the app):

    aws logs create-log-group --log-group-name /cc2asn/api/latest --region eu-west-1
    aws logs create-log-group --log-group-name /cc2asn/api/historic --region eu-west-1
    aws logs put-retention-policy --log-group-name /cc2asn/api/latest --retention-in-days 180
    aws logs put-retention-policy --log-group-name /cc2asn/api/historic --retention-in-days 180
    aws logs put-retention-policy --log-group-name /aws/lambda/CC2ASN-api --retention-in-days 180

   (The runtime does self-heal a missing group once, inside the request,
   but creating them upfront keeps the first lookups clean; retention is
   CLI-only and required — groups otherwise never expire.)

3. Function creation (first deploy):

    make CONFIRM_DEPLOY=yes deploy-create   # requires ROLE_ARN and LAYER_VERSION set

   Runtime python3.14, arm64, 512 MB, 10 s timeout, handler `run.sh`,
   layer attached, env vars: `AWS_LAMBDA_EXEC_WRAPPER=/opt/bootstrap`
   (the exact wrapper name matters — a typo boots to a Python import
   error instead of starting the app),
   `PORT`, `CC2ASN_DB_BUCKET`, `CC2ASN_TOKENS_TABLE`.
   Layer version pinned at deploy: `LAYER_VERSION=30` verified
   publicly readable on 2026-10-31 (:31 exists but is not yet publicly
   shared; re-check via the read-only GetLayerVersion probe).

4. Front door: Function URL `--auth-type NONE --invoke-mode BUFFERED`.

   **Important (Oct 2025+ policy rule): URLs created via the CLI do NOT auto-add
   the needed resource policy — add BOTH statements manually:**

    aws lambda add-permission --function-name CC2ASN-api \
        --statement-id FunctionURLAllowPublicUse \
        --action lambda:InvokeFunctionUrl --principal "*" \
        --function-url-auth-type NONE
    aws lambda add-permission --function-name CC2ASN-api \
        --statement-id FunctionURLInvokeAllowPublicAccess \
        --action lambda:InvokeFunction --principal "*" \
        --invoked-via-function-url

   (Missing the second `lambda:InvokeFunction` statement yields `403 AccessDeniedException`
   from the URL service even with auth-type NONE.)

5. Code updates afterwards:

    make build && make CONFIRM_DEPLOY=yes deploy

6. Smoke test (`FUNCTION_URL` from step 4): banner, `/NO`, historic
   401 without token, historic 200 with token, malformed date 404.
   Then issue a real token with `scripts/issue_token.py` and verify quota
   via CloudWatch + DynamoDB usage.
