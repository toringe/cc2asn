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

- `/cc2asn/api/latest` — free latest-day lookups, legacy whois line format
  plus a source marker:
  `2026-11-01 13:04:51 141.101.104.69 IPV4 US src=http|tcp43`
  (`src=tcp43` = whois bridge traffic; the IP is the end user's, forwarded
  behind the bridge secret — see below).
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
   `PORT`, `CC2ASN_DB_BUCKET`, `CC2ASN_TOKENS_TABLE`,
   `CC2ASN_BRIDGE_SECRET` (shared secret for the whois bridge — bridge
   requests bring `Authorization: Bearer <secret>` + the user's IP in
   `X-Forwarded-For`, trusted only on a constant-time match; per-user rate
   limits and logging apply to the forwarded IP. Empty/absent = bridge
   support disabled) and `CC2ASN_PROXY_SECRET` (the CloudFront
   distribution's fixed origin-header secret — same constant-time trust
   pattern for forwarded viewer IPs, see the custom-domain section below.
   Empty/absent = proxy support disabled).
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

   Note: `deploy` updates **code only**. When adding the whois bridge,
   the `CC2ASN_BRIDGE_SECRET` env var must be set on the live function
   separately — and `--environment`/`update-function-configuration`
   **replaces all variables**, so pass the full set:

    aws lambda update-function-configuration --region eu-west-1 \
        --function-name CC2ASN-api \
        --environment "Variables={AWS_LAMBDA_EXEC_WRAPPER=/opt/bootstrap,PORT=8080,CC2ASN_DB_BUCKET=cc2asn-db,CC2ASN_TOKENS_TABLE=cc2asn-tokens,CC2ASN_BRIDGE_SECRET=<bridge secret or comma-separated pair during rotation>,CC2ASN_PROXY_SECRET=<proxy secret or comma-separated pair during rotation>}"

   Until `CC2ASN_BRIDGE_SECRET` is set on the live function, bridge
   deployment is **silently limp**: queries still work but every bridged
   user shares the bridge VM's one anonymous rate-limit bucket and the
   latest log records the VM IP with `src=http`. Watch `/cc2asn/api/latest`
   for `src=tcp43` after cutover — its absence is the tell.

6. Smoke test (`FUNCTION_URL` from step 4): banner, `/NO`, historic
   401 without token, historic 200 with token, malformed date 404.
   Then issue a real token with `scripts/issue_token.py` and verify quota
   via CloudWatch + DynamoDB usage.

7. Custom domain (`api.cc2asn.com` — CloudFront in front of the Function URL):

   Lambda Function URLs support no custom domains and serve TLS only for
   `*.on.aws`, so the friendly name fronts the Function URL with
   CloudFront (the documented AWS pattern; free tier covers this easily).

   a. Pick the proxy secret (e.g. `openssl rand -hex 32`) — two places:
      the distribution's origin header `X-CC2ASN-Proxy` and the
      `CC2ASN_PROXY_SECRET` env var (full variable set — the command in
      step 5 above). The API is deployed first (step 5 change is
      additive; existing traffic unaffected).

   b. ACM certificate **in us-east-1** (CloudFront's region quirk):
        aws acm request-certificate --region us-east-1 \
            --domain-name api.cc2asn.com \
            --validation-method DNS
      Add the returned validation CNAME in Cloudflare (DNS only). Then:
        aws acm list-certificates --region us-east-1 --query "CertificateSummaryList[?DomainName=='api.cc2asn.com'].CertificateArn"

   c. Fill `cloudfront-distribution.json` (same directory): Function
      URL host, ACM ARN, proxy secret. Verify the managed policy IDs
      are still the current ones (read-only):
        aws cloudfront list-cache-policies --query "CachePolicyList.Items[?starts_with(CachePolicy.Comment,'CachingDisabled')].Id"
        aws cloudfront list-origin-request-policies --query "OriginRequestPolicyList.Items[?starts_with(OriginRequestPolicy.Comment,'Managed-AllViewerExceptHostHeader')].Id"

   d. Create the distribution (takes a few minutes to deploy):
        aws cloudfront create-distribution --distribution-config file://cloudfront-distribution.json
      Capture `DomainName` (`d<id>.cloudfront.net`) from the response.

   e. Cloudflare: `api` CNAME → `d<id>.cloudfront.net`, **DNS only**
      (grey cloud), TTL 300.

   f. Verify:
        curl -sv https://api.cc2asn.com/NV        # 200 + X-Data-Date
        aws logs tail /cc2asn/api/latest --region eu-west-1 --since 2m
      The `/cc2asn/api/latest` line must show **your IP** with
      `src=http` — an edge IP means `CC2ASN_PROXY_SECRET` is missing
      (limp: attribution + rate limiting collapse to proxy identity).
      Historic lookups are token-authed and unchanged; only their log
      line's ip field shares the (fixed) resolution path.

   Rollback is a DNS record deletion — the raw Function URL stays
   public throughout. Remove the distribution + cert with the alias.
