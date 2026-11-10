# CC2ASN Pro tier: design

**Status: PARKED 2026-11-10. Not implemented.** The owner decided not to offer
a Pro plan or historic lookups for now; the API serves the latest day only,
free, max 100 queries per user per day. Kept for a possible revisit. If
reopened, re-verify the facts below and note that the token code it builds on
(`api/lib/tokens.py`, `api/scripts/issue_token.py`) was removed on 2026-11-10
and must be restored from git history first.

## Intent

Let customers buy self-service access to historic lookups in the CC2ASN web API,
monthly or annually, with no monthly platform cost and as little machinery as
possible for a one-person company in Norway.

Public RIR archives and RIPEstat offer similar history for free, so Pro sells
convenience: one simple, stable API across all RIRs, in whois-compatible output.

## Decisions by the owner

| Topic | Decision |
|---|---|
| Paid plans | **Pro is the only paid plan** |
| Price | **$10/month or $100/year** |
| Quota | **1,000 historic lookups per day** |
| Free access | `standard` means anonymous, free latest-day lookups: no login, no token |
| Seller | The owner's one-person company, based in Norway |
| Order of work | Everything implemented and tested first; **DNS cutover is the very last step before launch** |

## Goals

- A customer can pay, receive a key, and make historic lookups without manual work.
- Cancelled, refunded or unpaid subscriptions lose access without manual work.
- No new monthly fixed cost; AWS stays within or near its free tiers.
- VAT is handled by the merchant of record.

## Non-goals

- User accounts, logins, or a customer dashboard.
- Additional paid plans, usage-based billing, or bulk-download endpoints.
- Historic lookups over whois/TCP 43 (Pro is HTTPS only).
- Automated lost-key recovery at launch.

## Anti-goals

- Selling coverage the data does not have.
- Any change that disturbs free latest-day lookups or the whois bridge.
- Storing plaintext keys.
- A Stripe outage cutting off paying customers immediately.

## Facts this design rests on (verified 2026-11-09)

- `cc2asn-db` holds 1,443 dated days. It is **continuous from 2023-02-01**. Earlier
  data is sparse: 2 days in 2019, 59 in 2020, none in 2021, 1 in 2022, 3 in Jan 2023.
- Usage in the last 30 days: 2 historic and 33 latest lookups. Real whois users are
  not on the API until the DNS cutover.
- Largest object seen is 1.46 MB; the average in the sampled day is about 10 KB.
- S3 GET in eu-west-1 is $0.004 per 10,000. (The API design doc says per 1,000; that is wrong.)
- The CloudFront template sets no allowed methods, so it passes GET/HEAD only. The
  claim flow uses GET.

## Payments: Stripe Managed Payments (builder recommendation, pending confirmation)

Stripe is the merchant of record: it calculates, collects, files and remits VAT,
and handles disputes and transaction support. Norway is a supported business location.
The owner already uses Stripe. **Fallback: Polar**, whose free plan is 5% + $0.50,
+1.5% on non-US cards, plus payout fees.

Must be confirmed before building:

1. Stripe's eligibility review approves this business and product.
2. The actual fee, as shown by Stripe (3.5% is from secondary sources; whether Billing's
   0.7% stacks is unconfirmed).
3. RIR data rights: accepted by the owner (2026-11-09) on the basis that the files are
   public and contain no personal data. APNIC's README allows free download and use,
   conditional on no liability, and does not address redistribution; other RIRs' terms
   were not found. The Pro terms must carry a no-warranty disclaimer and state that the
   data is not authoritative for where a resource is currently used.
4. Managed Payments can be exercised end to end in a Stripe sandbox.

Setup:

- **A separate Stripe account for CC2ASN**, under the existing login, so dispute rates,
  support email and business details stay apart from the other two projects.
- One product, two prices, two Payment Links.
- Tax code `txcd_10701410` (Electronically Delivered Information Services, Business Use),
  unless Stripe directs otherwise.
- **Prices are tax-exclusive**, so VAT is added at checkout and does not reduce revenue.

Estimated Stripe cost per sale (Norway account, about 10 NOK/USD, before confirmation):

| Card | $10 monthly | $100 annual |
|---|---|---|
| EEA | ~$0.86 | ~$6.80 |
| Non-EEA | ~$0.95 | ~$7.65 |
| Non-EEA with currency conversion | ~$1.15 | ~$9.65 |

## Purchase and access flow

1. The customer opens a Payment Link from the Pro page.
2. Stripe redirects to `https://api.cc2asn.com/billing/claim?session_id={CHECKOUT_SESSION_ID}`.
3. The API retrieves the session from Stripe and requires it to be complete, paid and
   linked to a subscription for the Pro product.
4. The API generates a key with a `cc2asn_pro_` prefix, stores only its SHA-256 hash,
   and displays the plaintext once. The response sets `Cache-Control: no-store` and
   `Referrer-Policy: no-referrer`.
5. A conditional DynamoDB write of a `claim#<session_id>` item makes each session
   claimable exactly once. A second visit shows "already claimed" and how to get help.
6. The customer uses `Authorization: Bearer <key>` on historic lookups.

No webhook is used. Access is checked against Stripe lazily.

## Subscription checks

The token item gains `stripe_subscription`, `checked_on` and `last_ok_on`.

- On the first authorized request of a UTC day, call Stripe for the subscription status.
- `active`, `trialing`, `past_due`: allow, and record the date. Stripe keeps a subscription
  cancelled at period end `active` until the period ends.
- Any other status (for example `canceled`, `unpaid`, `incomplete_expired`, `paused`): deny with 401.
- Stripe unreachable: keep allowing for up to 72 hours since `last_ok_on`, then deny.
- Manually issued tokens have no `stripe_subscription` and skip this check.

Use stdlib HTTPS calls with short timeouts, no Stripe SDK. Isolate Stripe in
`api/lib/billing.py` so a provider change stays local. Authenticate with a **restricted
key** with read access to Checkout Sessions and Subscriptions only, in a new env var
`CC2ASN_STRIPE_KEY`. (`update-function-configuration` replaces all variables, so the
full set must be passed.)

## API changes

Tier and tokens:

- Historic lookups require `tier == "pro"`. Other tokens get 403.
- `issue_token.py`: remove `standard`, make `pro` the default, default quota 1,000.
  Manual issuance remains for complimentary access and lost-key reissue.
- The one existing live `standard` token is re-issued as `pro` or deleted, as the owner chooses.

Quota (`lib/tokens.py`, `server.py`):

- Charge quota only for successful lookups; a 404 is free.
- Check the fetched item first and return 429 early when already exhausted.
- `consume()` takes the quota from the authorized item and tries the same-day update
  first, so a normal lookup is one write instead of two.
- DynamoDB throttling returns 429 with `Retry-After`, not 500.
- Quota 429s carry `Retry-After` (seconds until 00:00 UTC).
- Successful historic responses carry `X-Quota-Limit` and `X-Quota-Remaining`.
- Per-key burst limit: 300 requests/minute per warm environment, reusing `RateLimiter`.

Routes and content:

- `GET /billing/claim` handled before `parse_query`, with a per-IP limit and a
  `cs_` format check before any Stripe call.
- `GET /pro`: minimal HTML page with description, honest coverage, both Payment Links,
  terms, refund policy, privacy notice and contact.
- Banner points to `/pro`.
- Available dates are stated on the Pro page; no dates endpoint at launch.

Unchanged: anonymous latest-day lookups, the bridge and proxy trust paths, log group formats.

## Quota rationale (1,000/day)

| Job | Lookups | Time at 1,000/day |
|---|---|---|
| One country, one type, all dates | 1,443 | 2 days |
| All countries, all 4 types, one date (assuming ~250 countries) | ~1,000 | 1 day |
| Entire archive, `ALL` type only | ~360,000 | ~1 year |

## AWS cost

For a customer using the full quota (30,000 lookups/month): S3 is about $0.012, Lambda
and DynamoDB fit the free tier, and transfer is about 0.3 GB typically (44 GB at the
theoretical worst case of only the largest file).

## Testing

1. Unit tests with Stripe stubbed: claim success, unpaid session, wrong product, double
   claim including concurrent claim, subscription statuses, Stripe outage inside and
   beyond 72 hours, tier enforcement, 404 not charged, exhausted quota, throttling, headers.
2. Run the suite in an environment with `boto3` installed (3 errors today come only from it being absent).
3. Stripe sandbox: monthly and annual purchase, claim, cancel, failed renewal.
4. Live, before cutover: one real purchase, claim, lookups, cancellation and refund.

## Launch sequence

1. Confirm the four items under Payments.
2. Implement and run tests.
3. Deploy to the live API, with explicit owner approval. Payment Links stay unpublished.
4. Sandbox and live end-to-end tests.
5. Publish the Pro page and links.
6. **Last: DNS cutover** per `bridge/README.md`.

## Left to the builder

Exact page copy and layout (owner reviews terms, refund policy and privacy notice),
log wording for claims, and internal structure of `lib/billing.py`.
