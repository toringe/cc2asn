# CC2ASN whois-web API bridge

A whois-protocol (TCP 43) front-end for the CC2ASN web API. It keeps
`whois -h cc2asn.com fi` and `echo ipv4 dk | nc cc2asn.com 43` working while
all data lookups happen over HTTPS in the `CC2ASN-api` Lambda — the VM does no
local lookups and holds no data. A behavior-preserving Python 3 (stdlib-only)
port of `legacy/cc2asn-server.py`, whose skeleton has run for years unattended.

The user's IP travels in `X-Forwarded-For` behind the bridge secret, so the
API logs and rate-limits per user — not per bridge — and tags such lookups
`src=tcp43` in the `/cc2asn/api/latest` query log (direct HTTP traffic is
`src=http`). The VM keeps its own local log at `QUERYLOG`, one line per
request:

    2026-11-02 13:04:51 62.x.x.x IPV4 NO 200

This README is the ops record: provisioning, deploy, cutover, retirement.

## On-disk layout (VM, Debian 12)

    /opt/cc2asn-bridge/server.py     code (this repo's bridge/server.py)
    /opt/cc2asn-bridge/lib/api.py    code (this repo's bridge/lib/api.py)
    /etc/cc2asn-bridge.conf          conf (port, API base, secret, log path)
    /var/log/cc2asn/query.log        local query log (10 MB x 5 rotation)
    /etc/systemd/system/cc2asn-bridge.service   unit (this repo)

Python: system python3 only — no pip, no venv on the VM.

## Local testing (in the repo)

    cd bridge && make test     # or: ../.venv/bin/python -m unittest discover -s test -t .

Fake-API protocol tests cover the wire contract end-to-end (real sockets,
ephemeral ports, in-process `http.server` acting as the API).

## GCP provisioning (one-time, ~15 minutes)

Requires the `gcloud` CLI, a fresh project, and the free-tier region
**us-east1** (us-central1/us-west1 also covered; EU regions are NOT free).

    gcloud projects create cc2asn-bridge
    gcloud config set project cc2asn-bridge
    gcloud services enable compute.googleapis.com

    # Static external IP: free while attached to a RUNNING instance.
    # (A static IP attached to a STOPPED instance bills, and so does a
    # reserved-but-unattached one — create the address and the instance in
    # the same sitting, and when retiring the VM, delete the address too.)
    gcloud compute addresses create cc2asn-whois --region=us-east1
    gcloud compute addresses describe cc2asn-whois --region=us-east1 \
        --format='get(address)'

    gcloud compute instances create cc2asn-bridge \
        --zone=us-east1-b \
        --machine-type=e2-micro \
        --image-family=debian-12 --image-project=debian-cloud \
        --boot-disk-size=10GB \
        --address=cc2asn-whois \
        --metadata=enable-oslogin=TRUE

    gcloud compute firewall-rules create allow-whois-43 \
        --allow=tcp:43 --source-ranges=0.0.0.0/0

Free-tier guardrails: region in {us-east1, us-central1, us-west1}, e2-micro,
boot disk <= 30 GiB (10 GB here), connected static IP, egress < 1 GiB/month
(COUNTRY_ALL bodies are small; at real whois traffic this holds extremely
easily). ssh uses OS Login + IAP (port 22 stays on the default SSH rule;
tighten by deleting `default-allow-ssh` once IAP-only access is confirmed).

## VM software setup (one-time)

    # unprivileged user + dirs
    sudo groupadd -r cc2asn
    sudo useradd -r -g cc2asn -s /usr/sbin/nologin -M cc2asn
    sudo mkdir -p /opt/cc2asn-bridge /var/log/cc2asn
    sudo chown cc2asn:cc2asn /var/log/cc2asn

    # code (or: cd bridge && make scp, after the project/zone are gcloud-selected).
    # All installs go through sudo (OS Login users have it) and land as
    # cc2asn-owned so the service user can traverse/read them.
    gcloud compute scp --zone us-east1-b server.py lib/api.py cc2asn-bridge:/tmp
    gcloud compute ssh --zone us-east1-b cc2asn-bridge --command "\sh -c 'sudo install -d -o cc2asn -g cc2asn -m 750 /opt/cc2asn-bridge && sudo install -d -o cc2asn -g cc2asn -m 750 /opt/cc2asn-bridge/lib && sudo install -o cc2asn -g cc2asn -m 640 /tmp/server.py /opt/cc2asn-bridge/server.py && sudo install -o cc2asn -g cc2asn -m 640 /tmp/api.py /opt/cc2asn-bridge/lib/api.py'"

    # conf — holds the secret, by hand:
    gcloud compute scp --zone us-east1-b cc2asn-bridge.conf cc2asn-bridge:/tmp
    gcloud compute ssh --zone us-east1-b cc2asn-bridge --command "\sh -c 'sudo install -m 640 -o cc2asn -g cc2asn /tmp/cc2asn-bridge.conf /etc/cc2asn-bridge.conf'"

    # systemd unit
    gcloud compute scp --zone us-east1-b cc2asn-bridge.service cc2asn-bridge:/tmp
    gcloud compute ssh --zone us-east1-b cc2asn-bridge --command "\sh -c 'sudo install -m 644 /tmp/cc2asn-bridge.service /etc/systemd/system/cc2asn-bridge.service && sudo systemctl daemon-reload && sudo systemctl enable --now cc2asn-bridge'"

## Deploy updates (from the repo, after `gcloud config set project cc2asn-bridge`)

    cd bridge && make deploy       # test -> scp server.py+lib/api.py -> restart
    make restart                   # restart only

Conf/secret changes are manual: edit `/etc/cc2asn-bridge.conf` on the VM, then
`make restart`.

## Neutral smoke (no DNS change needed)

    whois -h <VM-IP> fi
    echo ipv4 dk | nc <VM-IP> 43
    tail /var/log/cc2asn/query.log       # on the VM: user ip, type, cc, status
    # on the AWS side, confirm the same lookup in /cc2asn/api/latest with
    # src=tcp43 and the same client IP.

## DNS cutover (retires rambase)

1. Lower the `cc2asn.com` whois A-record TTL to 300 well before the move.
2. Point the A record at the GCP address (`gcloud compute addresses` output).
3. Keep the rambase server serving until the old TTL fully expires.
4. Cancel the rambase VPS. If a rollback port is ever needed, the VM
   re-creates from this README in ~15 minutes and the repo holds the code.

## Secret rotation

`CC2ASN_BRIDGE_SECRET` accepts a comma-separated list on the API side, so
rotation overlaps: both secrets are trusted during the switchover and no
query degrades. If the two sides ever disagree, queries do NOT fail — they
silently degrade to anonymous direct-traffic treatment (logged `src=http` at
the bridge VM's IP, all users in one shared rate-limit bucket), so verify
with the `/cc2asn/api/latest` `tcp43` marker before shortening the list.

1. Generate a new secret.
2. Lambda env: `CC2ASN_BRIDGE_SECRET=<old>,<new>` (full variable list — see
   `api/README.md` step 5's update-function-configuration command).
3. VM conf: set `BRIDGE_SECRET = <new>` in `/etc/cc2asn-bridge.conf`,
   then `make restart`.
4. Verify `src=tcp43` lines flow with `<new>` on both sides, then set the
   Lambda env to `<new>` alone.

Ordering matters: API-side FIRST (step 2 before 3), never the reverse —
the API accepting only a secret the VM does not yet send degrades the bridge
without an error.

## Retirement (the exit — the design's whole point)

Both sides log greppable per-line data for the decay check:

- VM: `/var/log/cc2asn/query.log` (grep/awk by ip|date)
- API: Logs Insights on group `/cc2asn/api/latest`, filter `src=tcp43`

Check monthly: bridged lookups vs direct API lookups. When the whois fraction
is negligible, retire the bridge: remove the DNS record, delete the VM (and
release the static IP — free only while attached to a running instance).
GitHub/history in this repo is the re-creation path.

Monthly check, concretely (30-day window):

    # API side: bridge vs direct fraction
    aws logs tail /cc2asn/api/latest --region eu-west-1 --since 30d --format short \
        | grep -c 'src=tcp43'
    aws logs tail /cc2asn/api/latest --region eu-west-1 --since 30d --format short \
        | grep -c 'src=http'

    # VM side: total bridged lookups (all rotations, latest 30 days by mtime)
    ls -t /var/log/cc2asn/query.log* | xargs cat | wc -l
