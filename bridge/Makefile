VENV ?= ../.venv
PY ?= $(VENV)/bin/python
ZONE ?= us-east1-b
VM ?= cc2asn-bridge

.PHONY: test deploy scp restart clean

test:
	$(PY) -m unittest discover -s test -t .

# Deploy code updates. Both files land as cc2asn-owned, readable by the
# service user. NOTE: gcloud compute scp cannot sudo; we scp to /tmp and
# install with sudo over SSH. The conf is NOT pushed — it holds the bridge
# secret; update /etc/cc2asn-bridge.conf by hand (or scp explicitly).
# SSH/SCP go through IAP only (default-allow-ssh deleted 2026-11-03;
# access is via the allow-ingress-from-iap rule, 35.235.240.0/20). Every
# gcloud ssh/scp carries --tunnel-through-iap — without it gcloud tries the
# public IP first and stalls now that tcp:22 is closed to the world.
scp:
	gcloud compute scp --tunnel-through-iap --zone $(ZONE) server.py lib/api.py $(VM):/tmp --quiet
	gcloud compute ssh --tunnel-through-iap --zone $(ZONE) $(VM) --quiet --command "\sh -c 'sudo install -d -o cc2asn -g cc2asn -m 750 /opt/cc2asn-bridge/lib && sudo install -o cc2asn -g cc2asn -m 640 /tmp/server.py /opt/cc2asn-bridge/server.py && sudo install -o cc2asn -g cc2asn -m 640 /tmp/api.py /opt/cc2asn-bridge/lib/api.py'"

restart:
	gcloud compute ssh --tunnel-through-iap --zone $(ZONE) $(VM) --quiet --command "\sh -c 'sudo systemctl restart cc2asn-bridge && sleep 1 && sudo systemctl status cc2asn-bridge --no-pager -l | head -12'"

deploy: test scp restart

clean:
	rm -rf __pycache__ test/__pycache__ lib/__pycache__
