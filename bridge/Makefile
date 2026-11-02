VENV ?= ../.venv
PY ?= $(VENV)/bin/python
ZONE ?= us-east1-b
VM ?= cc2asn-bridge

.PHONY: test deploy scp restart clean

test:
	$(PY) -m unittest discover -s test -t .

# Deploy both code files and the unit. The conf is NOT pushed — it holds the
# bridge secret; update /etc/cc2asn-bridge.conf by hand (or scp explicitly).
scp:
	gcloud compute scp --zone $(ZONE) server.py lib/api.py $(VM):/tmp --quiet
	gcloud compute ssh --zone $(ZONE) $(VM) --quiet --command "\sh -c 'install -m 750 /tmp/server.py /opt/cc2asn-bridge/server.py && install -d -m 750 /opt/cc2asn-bridge/lib && install -m 750 /tmp/api.py /opt/cc2asn-bridge/lib/api.py && test -f /usr/bin/python3 && echo ok'"

restart:
	gcloud compute ssh --zone $(ZONE) $(VM) --quiet --command "\sh -c 'sudo systemctl restart cc2asn-bridge && sleep 1 && sudo systemctl status cc2asn-bridge --no-pager -l | head -12'"

deploy: test scp restart

clean:
	rm -rf __pycache__ test/__pycache__ lib/__pycache__
