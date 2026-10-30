PROJECT = CC2ASN
FUNCTION = CC2ASN-api
REGION = eu-west-1
LAYER_ARN = arn:aws:lambda:$(REGION):753240598075:layer:LambdaAdapterLayerArm64

# Required for deploy-create; see README deployment checklist steps 1-2.
ROLE_ARN =
LAYER_VERSION =
deploy-create: check-create-env build
check-create-env:
ifeq ($(ROLE_ARN),)
	$(error set ROLE_ARN=arn:aws:iam::<account>:role/CC2ASN-api-role — README step 2)
endif
ifeq ($(LAYER_VERSION),)
	$(error set LAYER_VERSION=<n> from the list-layer-versions read-only check — README step 0)
endif

# The venv to build from: the repo's uv-managed venv (repo root/.venv) by
# default, overridable with `make VENV=/path/to/other/venv`. Deliberately
# ignores any activated VIRTUAL_ENV, so a build is byte-identical no matter
# what shell it runs from.
VENV ?= $(abspath ../.venv)
PY := $(VENV)/bin/python
SITE := $(shell $(PY) -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])')

# Fail fast for the build target (clean/echo never need the venv)
ifneq (,$(filter build,$(MAKECMDGOALS)))
ifeq ($(wildcard $(PY)),)
$(error No interpreter at $(VENV)/bin/python — run `uv sync` in the repo root, or pass VENV=/path/to/venv)
endif
endif

# Deployments touch the AWS account and are gated: pass CONFIRM_DEPLOY=yes.
# All deploy steps also require the user's explicit go-ahead in conversation.
ifneq (,$(filter deploy deploy-create,$(MAKECMDGOALS)))
ifneq (yes,$(CONFIRM_DEPLOY))
$(error safety gate: pass CONFIRM_DEPLOY=yes after explicit user approval; read the README deployment checklist first)
endif
endif

.PHONY: build clean deploy deploy-create

build: check-freeze
	@echo 'Build $(FUNCTION) package'
	rm -rf build
	mkdir -p build/site-packages
	zip -r build/$(FUNCTION).zip server.py run.sh lib -x "*__pycache__*"
	cp -R $(SITE)/ build/site-packages
	cd build/site-packages; zip -g -r ../$(FUNCTION).zip . -x "*__pycache__*" -x "*test/*"
	@unzip -l build/$(FUNCTION).zip | tail -1

check-freeze:
	cp requirements.txt requirements.bkp 2>/dev/null || :
	uv pip freeze --python $(PY) > requirements.txt

clean:
	rm -rf build

# One-time creation; see README deployment checklist for role and table setup.
deploy-create:
	@echo 'Creating function, layer, and URL config: $(FUNCTION) in $(REGION)'
	aws lambda create-function \
		--region=$(REGION) \
		--function-name $(FUNCTION) \
		--runtime python3.14 \
		--architectures arm64 \
		--memory-size 512 \
		--timeout 10 \
		--role $(ROLE_ARN) \
		--handler run.sh \
		--layers $(LAYER_ARN):$(LAYER_VERSION) \
		--zip-file fileb://build/$(FUNCTION).zip \
		--environment Variables="{AWS_LWA_EXEC_WRAPPER=/opt/bootstrap,PORT=8080,CC2ASN_DB_BUCKET=cc2asn-db,CC2ASN_TOKENS_TABLE=cc2asn-tokens}"
	aws lambda create-function-url-config \
		--region=$(REGION) \
		--function-name $(FUNCTION) \
		--auth-type NONE \
		--invoke-mode BUFFERED

deploy:
	@echo 'Deploy $(FUNCTION) (code update)'
	aws lambda update-function-code \
		--region=$(REGION) \
		--function-name $(FUNCTION) \
		--zip-file fileb://build/$(FUNCTION).zip \
		--publish
