PROJECT = CC2ASN

# The venv to build from: the repo's uv-managed venv (repo root/.venv) by
# default, overridable with `make VENV=/path/to/other/venv`. Deliberately
# ignores any activated VIRTUAL_ENV, so a build is byte-identical no matter
# what shell it runs from.
VENV ?= $(abspath ../.venv)
PY := $(VENV)/bin/python
SITE := $(shell $(PY) -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])')

# Fail fast for targets that actually build (clean/runtime need no venv)
ifneq (,$(filter downloader parser curator all,$(MAKECMDGOALS)))
ifeq ($(wildcard $(PY)),)
$(error No interpreter at $(VENV)/bin/python — run `uv sync` in the repo root, or pass VENV=/path/to/venv)
endif
endif

$(eval deploy:;@:)

.PHONY: downloader parser curator runtime runtime-downloader runtime-parser runtime-curator

downloader parser curator:
ifeq (deploy, $(filter deploy,$(MAKECMDGOALS)))
	@echo 'Deploy $@'
	$(eval FUNCTION := $(PROJECT)-$@)
	$(eval REGION := $(shell pcregrep -o1 "^REGION = \"([a-z]{2}-[a-z]*-\d)\"" $@.py))
	cp requirements.txt requirements.bkp 2>/dev/null || :
	uv pip freeze --python $(PY) > requirements.txt
	rm -rf build
	mkdir -p build/site-packages
	zip -r build/$(FUNCTION).zip $@.py
	cp -R $(SITE)/ build/site-packages
	cd build/site-packages; zip -g -r ../$(FUNCTION).zip . -x "*__pycache__*"
	aws lambda update-function-code \
		--region=$(REGION) \
		--function-name $(FUNCTION) \
		--zip-file fileb://build/$(FUNCTION).zip \
		--publish
else
	@echo 'Build $@ package'
	$(eval FUNCTION := $(PROJECT)-$@)
	cp requirements.txt requirements.bkp 2>/dev/null || :
	uv pip freeze --python $(PY) > requirements.txt
	rm -rf build
	mkdir -p build/site-packages
	zip -r build/$(FUNCTION).zip $@.py 
	cp -R $(SITE)/ build/site-packages
	cd build/site-packages; zip -g -r ../$(FUNCTION).zip . -x "*__pycache__*"
endif

# Update the runtime of a deployed function (no code upload):
#   make runtime            update all three functions
#   make runtime-parser     update a single function
RUNTIME = python3.14
runtime-downloader runtime-parser runtime-curator:
	$(eval FN := $(subst runtime-,,$@))
	$(eval REGION := $(shell pcregrep -o1 "^REGION = \"([a-z]{2}-[a-z]*-\d)\"" $(FN).py))
	@echo 'Set runtime of $(PROJECT)-$(FN) to $(RUNTIME)'
	aws lambda update-function-configuration \
		--region=$(REGION) \
		--function-name $(PROJECT)-$(FN) \
		--runtime $(RUNTIME)
runtime: runtime-downloader runtime-parser runtime-curator

all: downloader parser curator 

clean:
	rm -rf build