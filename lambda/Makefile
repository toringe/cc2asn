PROJECT = CC2ASN

$(eval deploy:;@:)

.PHONY: downloader parser curator

downloader parser curator:
ifeq (deploy, $(filter deploy,$(MAKECMDGOALS)))
	@echo 'Deploy $@'
	$(eval FUNCTION := $(PROJECT)-$@)
	$(eval REGION := $(shell pcregrep -o1 "^REGION = \"([a-z]{2}-[a-z]*-\d)\"" $@.py))
	cp requirements.txt requirements.bkp 2>/dev/null || :
	pip3 freeze > requirements.txt
	rm -rf build
	mkdir -p build/site-packages
	zip -r build/$(FUNCTION).zip $@.py
	cp -r $$VIRTUAL_ENV/lib/python3.*/site-packages/ build/site-packages
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
	pip3 freeze > requirements.txt
	rm -rf build
	mkdir -p build/site-packages
	zip -r build/$(FUNCTION).zip $@.py 
	cp -r $$VIRTUAL_ENV/lib/python3.*/site-packages/ build/site-packages
	cd build/site-packages; zip -g -r ../$(FUNCTION).zip . -x "*__pycache__*"
endif

all: downloader parser curator 

clean:
	rm -rf build