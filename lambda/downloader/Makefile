PROJECT = CC2ASN
FUNCTION = $(PROJECT)-downloader
REGION = eu-west-1

all: build

.PHONY: clean build deploy

clean:
	rm -rf build

build: clean
	cp requirements.txt requirements.bkp 2>/dev/null || :
	pip3 freeze > requirements.txt
	mkdir -p build/site-packages
	zip -r build/$(FUNCTION).zip . -x "*.DS_Store*" "*.git*" "build*" "Makefile" "requirements.txt"
	cp -r $$VIRTUAL_ENV/lib/python3.*/site-packages/ build/site-packages
	cd build/site-packages; zip -g -r ../$(FUNCTION).zip . -x "*__pycache__*"

deploy: build
	aws lambda update-function-code \
		--region=$(REGION) \
		--function-name $(FUNCTION) \
		--zip-file fileb://build/$(FUNCTION).zip \
		--publish
