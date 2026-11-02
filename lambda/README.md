`make [deploy] <function>`

`make downloader` will build a package for the downloader function

`make deploy parser` will build a package and deploy the parser function

`make deploy all` will package and deploy all three functions

`make runtime` will update the runtime of all three deployed functions to `python3.14` (no code upload). Use `make runtime-parser` for a single function. Note: run `make runtime` **before** the first `make deploy` on a runtime upgrade, so the new code is uploaded to a function already running the new runtime.


`make clean` will remove the build sub-directory

