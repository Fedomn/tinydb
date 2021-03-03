GOPATH	?= $(shell go env GOPATH)
CURDIR	= $(shell go list -f '{{.Dir}}' ./...)
FILES	:= $$(find $(CURDIR) -name "*.go")

default: test fmtcheck

test:
	@go test -v ./... | sed /PASS/s//$(shell printf "\033[32mPASS\033[0m")/ | sed /FAIL/s//$(shell printf "\033[31mFAIL\033[0m")/

fmtcheck:
	@echo "fmtcheck"
	@command -v goimports > /dev/null 2>&1 || GO111MODULE=off go get golang.org/x/tools/cmd/goimports
	@CHANGES="$$(goimports -d $(CURDIR))"; \
		if [ -n "$${CHANGES}" ]; then \
			echo "Unformatted (run goimports -w .):\n\n$${CHANGES}\n\n"; \
			exit 1; \
		fi
	@# Annoyingly, goimports does not support the simplify flag.
	@CHANGES="$$(gofmt -s -d $(CURDIR))"; \
		if [ -n "$${CHANGES}" ]; then \
			echo "Unformatted (run gofmt -s -w .):\n\n$${CHANGES}\n\n"; \
			exit 1; \
		fi