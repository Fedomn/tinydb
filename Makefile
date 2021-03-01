default: test

test:
	@go test -v ./... | sed /PASS/s//$(shell printf "\033[32mPASS\033[0m")/ | sed /FAIL/s//$(shell printf "\033[31mFAIL\033[0m")/