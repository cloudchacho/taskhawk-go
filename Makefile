.PHONY: test

test: clean
	./run-tests.sh

lint:
	golangci-lint --build-tags test run ./...

mod-tidy:
	go mod tidy
	cd examples && go mod tidy

clean:
	find . -name coverage.txt -delete
