.PHONY: test

test: clean
	go test -mod=readonly -v -tags test -race ./...

lint:
	golangci-lint --build-tags test run ./...

mod-tidy:
	go mod tidy
	cd examples && go mod tidy

clean:
	find . -name coverage.txt -delete
