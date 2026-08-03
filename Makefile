.PHONY: build test test-race vet clean
build:
	mkdir -p bin
	go build -o bin/amux ./cmd/amux
	go build -o bin/amuxd ./cmd/amuxd
	go build -o bin/amux-shim ./cmd/amux-shim
test:
	go test ./...
test-race:
	go test -race ./...
vet:
	go vet ./...
clean:
	rm -rf bin
