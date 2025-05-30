.PHONY: run build clean test

# Run the application
run:
	go run main.go

# Build the application
build:
	go build -o gosqlite

# Clean build artifacts
clean:
	rm -f gosqlite

# Run tests
test:
	go test ./...

# Install dependencies
deps:
	go mod tidy

# Format code
fmt:
	go fmt ./...

# Run linter
lint:
	golangci-lint run 