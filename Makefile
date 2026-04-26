build:
	go build -o bin/publisher ./cmd/publisher

run:
	go run ./cmd/publisher/main.go

clean:
	rm -rf bin/
