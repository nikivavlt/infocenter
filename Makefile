run: build
	./server

build:
	go build -o server .

test:
	go test -race -v ./...