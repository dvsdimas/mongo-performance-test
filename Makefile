PORT?=8000

clean:
	rm -f feed-publisher/feed-publisher

build: clean
	go install ./feed-publisher/feed-publisher.go

run: build
	PORT=${PORT} ./feed-publisher/feed-publisher

test:
	go test -race ./...
