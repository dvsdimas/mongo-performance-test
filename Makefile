PORT?=8000

clean:
	rm -f ./bin/feed-publisher
	rm -f ./bin/feed-publisher.properties

build: clean
	go install ./feed-publisher/feed-publisher.go
	cp -n ./etc/feed-publisher.properties ./bin/feed-publisher.properties

run: build
	PORT=${PORT} ./bin/feed-publisher

test:
	go test -race ./...
