PORT?=8000

clean:
	rm -f ./bin/feed-generator
	rm -f ./bin/feed-generator.properties

build: clean
	go install ./feed-generator/feed-generator.go
	cp -n ./etc/feed-generator.properties ./bin/feed-generator.properties

run: build
	PORT=${PORT} ./bin/feed-generator

test:
	go test -race ./...
