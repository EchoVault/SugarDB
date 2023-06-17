build-server:
	go build -o bin/server server/main.go

build-client:
	go build -o bin/client client/main.go

run-server:
	./bin/server

run-client:
	./bin/client

server: build-server run-server

client: build-client run-client