build-plugins:
	go build -buildmode=plugin -o bin/plugins/commands/command_setget.so plugins/commands/setget/setget.go

build-server:
	go build -o bin/server ./*.go

build: build-plugins build-server

run:
	./bin/server
