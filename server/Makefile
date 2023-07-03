build-plugins:
	go build -buildmode=plugin -o bin/plugins/commands/command_get.so plugins/commands/get/get.go
	go build -buildmode=plugin -o bin/plugins/commands/command_set.so plugins/commands/set/set.go

build-server:
	go build -o bin/server ./*.go

build: build-plugins build-server

run:
	./bin/server
