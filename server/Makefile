build-plugins:
	go build -buildmode=plugin -o bin/plugins/commands/command_ping.so plugins/commands/ping/ping.go
	go build -buildmode=plugin -o bin/plugins/commands/command_setget.so plugins/commands/setget/setget.go
	go build -buildmode=plugin -o bin/plugins/commands/command_list.so plugins/commands/list/list.go

build-server:
	go build -o bin/server ./*.go

build: build-plugins build-server

run:
	./bin/server
