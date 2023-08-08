build-plugins:
	go build -buildmode=plugin -o bin/plugins/commands/command_ping.so plugins/commands/ping/command.go
	go build -buildmode=plugin -o bin/plugins/commands/command_setget.so plugins/commands/setget/command.go
	go build -buildmode=plugin -o bin/plugins/commands/command_list.so plugins/commands/list/command.go

build-server:
	go build -o bin/server ./*.go

build: build-plugins build-server

run:
	./bin/server