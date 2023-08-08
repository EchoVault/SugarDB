build-plugins-macos:
	GOOS=darwin GOARCH=arm64 go build -buildmode=plugin -o bin/macos/plugins/commands/command_ping.so plugins/commands/ping/command.go
	GOOS=darwin GOARCH=arm64 go build -buildmode=plugin -o bin/macos/plugins/commands/command_setget.so plugins/commands/setget/command.go
	GOOS=darwin GOARCH=arm64 go build -buildmode=plugin -o bin/macos/plugins/commands/command_list.so plugins/commands/list/command.go

build-plugins-linux:
	CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -buildmode=plugin -o bin/linux/plugins/commands/command_ping.so plugins/commands/ping/command.go
	CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -buildmode=plugin -o bin/linux/plugins/commands/command_setget.so plugins/commands/setget/command.go
	CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -buildmode=plugin -o bin/linux/plugins/commands/command_list.so plugins/commands/list/command.go

build-server-macos:
	GOOS=darwin GOARCH=arm64 go build -o bin/macos/server ./*.go

build-server-linux:
	GOOS=linux GOARCH=amd64 go build -o bin/linux/server ./*.go

build-macos: build-plugins-macos build-server-macos

build-linux: build-plugins-linux build-server-linux

build: build-macos build-linux 

run-macos:
	./bin/macos/server

run-linux:
	./bin/linux/server
