build-plugins:
	CGO_ENABLED=1 CC=x86_64-unknown-linux-musl-gcc GOOS=linux GOARCH=amd64 go build -buildmode=plugin -o bin/plugins/commands/command_ping.so plugins/commands/ping/command.go
	CGO_ENABLED=1 CC=x86_64-unknown-linux-musl-gcc GOOS=linux GOARCH=amd64 go build -buildmode=plugin -o bin/plugins/commands/command_setget.so plugins/commands/setget/command.go
	CGO_ENABLED=1 CC=x86_64-unknown-linux-musl-gcc GOOS=linux GOARCH=amd64 go build -buildmode=plugin -o bin/plugins/commands/command_list.so plugins/commands/list/command.go

build-server:
	CGO_ENABLED=1 CC=x86_64-unknown-linux-musl-gcc GOOS=linux GOARCH=amd64 go build -o bin/server ./*.go

build: build-plugins build-server
