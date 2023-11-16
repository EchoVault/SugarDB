build-plugins:
	CGO_ENABLED=$(CGO_ENABLED) CC=$(CC) GOOS=$(GOOS) GOARCH=$(GOARCH) go build -buildmode=plugin -o $(DEST)/commands/command_ping.so ./src/plugins/commands/ping/command.go
	CGO_ENABLED=$(CGO_ENABLED) CC=$(CC) GOOS=$(GOOS) GOARCH=$(GOARCH) go build -buildmode=plugin -o $(DEST)/commands/command_set.so ./src/plugins/commands/set/command.go
	CGO_ENABLED=$(CGO_ENABLED) CC=$(CC) GOOS=$(GOOS) GOARCH=$(GOARCH) go build -buildmode=plugin -o $(DEST)/commands/command_get.so ./src/plugins/commands/get/command.go
	CGO_ENABLED=$(CGO_ENABLED) CC=$(CC) GOOS=$(GOOS) GOARCH=$(GOARCH) go build -buildmode=plugin -o $(DEST)/commands/command_list.so ./src/plugins/commands/list/command.go

build-server:
	CGO_ENABLED=$(CGO_ENABLED) CC=$(CC) GOOS=$(GOOS) GOARCH=$(GOARCH) go build -o $(DEST)/server ./src/*.go

build:
	env CGO_ENABLED=1 CC=x86_64-linux-musl-gcc GOOS=linux GOARCH=amd64 DEST=bin/linux/x86_64/plugins make build-plugins
	env CGO_ENABLED=1 CC=x86_64-linux-musl-gcc GOOS=linux GOARCH=amd64 DEST=bin/linux/x86_64 make build-server
