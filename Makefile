build-server:
	CGO_ENABLED=$(CGO_ENABLED) CC=$(CC) GOOS=$(GOOS) GOARCH=$(GOARCH) go build -o $(DEST)/server ./src/*.go

build:
	env CGO_ENABLED=1 CC=x86_64-linux-musl-gcc GOOS=linux GOARCH=amd64 DEST=bin/linux/x86_64 make build-server

test:
	go clean -testcache && go test ./src/... -coverprofile coverage/cover.out
