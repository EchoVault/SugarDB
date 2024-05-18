build-modules:
	CGO_ENABLED=$(CGO_ENABLED) CC=$(CC) GOOS=$(GOOS) GOARCH=$(GOARCH) go build -buildmode=plugin -o $(DEST)/module_set/module_set.so ./volumes/modules/module_set/module_set.go && \
	CGO_ENABLED=$(CGO_ENABLED) CC=$(CC) GOOS=$(GOOS) GOARCH=$(GOARCH) go build -buildmode=plugin -o $(DEST)/module_get/module_get.so ./volumes/modules/module_get/module_get.go

build-modules-test:
	CGO_ENABLED=1 go build --race=$(RACE) -buildmode=plugin -o $(OUT)/modules/module_set/module_set.so ./volumes/modules/module_set/module_set.go && \
	CGO_ENABLED=1 go build --race=$(RACE) -buildmode=plugin -o $(OUT)/modules/module_get/module_get.so ./volumes/modules/module_get/module_get.go

build-server:
	 CGO_ENABLED=$(CGO_ENABLED) CC=$(CC) GOOS=$(GOOS) GOARCH=$(GOARCH) go build -o $(DEST)/server ./cmd/main.go

build:
	env CGO_ENABLED=1 CC=x86_64-linux-musl-gcc GOOS=linux GOARCH=amd64 DEST=volumes/modules make build-modules && \
	env CGO_ENABLED=1 CC=x86_64-linux-musl-gcc GOOS=linux GOARCH=amd64 DEST=bin/linux/x86_64 make build-server

run:
	make build && \
	docker-compose up --build

test-unit:
	env RACE=false OUT=internal/modules/admin/testdata make build-modules-test && \
	env RACE=false OUT=echovault/testdata make build-modules-test && \
	go clean -testcache && \
	CGO_ENABLED=1 go test ./... -coverprofile coverage/coverage.out

test-race:
	env RACE=true OUT=internal/modules/admin/testdata make build-modules-test && \
	env RACE=true OUT=echovault/testdata make build-modules-test && \
	go clean -testcache && \
	CGO_ENABLED=1 go test ./... --race