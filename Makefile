run:
	docker-compose up --build

build-local:
	CGO_ENABLED=1 go build -buildmode=plugin -o ./bin/modules/module_set/module_set.so ./volumes/modules/module_set/module_set.go && \
	CGO_ENABLED=1 go build -buildmode=plugin -o ./bin/modules/module_get/module_get.so ./volumes/modules/module_get/module_get.go && \
	CGO_ENABLED=1 go build -o ./bin ./cmd/...


build-modules-test:
	CGO_ENABLED=1 go build --race=$(RACE) -buildmode=plugin -o $(OUT)/modules/module_set/module_set.so ./volumes/modules/module_set/module_set.go && \
	CGO_ENABLED=1 go build --race=$(RACE) -buildmode=plugin -o $(OUT)/modules/module_get/module_get.so ./volumes/modules/module_get/module_get.go

test:
	env RACE=false OUT=internal/modules/admin/testdata make build-modules-test && \
	env RACE=false OUT=echovault/testdata make build-modules-test && \
	CGO_ENABLED=1 go test ./... -coverprofile coverage.out

test-race:
	env RACE=true OUT=internal/modules/admin/testdata make build-modules-test && \
	env RACE=true OUT=echovault/testdata make build-modules-test && \
	CGO_ENABLED=1 go test ./... --race

cover:
	go tool cover -html=./coverage.out

