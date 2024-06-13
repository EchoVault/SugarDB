build-modules-test:
	CGO_ENABLED=1 go build --race=$(RACE) -buildmode=plugin -o $(OUT)/modules/module_set/module_set.so ./volumes/modules/module_set/module_set.go && \
	CGO_ENABLED=1 go build --race=$(RACE) -buildmode=plugin -o $(OUT)/modules/module_get/module_get.so ./volumes/modules/module_get/module_get.go

run:
	docker-compose up --build

test:
	env RACE=false OUT=internal/modules/admin/testdata make build-modules-test && \
	env RACE=false OUT=echovault/testdata make build-modules-test && \
	CGO_ENABLED=1 go test ./... -coverprofile coverage/coverage.out

test-race:
	env RACE=true OUT=internal/modules/admin/testdata make build-modules-test && \
	env RACE=true OUT=echovault/testdata make build-modules-test && \
	CGO_ENABLED=1 go test ./... --race

cover:
	go tool cover -html=./coverage/coverage.out

