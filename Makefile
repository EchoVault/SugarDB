run:
	docker-compose up --build

build-local:
	CGO_ENABLED=1 go build -buildmode=plugin -o ./bin/modules/module_set/module_set.so ./internal/volumes/modules/module_set/module_set.go && \
	CGO_ENABLED=1 go build -buildmode=plugin -o ./bin/modules/module_get/module_get.so ./internal/volumes/modules/module_get/module_get.go && \
	CGO_ENABLED=1 go build -o ./bin ./...


build-modules-test:
	CGO_ENABLED=1 go build --race=$(RACE) -buildmode=plugin -o $(OUT)/modules/module_set/module_set.so ./internal/volumes/modules/module_set/module_set.go && \
	CGO_ENABLED=1 go build --race=$(RACE) -buildmode=plugin -o $(OUT)/modules/module_get/module_get.so ./internal/volumes/modules/module_get/module_get.go

test:
	env RACE=false OUT=internal/modules/admin/testdata make build-modules-test && \
	env RACE=false OUT=sugardb/testdata make build-modules-test && \
	CGO_ENABLED=1 go test ./... -coverprofile coverage/coverage.out

test-race:
	env RACE=true OUT=internal/modules/admin/testdata make build-modules-test && \
	env RACE=true OUT=sugardb/testdata make build-modules-test && \
	CGO_ENABLED=1 go test ./... --race

testenv-run:
	docker-compose -f test_env/run/docker-compose.yaml build
	docker-compose -f test_env/run/docker-compose.yaml run projenv

testenv-test:
	docker-compose -f test_env/test/docker-compose.yaml up --build
	
testenv-test-race:
	docker-compose -f test_env/test_race/docker-compose.yaml up --build

testenv-all:
	docker-compose -f test_env/all/docker-compose.yaml up --build

cover:
	go tool cover -html=./coverage/coverage.out

benchmark:
	go run redis_benchmark.go $(if $(commands),-commands="$(commands)") $(if $(use_local_server),-use_local_server)
