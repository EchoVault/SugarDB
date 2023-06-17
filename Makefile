
build:
	go build -o bin/main main.go

run:
	./bin/main

all: build run