# run docker-compose from root dir
FROM golang:latest

WORKDIR /testspace

COPY . ./

CMD make test; make test-race;
