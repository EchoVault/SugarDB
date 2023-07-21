FROM golang:1.20.0

WORKDIR /app/memstore

COPY ["./server", "./vendor", "./go.mod", "./go.sum", "./"]
COPY ["./openssl/server", "./ssl"]

RUN go build -o bin/server ./*.go

CMD ["./bin/server", "--config", "./config.json", "--tls"]

EXPOSE 7480
EXPOSE 8000
EXPOSE 7946

