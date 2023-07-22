FROM golang:1.20.0

WORKDIR /app/memstore

COPY ["./server", "./vendor", "./go.mod", "./go.sum", "./"]
COPY ["./openssl/server", "./ssl"]

RUN go build -o bin/server ./*.go

CMD "./bin/server" \ 
  "--bindAddr" "${BINDADDR}" \
  "--port" "${PORT}" \
  "--mlPort" "${MLPORT}" \
  "--raftPort" "${RAFTPORT}" \
  "--serverId" "${SERVERID}" \
  "--joinAddr" "${JOINADDR}" \
  "--key" "${KEY}" \
  "--cert" "${CERT}" \
  "--http" "${HTTP}" \
  "--tls" "${TLS}"