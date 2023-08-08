FROM alpine:3.14

RUN mkdir -p /usr/local/lib/memstore
RUN mkdir -p /opt/memstore/bin
RUN mkdir -p /etc/ssl/certs/memstore

COPY ./server/bin/plugins /usr/local/lib/memstore
COPY ./server/bin/server /opt/memstore/bin
COPY ./openssl/server /etc/ssl/certs/memstore

WORKDIR /opt/memstore/bin

CMD "./server" \ 
  "--bindAddr" "${BINDADDR}" \
  "--port" "${PORT}" \
  "--mlPort" "${MLPORT}" \
  "--raftPort" "${RAFTPORT}" \
  "--serverId" "${SERVERID}" \
  "--joinAddr" "${JOINADDR}" \
  "--key" "${KEY}" \
  "--cert" "${CERT}" \
  "--http" "${HTTP}" \
  "--tls" "${TLS}" \
  "--inMemory" "${INMEMORY}" \