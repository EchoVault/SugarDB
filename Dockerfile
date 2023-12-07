FROM --platform=linux/amd64 alpine:latest

RUN mkdir -p /usr/local/lib/memstore
RUN mkdir -p /opt/memstore/bin
RUN mkdir -p /etc/ssl/certs/memstore

COPY ./bin/linux/x86_64/plugins /usr/local/lib/memstore
COPY ./bin/linux/x86_64/server /opt/memstore/bin
COPY ./openssl/server /etc/ssl/certs/memstore
COPY ./config /etc/config/memstore

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
  "--pluginDir" "${PLUGINDIR}" \
  "--dataDir" "${DATADIR}" \
  "--http=${HTTP}" \
  "--tls=${TLS}" \
  "--inMemory=${INMEMORY}" \
  "--bootstrapCluster=${BOOTSTRAP_CLUSTER}" \
  "--aclConfig=${ACL_CONFIG}" \