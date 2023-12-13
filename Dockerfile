FROM --platform=linux/amd64 alpine:latest

RUN mkdir -p /usr/local/lib/memstore
RUN mkdir -p /opt/memstore/bin
RUN mkdir -p /etc/ssl/certs/memstore

COPY ./bin/linux/x86_64/server /opt/memstore/bin
COPY ./openssl/server /etc/ssl/certs/memstore
COPY ./config /etc/config/memstore

WORKDIR /opt/memstore/bin

CMD "./server" \ 
  "--bindAddr" "${BIND_ADDR}" \
  "--port" "${PORT}" \
  "--mlPort" "${ML_PORT}" \
  "--raftPort" "${RAFT_PORT}" \
  "--serverId" "${SERVER_ID}" \
  "--joinAddr" "${JOIN_ADDR}" \
  "--key" "${KEY}" \
  "--cert" "${CERT}" \
  "--pluginDir" "${PLUGIN_DIR}" \
  "--dataDir" "${DATA_DIR}" \
  "--http=${HTTP}" \
  "--tls=${TLS}" \
  "--inMemory=${IN_MEMORY}" \
  "--bootstrapCluster=${BOOTSTRAP_CLUSTER}" \
  "--aclConfig=${ACL_CONFIG}" \
  "--requirePass=${REQUIRE_PASS}" \
  "--password=${PASSWORD}" \