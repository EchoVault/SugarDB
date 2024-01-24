FROM --platform=linux/amd64 alpine:latest

RUN mkdir -p /usr/local/lib/echovault
RUN mkdir -p /opt/echovault/bin
RUN mkdir -p /etc/ssl/certs/echovault

COPY ./bin/linux/x86_64/server /opt/echovault/bin
COPY ./openssl/server /etc/ssl/certs/echovault

WORKDIR /opt/echovault/bin

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
  "--tls=${TLS}" \
  "--inMemory=${IN_MEMORY}" \
  "--bootstrapCluster=${BOOTSTRAP_CLUSTER}" \
  "--aclConfig=${ACL_CONFIG}" \
  "--requirePass=${REQUIRE_PASS}" \
  "--password=${PASSWORD}" \
  "--forwardCommand=${FORWARD_COMMAND}" \