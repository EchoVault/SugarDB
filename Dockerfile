FROM --platform=linux/amd64 alpine:latest

RUN mkdir -p /usr/local/lib/echovault
RUN mkdir -p /opt/echovault/bin
RUN mkdir -p /etc/ssl/certs/echovault/server
RUN mkdir -p /etc/ssl/certs/echovault/client

COPY ./bin/linux/x86_64/server /opt/echovault/bin
COPY ./openssl/server /etc/ssl/certs/echovault/server
COPY ./openssl/client /etc/ssl/certs/echovault/client

WORKDIR /opt/echovault/bin

CMD "./server" \
  "--bindAddr" "${BIND_ADDR}" \
  "--port" "${PORT}" \
  "--mlPort" "${ML_PORT}" \
  "--raftPort" "${RAFT_PORT}" \
  "--serverId" "${SERVER_ID}" \
  "--joinAddr" "${JOIN_ADDR}" \
  "--pluginDir" "${PLUGIN_DIR}" \
  "--dataDir" "${DATA_DIR}" \
  "--snapshotThreshold" "${SNAPSHOT_THRESHOLD}" \
  "--snapshotInterval" "${SNAPSHOT_INTERVAL}" \
  "--tls=${TLS}" \
  "--mtls=${MTLS}" \
  "--inMemory=${IN_MEMORY}" \
  "--bootstrapCluster=${BOOTSTRAP_CLUSTER}" \
  "--aclConfig=${ACL_CONFIG}" \
  "--requirePass=${REQUIRE_PASS}" \
  "--password=${PASSWORD}" \
  "--forwardCommand=${FORWARD_COMMAND}" \
  "--restoreSnapshot=${RESTORE_SNAPSHOT}" \
  "--restoreAOF=${RESTORE_AOF}" \
  # List of server cert/key pairs
  "--certKeyPair=${CERT_KEY_PAIR_1}" \
  "--certKeyPair=${CERT_KEY_PAIR_2}" \
  # List of client certs
  "--clientCA=${CLIENT_CA_1}" \
