CLIENT_PORT=3379
PEER_PORT=3380

MEMBER_1_NAME=etcd0
MEMBER_1_IP=127.0.0.1
MEMBER_1_DATA="./etcd-data-0"

MEMBER_2_NAME=etcd1
MEMBER_2_IP=127.0.0.2
MEMBER_2_DATA="./etcd-data-1"

MEMBER_3_NAME=etcd2
MEMBER_3_IP=127.0.0.3
MEMBER_3_DATA="./etcd-data-2"

LOGS_DIR="./etcd-logs"

INITIAL_CLUSTER="etcd0=https://${MEMBER_1_IP}:${PEER_PORT},etcd1=https://${MEMBER_2_IP}:${PEER_PORT},etcd2=https://${MEMBER_3_IP}:${PEER_PORT}"

if [ -d "${MEMBER_1_DATA}" ]; then rm -Rf ./${MEMBER_1_DATA}; fi
if [ -d "${MEMBER_2_DATA}" ]; then rm -Rf ./${MEMBER_2_DATA}; fi
if [ -d "${MEMBER_3_DATA}" ]; then rm -Rf ./${MEMBER_3_DATA}; fi
if [ -d "${LOGS_DIR}" ]; then rm -Rf ./${LOGS_DIR}; fi
mkdir -p ${LOGS_DIR}

etcd --name $MEMBER_1_NAME \
     --advertise-client-urls https://${MEMBER_1_IP}:${CLIENT_PORT} \
     --listen-client-urls https://${MEMBER_1_IP}:${CLIENT_PORT} \
     --initial-advertise-peer-urls https://${MEMBER_1_IP}:${PEER_PORT} \
     --listen-peer-urls https://${MEMBER_1_IP}:${PEER_PORT} \
     --initial-cluster-token etcd-cluster-1 \
     --initial-cluster $INITIAL_CLUSTER \
     --client-cert-auth \
     --trusted-ca-file ./certs/ca.pem \
     --cert-file ./certs/server.pem \
     --key-file ./certs/server.key \
     --peer-client-cert-auth \
     --peer-trusted-ca-file ./certs/ca.pem \
     --peer-cert-file ./certs/server.pem \
     --peer-key-file ./certs/server.key \
     --data-dir ${MEMBER_1_DATA} \
     --log-outputs "${LOGS_DIR}/${MEMBER_1_NAME}.log" \
     -initial-cluster-state new &
ETCD_1_PID=$!

etcd --name $MEMBER_2_NAME \
     --advertise-client-urls https://${MEMBER_2_IP}:${CLIENT_PORT} \
     --listen-client-urls https://${MEMBER_2_IP}:${CLIENT_PORT} \
     --initial-advertise-peer-urls https://${MEMBER_2_IP}:${PEER_PORT} \
     --listen-peer-urls https://${MEMBER_2_IP}:${PEER_PORT} \
     --initial-cluster-token etcd-cluster-1 \
     --initial-cluster $INITIAL_CLUSTER \
     --client-cert-auth \
     --trusted-ca-file ./certs/ca.pem \
     --cert-file ./certs/server.pem \
     --key-file ./certs/server.key \
     --peer-client-cert-auth \
     --peer-trusted-ca-file ./certs/ca.pem \
     --peer-cert-file ./certs/server.pem \
     --peer-key-file ./certs/server.key \
     --data-dir ${MEMBER_2_DATA} \
     --log-outputs "${LOGS_DIR}/${MEMBER_2_NAME}.log" \
     -initial-cluster-state new &
ETCD_2_PID=$!

etcd --name $MEMBER_3_NAME \
     --advertise-client-urls https://${MEMBER_3_IP}:${CLIENT_PORT} \
     --listen-client-urls https://${MEMBER_3_IP}:${CLIENT_PORT} \
     --initial-advertise-peer-urls https://${MEMBER_3_IP}:${PEER_PORT} \
     --listen-peer-urls https://${MEMBER_3_IP}:${PEER_PORT} \
     --initial-cluster-token etcd-cluster-1 \
     --initial-cluster $INITIAL_CLUSTER \
     --client-cert-auth \
     --trusted-ca-file ./certs/ca.pem \
     --cert-file ./certs/server.pem \
     --key-file ./certs/server.key \
     --peer-client-cert-auth \
     --peer-trusted-ca-file ./certs/ca.pem \
     --peer-cert-file ./certs/server.pem \
     --peer-key-file ./certs/server.key \
     --data-dir ${MEMBER_3_DATA} \
     --log-outputs "${LOGS_DIR}/${MEMBER_3_NAME}.log" \
     -initial-cluster-state new &
ETCD_3_PID=$!

kill_etcd() {
    kill -9 $ETCD_1_PID
    kill -9 $ETCD_2_PID
    kill -9 $ETCD_3_PID
}

trap 'kill_etcd' SIGINT

echo "Etcd Running"
sleep infinity