#!/usr/bin/env sh

set -o nounset

TLS_DIR="${TLS_DIR:-/etc/deephaven/certs}"
TLS_CRT="${TLS_CRT:-$TLS_DIR/tls.crt}"
TLS_KEY="${TLS_KEY:-$TLS_DIR/tls.key}"
TLS_CA="${TLS_CA:-$TLS_DIR/ca.crt}"

PROXY_TLS_PORT="${PROXY_TLS_PORT:-8443}"
PROXY_DEBUG_PORT="${PROXY_DEBUG_PORT:-8080}"

ALLOWED_ORIGINS="${ALLOWED_ORIGINS:+--allowed_origins=$ALLOWED_ORIGINS}"
ALLOWED_ORIGINS="${ALLOWED_ORIGINS:---allow_all_origins}"

if [ -s "$TLS_CRT" ]; then

    CA_FILES="$TLS_CA"
    [ -f /etc/ssl/certs/ca-certificates.crt ] && CA_FILES="${CA_FILES},/etc/ssl/certs/ca-certificates.crt"
    [ -f /var/run/secrets/kubernetes.io/serviceaccount/ca.crt ] && CA_FILES="${CA_FILES},/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"

    exec /app/dist/grpcwebproxy \
      --run_http_server=false \
      --server_http_debug_port="${PROXY_DEBUG_PORT}" \
      --backend_addr="${BACKEND_ADDR}" \
      --server_http_tls_port="${PROXY_TLS_PORT}" \
      --server_tls_cert_file="$TLS_CRT" \
      --server_tls_key_file="$TLS_KEY" \
      --server_tls_client_ca_files="$CA_FILES" \
      --backend_tls=true \
      "$ALLOWED_ORIGINS" \
      --use_websockets \
      --backend_max_call_recv_msg_size=104857600
else
    exec /app/dist/grpcwebproxy \
      --run_tls_server=false \
      --server_http_debug_port="${PROXY_DEBUG_PORT}" \
      --backend_addr="${BACKEND_ADDR}" \
      --backend_tls=false \
      "${ALLOWED_ORIGINS}" \
      --use_websockets \
      --backend_max_call_recv_msg_size=104857600
fi
