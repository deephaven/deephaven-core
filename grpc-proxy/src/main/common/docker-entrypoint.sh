#!/usr/bin/env sh

set -o nounset

TLS_DIR="${TLS_DIR:-/etc/deephaven/certs}"

PROXY_TLS_PORT="${PROXY_TLS_PORT:-8443}"
PROXY_DEBUG_PORT="${PROXY_DEBUG_PORT:-8008}"

ALLOWED_ORIGINS="${ALLOWED_ORIGINS:+--allowed_origins=$ALLOWED_ORIGINS}"
ALLOWED_ORIGINS="${ALLOWED_ORIGINS:---allow_all_origins}"

if [ -d "$TLS_DIR" ]; then

    exec /app/dist/grpcwebproxy \
      --run_http_server=false \
      --server_http_debug_port="${PROXY_DEBUG_PORT}" \
      --backend_addr="${BACKEND_ADDR}" \
      --server_http_tls_port="${PROXY_TLS_PORT}" \
      --server_tls_cert_file="$TLS_DIR/tls.crt" \
      --server_tls_key_file="$TLS_DIR/tls.key" \
      --server_tls_client_ca_files="$TLS_DIR/ca.crt,/etc/ssl/certs/ca-certificates.crt,/var/run/secrets/kubernetes.io/serviceaccount/ca.crt" \
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
