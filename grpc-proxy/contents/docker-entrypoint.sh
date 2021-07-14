#!/usr/bin/env sh

set -o nounset

# Default port is 8080, but that conflicts w/ grpc-api when sharing space inside a kubernetes pod
# so, we'll just move the proxy to 8008
PROXY_DEBUG_PORT=8008

exec /app/dist/grpcwebproxy \
  --run_tls_server=false \
  --backend_addr="${BACKEND_ADDR}" \
  --backend_tls=false \
  --allow_all_origins \
  --use_websockets \
  --server_http_debug_port="${PROXY_DEBUG_PORT}" \
  --backend_max_call_recv_msg_size=104857600

