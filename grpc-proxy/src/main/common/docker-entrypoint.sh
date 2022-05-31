#!/usr/bin/env sh

set -o nounset

exec /app/dist/grpcwebproxy \
  --run_tls_server=false \
  --backend_addr="${BACKEND_ADDR}" \
  --backend_tls=false \
  --allow_all_origins \
  --use_websockets \
  --backend_max_call_recv_msg_size=104857600
