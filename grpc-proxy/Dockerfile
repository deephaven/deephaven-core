# Simple dockerfile for the improbable-eng grpc to grpc-web/websocket proxy

# in theory we can use scratch, but we need the sh -c syntax to include envs vars, or hardcode the backend addr
FROM alpine

ADD https://github.com/improbable-eng/grpc-web/releases/download/v0.13.0/grpcwebproxy-v0.13.0-linux-x86_64.zip /grpcwebproxy.zip
RUN set -eux; \
    unzip /grpcwebproxy.zip; \
    mv dist/grpcwebproxy* /grpcwebproxy

EXPOSE 8080 8443

ENV BACKEND_ADDR=""

ENTRYPOINT /grpcwebproxy --run_tls_server=false --backend_addr=$BACKEND_ADDR --backend_tls=false --allow_all_origins --use_websockets --backend_max_call_recv_msg_size=104857600