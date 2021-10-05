DH_DIR="${DH_DIR:-/dh}"
# While we _should_ get docker-compose.yml from github via curl -O https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python-examples/docker-compose.yml
# the version there isn't parameterized like this one, as we want to replace the docker repo with our demo-specific one
cat << 'EOF' > "$DH_DIR/docker-compose.yml"
version: "3.4"

services:
  grpc-api:
    image: ${REPO:-ghcr.io/deephaven}/grpc-api:${VERSION:-latest}
    expose:
      - '8888'
    volumes:
      - ./data:/data
      - api-cache:/cache
      - /etc/ssl/dh:/etc/ssl/dh
      - /etc/ssl/internal:/etc/ssl/internal
    environment:
      - JAVA_TOOL_OPTIONS=-Xmx2g -Ddeephaven.console.type=${TYPE:-python}
      - DH_TLS_CHAIN=/etc/ssl/internal/tls.crt
      - DH_TLS_KEY=/etc/ssl/internal/tls.key.pk8
      - EXTRA_HEALTH_ARGS="-tls -tls-no-verify"

  demo-server:
    image: ${REPO:-ghcr.io/deephaven}/demo-server:${VERSION:-latest}
    expose:
      - '7117'
    volumes:
      - api-cache:/cache
    depends_on:
      grpc-api:
        condition: service_healthy
    environment:
      - JAVA_TOOL_OPTIONS="-Xmx12g -Dquarkus.http.cors.origins=https://${FIRST_DOMAIN:-demo.deephaven.app}"

  web:
    image: ${REPO:-ghcr.io/deephaven}/web:${VERSION:-latest}
    expose:
      - '8080'
    volumes:
      - ./data:/data
      - /etc/ssl/dh:/etc/ssl/dh
      - web-tmp:/tmp

  envoy:
    # We use stock envoy and pass in our own envoy.yaml, since we require tls deephaven localhost does not
    image: envoyproxy/envoy:v1.18.3
    ports:
      - "${PORT:-10000}:10000"
    depends_on:
      grpc-api:
        condition: service_healthy
      web:
        condition: service_started

    volumes:
      - /etc/ssl/dh:/etc/ssl/dh
      - /etc/ssl/internal:/etc/ssl/internal
      - /etc/envoy:/etc/envoy

  examples:
    # image: ${REPO:-ghcr.io/deephaven}/examples
    # this one isn't deployed to the gcloud docker repo
    image: ghcr.io/deephaven/examples
    volumes:
      - ./data:/data
    command: initialize

volumes:
    web-tmp:
    api-cache:

EOF
# End default docker-compose.yml

test -d /etc/envoy || mkdir -p /etc/envoy
cat << 'EOF' > /etc/envoy/envoy.yaml
admin:
  # access_log_path: /dev/stdout
  access_log_path: /tmp/admin_access.log
  address:
    socket_address:
      address: 127.0.0.1
      port_value: 9090
static_resources:
  listeners:
    - name: listener_0
      address:
        socket_address:
          address: 0.0.0.0
          port_value: 10000
      filter_chains:
        - filters:
            - name: envoy.filters.network.http_connection_manager
              typed_config:
                "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
                access_log:
                  - name: envoy.access_loggers.file
                    typed_config:
                      "@type": type.googleapis.com/envoy.extensions.access_loggers.file.v3.FileAccessLog
                      path: "/dev/stdout" # easier debugging
                codec_type: AUTO
                stat_prefix: ingress_https
                upgrade_configs:
                  - upgrade_type: websocket
                stream_idle_timeout: 0s
                route_config:
                  name: local_route
                  virtual_hosts:
                    - name: reverse_proxy
                      domains: ["*"]
                      routes:
                        - match: # Call to / goes to the landing page
                            path: "/"
                          route: { cluster: web }
                        - match: # Web IDE lives in this path
                            prefix: "/ide"
                          route: { cluster: web }
                        - match: # JS API lives in this path
                            prefix: "/jsapi"
                          route: { cluster: web }
                        - match: # Notebook file storage at this path
                            prefix: "/notebooks"
                          route: { cluster: web }
                        - match: # Application mode layout storage at this path
                            prefix: "/layouts"
                          route: { cluster: web }
                        - match: # The controller serves /health url
                            prefix: "/health"
                          route: { cluster: control }
                        - match: # The controller serves /health url
                            prefix: "/healthz"
                          route: { cluster: control }
                        - match: # Any GRPC call is assumed to be forwarded to the real service
                            prefix: "/"
                          route:
                            cluster: grpc-api
                            max_stream_duration:
                              grpc_timeout_header_max: 0s
                            timeout: 0s
                http_filters:
                  - name: envoy.filters.http.health_check
                    typed_config:
                      "@type": type.googleapis.com/envoy.extensions.filters.http.health_check.v3.HealthCheck
                      pass_through_mode: false
                      headers:
                        - name: ":path"
                          exact_match: "/healthz"
                        - name: "x-envoy-livenessprobe"
                          exact_match: "healthz"
                  - name: envoy.filters.http.grpc_web
                  - name: envoy.filters.http.router
          transport_socket:
            name: envoy.transport_sockets.tls
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.DownstreamTlsContext
              session_timeout: 3600s
              common_tls_context:
                alpn_protocols: ["h2"]
                tls_certificates:
                  - certificate_chain:
                      filename: /etc/ssl/dh/tls.crt
                    private_key:
                      filename: /etc/ssl/dh/tls.key
  clusters:
    - name: grpc-api
      connect_timeout: 10s
      type: LOGICAL_DNS
      lb_policy: ROUND_ROBIN
      http2_protocol_options: {}
      common_http_protocol_options:
        max_stream_duration: 3000s
      load_assignment:
        cluster_name: grpc-api
        endpoints:
          - lb_endpoints:
              - endpoint:
                  address:
                    socket_address:
                      address: grpc-api
                      # address: 127.0.0.1
                      port_value: 8888
      #      health_checks:
      #        timeout: 1s
      #        interval: 10s
      #        unhealthy_threshold: 2
      #        healthy_threshold: 2
      #        grpc_health_check: { }
      transport_socket:
        name: envoy.transport_sockets.tls
        typed_config:
          "@type": type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.UpstreamTlsContext
          common_tls_context:
            validation_context:
              trusted_ca:
                filename: /etc/ssl/internal/ca.crt
    - name: web
      connect_timeout: 10s
      type: LOGICAL_DNS
      lb_policy: ROUND_ROBIN
      http_protocol_options: {}
      load_assignment:
        cluster_name: web
        endpoints:
          - lb_endpoints:
              - endpoint:
                  hostname: web
                  address:
                    socket_address:
                      address: web
                      #address: 127.0.0.1
                      port_value: 8080
    - name: control
      connect_timeout: 10s
      type: LOGICAL_DNS
      lb_policy: ROUND_ROBIN
      http_protocol_options: {}
      load_assignment:
        cluster_name: control
        endpoints:
          - lb_endpoints:
              - endpoint:
                  hostname: control
                  address:
                    socket_address:
                      address: demo-server
                      port_value: 7117
EOF