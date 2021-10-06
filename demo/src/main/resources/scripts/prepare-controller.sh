# While we _should_ get docker-compose.yml from github via curl -O https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python-examples/docker-compose.yml
# the version there isn't parameterized like this one, as we want to replace the docker repo with our demo-specific one
test -f "$DH_DIR/docker-compose.yml" || cat << 'EOF' > "$DH_DIR/docker-compose.yml"
version: "3.4"

services:
  demo-server:
    image: ${REPO:-ghcr.io/deephaven}/demo-server:${VERSION:-latest}
    expose:
      - '7117'
    volumes:
      # TODO: reduce this to /root/.config/gcloud and whatever other minimal tools we need.
      - /root/.config:/root/.config
    environment:
      - JAVA_TOOL_OPTIONS="-Xmx12g -Dquarkus.http.cors.origins=https://${DOMAIN:-demo.deephaven.app}"

  envoy:
    image: envoyproxy/envoy:v1.18.3
    depends_on:
      - demo-server
    ports:
      - "${PORT:-10000}:10000"
    volumes:
      - /etc/ssl/dh:/etc/ssl/dh
      - /etc/ssl/internal:/etc/ssl/internal
      - /etc/envoy:/etc/envoy

EOF
# End default docker-compose.yml

test -d /etc/envoy || mkdir -p /etc/envoy
cat << EOF > /etc/envoy/envoy.yaml
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
                      path: "/dev/stdout"
                codec_type: AUTO
                stat_prefix: ingress_https
                route_config:
                  name: local_route
                  virtual_hosts:
                    - name: reverse_proxy
                      domains: ["*"]
                      routes:
                        - match: # Call to / goes to the landing page
                            prefix: "/"
                          route: { cluster: control, timeout: 120s }
                common_http_protocol_options:
                  max_stream_duration: 120s
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
              session_timeout: 120s
              common_tls_context:
                alpn_protocols: ["h2","http/1.1"]
                tls_certificates:
                  - certificate_chain:
                      filename: /etc/ssl/dh/tls.crt
                    private_key:
                      filename: /etc/ssl/dh/tls.key
  clusters:
    - name: control
      connect_timeout: 10s
      http_protocol_options:
        max_stream_duration: 120s
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
                      address: 0.0.0.0
                      port_value: 7117

EOF