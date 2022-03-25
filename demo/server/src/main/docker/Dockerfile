FROM deephaven/server-all-ai:local-build

# Add the extra libraries to the classpath
COPY libs/ /app/libs

HEALTHCHECK --interval=3s --retries=10 --timeout=11s CMD /bin/grpc_health_probe \
    -addr=localhost:8888 \
    -connect-timeout=10s \
    -tls \
    -tls-ca-cert=/etc/ssl/internal/ca.crt \
    -tls-client-key=/etc/ssl/internal/tls.key \
    -tls-client-cert=/etc/ssl/internal/tls.crt || exit 1