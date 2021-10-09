FROM deephaven/grpc-api:local-build

# Add the extra libraries to the classpath
COPY libs/ /app/libs

# Add the application directory
COPY app.d/ /app.d

HEALTHCHECK --interval=2s --retries=10 --timeout=6s CMD /bin/grpc_health_probe \
    -addr=localhost:8888 \
    -connect-timeout=10s \
    -tls \
    -tls-ca-cert=/etc/ssl/internal/ca.crt \
    -tls-client-key=/etc/ssl/internal/tls.key \
    -tls-client-cert=/etc/ssl/internal/tls.crt || exit 1