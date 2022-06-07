package io.deephaven.server.jetty;

import io.deephaven.base.system.PrintStreamGlobals;
import io.deephaven.configuration.Configuration;
import io.deephaven.server.jetty.JettyConfig.Builder;
import io.deephaven.server.runner.Main;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

public class JettyMain extends Main {
    public static void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException, TimeoutException {
        final Configuration config = init(args, Main.class);

        // Defaults defined in JettyConfig
        int httpSessionExpireMs = config.getIntegerWithDefault("http.session.durationMs", -1);
        String httpHost = config.getStringWithDefault("http.host", null);
        int httpPort = config.getIntegerWithDefault("http.port", -1);
        String httpWebsockets = config.getStringWithDefault("http.websockets", null);
        int schedulerPoolSize = config.getIntegerWithDefault("scheduler.poolSize", -1);
        int maxInboundMessageSize = config.getIntegerWithDefault("grpc.maxInboundMessageSize", -1);

        Builder builder = JettyConfig.builder();
        if (httpSessionExpireMs > -1) {
            builder.tokenExpire(Duration.ofMillis(httpSessionExpireMs));
        }
        if (httpHost != null) {
            builder.host(httpHost);
        }
        if (httpPort > -1) {
            builder.port(httpPort);
        }
        if (schedulerPoolSize > -1) {
            builder.schedulerPoolSize(schedulerPoolSize);
        }
        if (maxInboundMessageSize > -1) {
            builder.maxInboundMessageSize(maxInboundMessageSize);
        }
        if (httpWebsockets != null) {
            builder.websockets(Boolean.parseBoolean(httpWebsockets));
        }
        Main.parseSSLConfig(config).ifPresent(builder::ssl);

        DaggerJettyServerComponent
                .builder()
                .withJettyConfig(builder.build())
                .withOut(PrintStreamGlobals.getOut())
                .withErr(PrintStreamGlobals.getErr())
                .build()
                .getServer()
                .run()
                .join();
    }
}
