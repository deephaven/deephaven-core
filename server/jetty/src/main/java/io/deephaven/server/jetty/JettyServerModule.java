package io.deephaven.server.jetty;

import dagger.Module;
import dagger.Provides;
import io.deephaven.server.runner.GrpcServer;
import io.grpc.BindableService;
import io.grpc.ServerInterceptor;
import io.grpc.servlet.jakarta.ServletAdapter;
import io.grpc.servlet.jakarta.ServletServerBuilder;

import javax.inject.Named;
import java.io.File;
import java.util.Set;

@Module
public class JettyServerModule {

    @Provides
    static GrpcServer bindServer(JettyBackedGrpcServer jettyBackedGrpcServer) {
        return jettyBackedGrpcServer;
    }

    @Provides
    static ServletAdapter provideGrpcServletAdapter(
            final @Named("grpc.maxInboundMessageSize") int maxMessageSize,
            final Set<BindableService> services,
            final Set<ServerInterceptor> interceptors) {
        final ServletServerBuilder serverBuilder = new ServletServerBuilder();

        String chainLoc = System.getenv("DH_TLS_CHAIN");
        if (chainLoc != null) {
            String chainKey = System.getenv("DH_TLS_KEY");
            final File chain = new File(chainLoc);
            final File key = new File(chainKey);
            if (!chain.exists()) {
                throw new IllegalArgumentException("TLS chain " + chain + " does not exist!");
            }
            if (!key.exists()) {
                throw new IllegalArgumentException("TLS key " + key + " does not exist!");
            }
            serverBuilder.useTransportSecurity(chain, key);
        }
        services.forEach(serverBuilder::addService);
        interceptors.forEach(serverBuilder::intercept);

        serverBuilder.maxInboundMessageSize(maxMessageSize);

        serverBuilder.directExecutor();
        return serverBuilder.buildServletAdapter();
    }
}
