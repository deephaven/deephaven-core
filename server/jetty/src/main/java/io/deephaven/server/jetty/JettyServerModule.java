package io.deephaven.server.jetty;

import dagger.Module;
import dagger.Provides;
import io.deephaven.server.runner.GrpcServer;
import io.grpc.BindableService;
import io.grpc.ServerInterceptor;
import io.grpc.servlet.ServletAdapter;
import io.grpc.servlet.ServletServerBuilder;

import java.util.Set;

@Module
public class JettyServerModule {

    @Provides
    static GrpcServer bindServer(JettyBackedGrpcServer jettyBackedGrpcServer) {
        return jettyBackedGrpcServer;
    }

    @Provides
    static ServletAdapter provideGrpcServletAdapter(
            Set<BindableService> services,
            Set<ServerInterceptor> interceptors) {
        ServletServerBuilder serverBuilder = new ServletServerBuilder();
        services.forEach(serverBuilder::addService);
        interceptors.forEach(serverBuilder::intercept);

        serverBuilder.directExecutor();
        return serverBuilder.buildServletAdapter();
    }
}
