/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.jetty;

import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import io.deephaven.server.config.ServerConfig;
import io.deephaven.server.jetty.jsplugin.JsPlugins;
import io.deephaven.server.runner.GrpcServer;
import io.grpc.BindableService;
import io.grpc.ServerInterceptor;
import io.grpc.servlet.jakarta.ServletAdapter;
import io.grpc.servlet.jakarta.ServletServerBuilder;

import javax.inject.Named;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Set;

@Module
public interface JettyServerModule {

    @Binds
    GrpcServer bindServer(JettyBackedGrpcServer jettyBackedGrpcServer);

    @Binds
    ServerConfig bindsServerConfig(JettyConfig serverConfig);

    @Provides
    static ServletAdapter provideGrpcServletAdapter(
            final @Named("grpc.maxInboundMessageSize") int maxMessageSize,
            final Set<BindableService> services,
            final Set<ServerInterceptor> interceptors) {
        final ServletServerBuilder serverBuilder = new ServletServerBuilder();
        services.forEach(serverBuilder::addService);
        interceptors.forEach(serverBuilder::intercept);

        serverBuilder.maxInboundMessageSize(maxMessageSize);

        serverBuilder.directExecutor();

        serverBuilder.intercept(new JettyCertInterceptor());

        return serverBuilder.buildServletAdapter();
    }

    @Provides
    @Singleton
    static JsPlugins providesJsPlugins() {
        try {
            return JsPlugins.create();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
