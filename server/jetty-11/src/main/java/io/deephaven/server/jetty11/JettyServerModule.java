//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.jetty11;

import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import io.deephaven.plugin.js.JsPluginRegistration;
import io.deephaven.server.config.ServerConfig;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static io.grpc.internal.GrpcUtil.getThreadFactory;

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

        // create a custom executor service, just like grpc would use, so that grpc doesn't shut it down ahead
        // of when we are ready
        // We don't use newSingleThreadScheduledExecutor because it doesn't return a
        // ScheduledThreadPoolExecutor.
        ScheduledThreadPoolExecutor service = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(
                1, getThreadFactory("grpc-timer-%d", true));

        // If there are long timeouts that are cancelled, they will not actually be removed from
        // the executors queue. This forces immediate removal upon cancellation to avoid a
        // memory leak.
        service.setRemoveOnCancelPolicy(true);

        ScheduledExecutorService executorService = Executors.unconfigurableScheduledExecutorService(service);

        serverBuilder.scheduledExecutorService(executorService);

        serverBuilder.maxInboundMessageSize(maxMessageSize);

        serverBuilder.directExecutor();

        serverBuilder.intercept(new JettyCertInterceptor());

        return serverBuilder.buildServletAdapter();
    }

    @Binds
    JsPluginRegistration bindJsPlugins(JsPlugins plugins);

    @Provides
    @Singleton
    static JsPlugins providesJsPluginRegistration() {
        try {
            return JsPlugins.create();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
