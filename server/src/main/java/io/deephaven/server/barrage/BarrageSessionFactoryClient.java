//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.barrage;

import io.deephaven.appmode.ApplicationState;
import io.deephaven.appmode.ApplicationState.Factory;
import io.deephaven.appmode.ApplicationState.Listener;
import io.deephaven.client.impl.BarrageSessionFactory;
import io.deephaven.client.impl.BarrageSessionFactoryBuilder;
import io.deephaven.client.impl.ClientChannelFactory;
import io.deephaven.client.impl.ClientConfig;
import io.deephaven.ssl.config.SSLConfig;
import org.apache.arrow.memory.BufferAllocator;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;

public final class BarrageSessionFactoryClient {

    private final BarrageSessionFactoryBuilder factoryBuilder;
    private final ScheduledExecutorService scheduler;
    private final BufferAllocator allocator;
    private final ClientChannelFactory clientChannelFactory;
    private final SSLConfig sslConfig;

    @Inject
    public BarrageSessionFactoryClient(
            BarrageSessionFactoryBuilder factoryBuilder,
            ScheduledExecutorService scheduler,
            BufferAllocator allocator,
            @Named("client.sslConfig") SSLConfig sslConfig,
            ClientChannelFactory clientChannelFactory) {
        this.factoryBuilder = Objects.requireNonNull(factoryBuilder);
        this.scheduler = Objects.requireNonNull(scheduler);
        this.allocator = Objects.requireNonNull(allocator);
        this.sslConfig = Objects.requireNonNull(sslConfig);
        this.clientChannelFactory = Objects.requireNonNull(clientChannelFactory);
    }

    /**
     * Creates a {@link BarrageSessionFactory}. If {@code clientConfig} does not specify {@link ClientConfig#ssl()} and
     * the target is secure, a {@code clientConfig} with {@code sslConfig} will be used. Equivalent to
     * 
     * <pre>
     * factoryBuilder
     *         .managedChannel(clientChannelFactory.create(clientConfig))
     *         .scheduler(scheduler)
     *         .allocator(allocator)
     *         .authenticationTypeAndValue(authenticationTypeAndValue)
     *         .build()
     * </pre>
     * 
     * @param clientConfig the client configuration
     * @param authenticationTypeAndValue the authentication type and value
     * @return the barrage session factory
     */
    public BarrageSessionFactory factory(ClientConfig clientConfig, @Nullable String authenticationTypeAndValue) {
        final ClientConfig config;
        if (clientConfig.ssl().isEmpty() && clientConfig.target().isSecure()) {
            config = clientConfig.withSsl(sslConfig);
        } else {
            config = clientConfig;
        }
        return factoryBuilder
                .managedChannel(clientChannelFactory.create(config))
                .scheduler(scheduler)
                .allocator(allocator)
                .authenticationTypeAndValue(authenticationTypeAndValue)
                .build();
    }

    /**
     * Provides an application id as {@link BarrageSessionFactoryClient} class name. A
     * {@link BarrageSessionFactoryClient} is set as the field name {@value INSTANCE}.
     */
    public static final class Application implements Factory {

        public static final String INSTANCE = "instance";

        private final BarrageSessionFactoryClient barrageSessionFactoryClient;

        @Inject
        public Application(BarrageSessionFactoryClient barrageSessionFactoryClient) {
            this.barrageSessionFactoryClient = Objects.requireNonNull(barrageSessionFactoryClient);
        }

        @Override
        public ApplicationState create(Listener appStateListener) {
            final ApplicationState state =
                    new ApplicationState(appStateListener, BarrageSessionFactoryClient.class.getName(),
                            Application.class.getSimpleName());
            state.setField(INSTANCE, barrageSessionFactoryClient);
            return state;
        }
    }
}
