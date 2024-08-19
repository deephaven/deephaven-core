//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.session;

import io.deephaven.client.impl.BarrageSessionFactoryConfig;
import io.deephaven.client.impl.ClientChannelFactory;
import io.deephaven.client.impl.ClientConfig;
import io.deephaven.client.impl.FlightSessionFactoryConfig;
import io.deephaven.client.impl.SessionConfig;
import io.deephaven.client.impl.SessionFactoryConfig;
import org.apache.arrow.memory.BufferAllocator;

import javax.inject.Inject;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;

public final class SessionFactoryCreator {

    // The server could maintain a default SessionConfig in the future; for example, the potential to have a default
    // outbound authentication configured
    private static final SessionConfig SESSION_CONFIG_EMPTY = SessionConfig.builder().build();

    private final ScheduledExecutorService scheduler;
    private final BufferAllocator allocator;
    private final ClientChannelFactory clientChannelFactory;

    @Inject
    public SessionFactoryCreator(
            ScheduledExecutorService scheduler,
            BufferAllocator allocator,
            ClientChannelFactory clientChannelFactory) {
        this.scheduler = Objects.requireNonNull(scheduler);
        this.allocator = Objects.requireNonNull(allocator);
        this.clientChannelFactory = Objects.requireNonNull(clientChannelFactory);
    }

    /**
     * Creates a {@link SessionFactoryConfig.Factory} by setting up the server defaults for a
     * {@link SessionFactoryConfig}. If {@code clientConfig} does not specify {@link ClientConfig#ssl()} and the target
     * is secure, a {@code clientConfig} with {@code defaultOutboundSsl} will be used.
     *
     * @param clientConfig the client configuration
     * @return the barrage factory
     */
    public SessionFactoryConfig.Factory sessionFactory(ClientConfig clientConfig) {
        return SessionFactoryConfig.builder()
                .clientConfig(clientConfig)
                .clientChannelFactory(clientChannelFactory)
                .sessionConfig(SESSION_CONFIG_EMPTY)
                .scheduler(scheduler)
                .build()
                .factory();
    }

    /**
     * Creates a {@link FlightSessionFactoryConfig.Factory} by setting up the server defaults for a
     * {@link FlightSessionFactoryConfig}. If {@code clientConfig} does not specify {@link ClientConfig#ssl()} and the
     * target is secure, a {@code clientConfig} with {@code defaultOutboundSsl} will be used.
     *
     * @param clientConfig the client configuration
     * @return the flight factory
     */
    public FlightSessionFactoryConfig.Factory flightFactory(ClientConfig clientConfig) {
        return FlightSessionFactoryConfig.builder()
                .clientConfig(clientConfig)
                .clientChannelFactory(clientChannelFactory)
                .sessionConfig(SESSION_CONFIG_EMPTY)
                .allocator(allocator)
                .scheduler(scheduler)
                .build()
                .factory();
    }

    /**
     * Creates a {@link BarrageSessionFactoryConfig.Factory} by setting up the server defaults for a
     * {@link BarrageSessionFactoryConfig}. If {@code clientConfig} does not specify {@link ClientConfig#ssl()} and the
     * target is secure, a {@code clientConfig} with {@code defaultOutboundSsl} will be used.
     *
     * @param clientConfig the client configuration
     * @return the barrage factory
     */
    public BarrageSessionFactoryConfig.Factory barrageFactory(ClientConfig clientConfig) {
        return BarrageSessionFactoryConfig.builder()
                .clientConfig(clientConfig)
                .clientChannelFactory(clientChannelFactory)
                .sessionConfig(SESSION_CONFIG_EMPTY)
                .allocator(allocator)
                .scheduler(scheduler)
                .build()
                .factory();
    }
}
