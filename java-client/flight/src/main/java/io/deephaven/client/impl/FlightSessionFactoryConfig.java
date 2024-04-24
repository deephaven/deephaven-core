//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import io.deephaven.annotations.BuildableStyle;
import io.grpc.ManagedChannel;
import org.apache.arrow.memory.BufferAllocator;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;

@Immutable
@BuildableStyle
public abstract class FlightSessionFactoryConfig {
    private static final SessionConfig SESSION_CONFIG_EMPTY = SessionConfig.builder().build();
    private static final FlightSessionConfig FLIGHT_SESSION_CONFIG_EMPTY = FlightSessionConfig.builder().build();

    public static Builder builder() {
        return ImmutableFlightSessionFactoryConfig.builder();
    }

    /**
     * The client configuration.
     */
    public abstract ClientConfig clientConfig();

    /**
     * The client channel factory. By default is {@link ClientChannelFactory#defaultInstance()}.
     */
    @Default
    public ClientChannelFactory clientChannelFactory() {
        return ClientChannelFactory.defaultInstance();
    }

    /**
     * The default session config, used by the factory when {@link SessionConfig} is not provided. By default is
     * {@code SessionConfig.builder().build()}.
     */
    @Default
    public SessionConfig sessionConfig() {
        return SESSION_CONFIG_EMPTY;
    }

    /**
     * The default scheduler, used by the factory when {@link SessionConfig#scheduler()} is not set.
     */
    public abstract ScheduledExecutorService scheduler();

    /**
     * The allocator.
     */
    public abstract BufferAllocator allocator();

    /**
     * Creates a new factory with a new {@link ManagedChannel}.
     *
     * @return the factory
     */
    public final Factory factory() {
        return new Factory(SessionFactoryConfig.builder()
                .clientConfig(clientConfig())
                .clientChannelFactory(clientChannelFactory())
                .sessionConfig(sessionConfig())
                .scheduler(scheduler())
                .build()
                .factory());
    }

    public final class Factory implements FlightSessionFactory {
        private final SessionFactoryConfig.Factory factory;

        private Factory(SessionFactoryConfig.Factory factory) {
            this.factory = Objects.requireNonNull(factory);
        }

        @Override
        public FlightSession newFlightSession() {
            final Session session = factory.newSession();
            return FlightSession.of((SessionImpl) session, allocator(), factory.managedChannel());
        }

        /**
         * Creates a new {@link FlightSession} with {@code sessionConfig}. Closing the session does <b>not</b> close the
         * {@link #managedChannel()}.
         *
         * @param sessionConfig the session config
         * @return the new flight session
         */
        public FlightSession newFlightSession(SessionConfig sessionConfig) {
            final Session session = factory.newSession(sessionConfig);
            return FlightSession.of((SessionImpl) session, allocator(), factory.managedChannel());
        }

        @Override
        public ManagedChannel managedChannel() {
            return factory.managedChannel();
        }
    }

    public interface Builder {

        Builder clientConfig(ClientConfig clientConfig);

        Builder clientChannelFactory(ClientChannelFactory clientChannelFactory);

        Builder sessionConfig(SessionConfig sessionConfig);

        Builder scheduler(ScheduledExecutorService scheduler);

        Builder allocator(BufferAllocator allocator);

        FlightSessionFactoryConfig build();
    }
}
