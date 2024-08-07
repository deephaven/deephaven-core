//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.barrage.flatbuf.BarrageMessageWrapper;
import io.deephaven.client.grpc.UserAgentUtility;
import io.grpc.ManagedChannel;
import org.apache.arrow.memory.BufferAllocator;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Immutable
@BuildableStyle
public abstract class BarrageSessionFactoryConfig {
    private static final SessionConfig SESSION_CONFIG_EMPTY = SessionConfig.builder().build();

    static final List<String> VERSION_PROPERTIES = Collections.unmodifiableList(Stream.concat(
            FlightSessionFactoryConfig.VERSION_PROPERTIES.stream(),
            Stream.of(UserAgentUtility.versionProperty("barrage", BarrageMessageWrapper.class)))
            .collect(Collectors.toUnmodifiableList()));

    private static final String DEEPHAVEN_JAVA_CLIENT_BARRAGE = "deephaven-java-client-barrage";

    private static final ClientChannelFactory CLIENT_CHANNEL_FACTORY = ClientChannelFactoryDefaulter.builder()
            .userAgent(userAgent(List.of(DEEPHAVEN_JAVA_CLIENT_BARRAGE)))
            .build();

    public static Builder builder() {
        return ImmutableBarrageSessionFactoryConfig.builder();
    }

    /**
     * Constructs a <a href="https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md#user-agents">grpc
     * user-agent</a> with {@code grpc-java}, {@code deephaven}, {@code flight}, and {@code barrage} versions, with the
     * addition of {@code extraProperties}.
     *
     * @param extraProperties the extra properties
     * @return the user-agent
     * @see UserAgentUtility#userAgent(List)
     */
    public static String userAgent(List<String> extraProperties) {
        return UserAgentUtility.userAgent(Stream.concat(
                VERSION_PROPERTIES.stream(),
                extraProperties.stream())
                .collect(Collectors.toList()));
    }

    /**
     * The client configuration.
     */
    public abstract ClientConfig clientConfig();

    /**
     * The client channel factory. By default, is a factory that sets a user-agent which includes relevant versions (see
     * {@link #userAgent(List)}) and the property {@value DEEPHAVEN_JAVA_CLIENT_BARRAGE}.
     */
    @Default
    public ClientChannelFactory clientChannelFactory() {
        return CLIENT_CHANNEL_FACTORY;
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
     * The scheduler, used by the factory when {@link SessionConfig#scheduler()} is not set.
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

    public final class Factory implements BarrageSessionFactory {
        private final SessionFactoryConfig.Factory factory;

        private Factory(SessionFactoryConfig.Factory factory) {
            this.factory = Objects.requireNonNull(factory);
        }

        @Override
        public BarrageSession newBarrageSession() {
            final Session session = factory.newSession();
            return BarrageSession.of((SessionImpl) session, allocator(), factory.managedChannel());
        }

        /**
         * Creates a new {@link BarrageSession} with {@code sessionConfig}. Closing the session does <b>not</b> close
         * the {@link #managedChannel()}.
         *
         * @param sessionConfig the session config
         * @return the new barrage session
         */
        public BarrageSession newBarrageSession(SessionConfig sessionConfig) {
            final Session session = factory.newSession(sessionConfig);
            return BarrageSession.of((SessionImpl) session, allocator(), factory.managedChannel());
        }

        @Override
        public ManagedChannel managedChannel() {
            return factory.managedChannel();
        }
    }

    // ------------------------------------------------

    public interface Builder {

        Builder clientConfig(ClientConfig clientConfig);

        Builder clientChannelFactory(ClientChannelFactory clientChannelFactory);

        Builder sessionConfig(SessionConfig sessionConfig);

        Builder scheduler(ScheduledExecutorService scheduler);

        Builder allocator(BufferAllocator allocator);

        BarrageSessionFactoryConfig build();
    }
}
