//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.client.grpc.UserAgentUtility;
import io.grpc.ManagedChannel;
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
public abstract class SessionFactoryConfig {

    private static final SessionConfig SESSION_CONFIG_EMPTY = SessionConfig.builder().build();

    static final List<String> VERSION_PROPERTIES =
            Collections.singletonList(UserAgentUtility.versionProperty("deephaven", SessionFactoryConfig.class));

    private static final String DEEPHAVEN_JAVA_CLIENT_SESSION = "deephaven-java-client-session";

    private static final ClientChannelFactory CLIENT_CHANNEL_FACTORY = ClientChannelFactoryDefaulter.builder()
            .userAgent(userAgent(Collections.singletonList(DEEPHAVEN_JAVA_CLIENT_SESSION)))
            .build();

    public static Builder builder() {
        return ImmutableSessionFactoryConfig.builder();
    }

    /**
     * Constructs a <a href="https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md#user-agents">grpc
     * user-agent</a> with {@code grpc-java} and {@code deephaven} versions, with the addition of
     * {@code extraProperties}.
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
     * {@link #userAgent(List)}) and the property {@value DEEPHAVEN_JAVA_CLIENT_SESSION}.
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
     * The default scheduler, used by the factory when {@link SessionConfig#scheduler()} is not set.
     */
    public abstract ScheduledExecutorService scheduler();

    /**
     * Creates a new factory with a new {@link ManagedChannel}.
     *
     * @return the factory
     */
    public final Factory factory() {
        return new Factory(clientChannelFactory().create(clientConfig()));
    }

    public final class Factory implements SessionFactory {

        private final ManagedChannel managedChannel;

        private Factory(ManagedChannel managedChannel) {
            this.managedChannel = Objects.requireNonNull(managedChannel);
        }

        /**
         * {@inheritDoc} Equivalent to {@code newSession(defaultSessionConfig())}.
         */
        @Override
        public Session newSession() {
            return newSession(sessionConfig());
        }

        /**
         * Creates a new {@link Session} with {@code sessionConfig}. Closing the session does <b>not</b> close the
         * {@link #managedChannel()}.
         *
         * @param sessionConfig the session config
         * @return the new session
         */
        public Session newSession(SessionConfig sessionConfig) {
            try {
                return sessionImpl(sessionConfig);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        @Override
        public ManagedChannel managedChannel() {
            return managedChannel;
        }

        private SessionImpl sessionImpl(SessionConfig sessionConfig) throws InterruptedException {
            return SessionImpl.create(SessionImplConfig.from(sessionConfig, managedChannel, scheduler()));
        }
    }

    // ------------------------------------------------

    public interface Builder {
        Builder clientConfig(ClientConfig clientConfig);

        Builder clientChannelFactory(ClientChannelFactory clientChannelFactory);

        Builder sessionConfig(SessionConfig sessionConfig);

        Builder scheduler(ScheduledExecutorService scheduler);

        SessionFactoryConfig build();
    }
}
