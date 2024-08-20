//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.proto.DeephavenChannel;
import io.deephaven.proto.DeephavenChannelImpl;
import io.grpc.ManagedChannel;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Redacted;

import javax.annotation.Nullable;
import javax.inject.Named;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;

@Immutable
@BuildableStyle
public abstract class SessionImplConfig {

    public static final String DEEPHAVEN_SESSION_BATCH = "deephaven.session.batch";
    public static final String DEEPHAVEN_SESSION_BATCH_STACKTRACES = "deephaven.session.batch.stacktraces";
    public static final String DEEPHAVEN_SESSION_EXECUTE_TIMEOUT = "deephaven.session.executeTimeout";
    public static final String DEEPHAVEN_SESSION_CLOSE_TIMEOUT = "deephaven.session.closeTimeout";

    public static Builder builder() {
        return ImmutableSessionImplConfig.builder();
    }

    public static SessionImplConfig of(
            DeephavenChannel channel,
            ScheduledExecutorService scheduler,
            @Nullable @Named("authenticationTypeAndValue") String authenticationTypeAndValue) {
        final Builder builder = SessionImplConfig.builder()
                .executor(scheduler)
                .channel(channel);
        if (authenticationTypeAndValue != null) {
            builder.authenticationTypeAndValue(authenticationTypeAndValue);
        }
        return builder.build();
    }

    /**
     * A low level adapter from {@link SessionConfig} into {@link SessionImplConfig}. Most callers should prefer to use
     * the higher-level options encapsulated in {@link SessionFactoryConfig}.
     *
     * @param sessionConfig the session config
     * @param managedChannel the managed channel
     * @param defaultScheduler the scheduler to use when {@link SessionConfig#scheduler()} is empty
     * @return the session impl config
     */
    public static SessionImplConfig from(
            SessionConfig sessionConfig,
            ManagedChannel managedChannel,
            ScheduledExecutorService defaultScheduler) {
        final SessionImplConfig.Builder builder = SessionImplConfig.builder()
                .executor(sessionConfig.scheduler().orElse(defaultScheduler))
                .channel(new DeephavenChannelImpl(managedChannel))
                .delegateToBatch(sessionConfig.delegateToBatch())
                .mixinStacktrace(sessionConfig.mixinStacktrace())
                .executeTimeout(sessionConfig.executeTimeout())
                .closeTimeout(sessionConfig.closeTimeout());
        sessionConfig.authenticationTypeAndValue().ifPresent(builder::authenticationTypeAndValue);
        return builder.build();
    }

    public abstract ScheduledExecutorService executor();

    public abstract DeephavenChannel channel();

    @Default
    @Redacted
    public String authenticationTypeAndValue() {
        return "Anonymous";
    }

    /**
     * Whether the {@link Session} implementation will implement a batch {@link TableHandleManager}. By default, is
     * {@code true}. The default can be overridden via the system property {@value DEEPHAVEN_SESSION_BATCH}.
     *
     * @return true if the session will implement a batch manager, false if the session will implement a serial manager
     */
    @Default
    public boolean delegateToBatch() {
        return SessionConfigHelper.delegateToBatch();
    }

    /**
     * Whether the default batch {@link TableHandleManager} will use mix-in more relevant stacktraces. By default, is
     * {@code false}. The default can be overridden via the system property
     * {@value DEEPHAVEN_SESSION_BATCH_STACKTRACES}.
     *
     * @return true if the default batch manager will mix-in stacktraces, false otherwise
     */
    @Default
    public boolean mixinStacktrace() {
        return SessionConfigHelper.mixinStacktrace();
    }

    /**
     * The session execute timeout. By default, is {@code PT1m}. The default can be overridden via the system property
     * {@value DEEPHAVEN_SESSION_EXECUTE_TIMEOUT}.
     *
     * @return the session execute timeout
     */
    @Default
    public Duration executeTimeout() {
        return SessionConfigHelper.executeTimeout();
    }

    /**
     * The {@link Session} and {@link ConsoleSession} close timeout. By default, is {@code PT5s}. The default can be
     * overridden via the system property {@value DEEPHAVEN_SESSION_CLOSE_TIMEOUT}.
     *
     * @return the close timeout
     */
    @Default
    public Duration closeTimeout() {
        return SessionConfigHelper.closeTimeout();
    }

    /**
     * Equivalent to {@code SessionImpl.create(this)}.
     *
     * @return the session
     * @throws InterruptedException if the thread is interrupted
     * @see SessionImpl#create(SessionImplConfig)
     */
    public final SessionImpl createSession() throws InterruptedException {
        return SessionImpl.create(this);
    }

    public interface Builder {

        Builder executor(ScheduledExecutorService executor);

        Builder channel(DeephavenChannel channel);

        Builder authenticationTypeAndValue(String authenticationTypeAndValue);

        Builder delegateToBatch(boolean delegateToBatch);

        Builder mixinStacktrace(boolean mixinStacktrace);

        Builder executeTimeout(Duration executeTimeout);

        Builder closeTimeout(Duration closeTimeout);

        SessionImplConfig build();
    }
}
