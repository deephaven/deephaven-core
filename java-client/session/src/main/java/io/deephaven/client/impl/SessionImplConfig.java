//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.proto.DeephavenChannel;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import javax.annotation.Nullable;
import javax.inject.Named;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;

import static java.lang.Boolean.parseBoolean;

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

    public abstract ScheduledExecutorService executor();

    public abstract DeephavenChannel channel();

    @Default
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
        final String property = System.getProperty(DEEPHAVEN_SESSION_BATCH);
        return property == null || parseBoolean(property);
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
        return Boolean.getBoolean(DEEPHAVEN_SESSION_BATCH_STACKTRACES);
    }

    /**
     * The session execute timeout. By default, is {@code PT1m}. The default can be overridden via the system property
     * {@value DEEPHAVEN_SESSION_EXECUTE_TIMEOUT}.
     *
     * @return the session execute timeout
     */
    @Default
    public Duration executeTimeout() {
        return Duration.parse(System.getProperty(DEEPHAVEN_SESSION_EXECUTE_TIMEOUT, "PT1m"));
    }

    /**
     * The {@link Session} and {@link ConsoleSession} close timeout. By default, is {@code PT5s}. The default can be
     * overridden via the system property {@value DEEPHAVEN_SESSION_CLOSE_TIMEOUT}.
     *
     * @return the close timeout
     */
    @Default
    public Duration closeTimeout() {
        return Duration.parse(System.getProperty(DEEPHAVEN_SESSION_CLOSE_TIMEOUT, "PT5s"));
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
