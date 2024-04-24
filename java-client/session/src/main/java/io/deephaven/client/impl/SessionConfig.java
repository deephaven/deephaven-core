//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Redacted;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;

/**
 * The configuration values for a specific {@link Session}, typically used along with a {@link ClientConfig channel
 * configuration}.
 */
@Immutable
@BuildableStyle
public abstract class SessionConfig {

    public static Builder builder() {
        return ImmutableSessionConfig.builder();
    }

    /**
     * The authentication type and value.
     */
    @Redacted
    public abstract Optional<String> authenticationTypeAndValue();

    /**
     * The scheduler.
     */
    public abstract Optional<ScheduledExecutorService> scheduler();

    /**
     * Whether the {@link Session} implementation will implement a batch {@link TableHandleManager}. By default, is
     * {@code true}. The default can be overridden via the system property
     * {@value SessionImplConfig#DEEPHAVEN_SESSION_BATCH}.
     */
    @Default
    public boolean delegateToBatch() {
        return SessionConfigHelper.delegateToBatch();
    }

    /**
     * Whether the default batch {@link TableHandleManager} will use mix-in more relevant stacktraces. By default, is
     * {@code false}. The default can be overridden via the system property
     * {@value SessionImplConfig#DEEPHAVEN_SESSION_BATCH_STACKTRACES}.
     */
    @Default
    public boolean mixinStacktrace() {
        return SessionConfigHelper.mixinStacktrace();
    }

    /**
     * The session execute timeout. By default, is {@code PT1m}. The default can be overridden via the system property
     * {@value SessionImplConfig#DEEPHAVEN_SESSION_EXECUTE_TIMEOUT}.
     */
    @Default
    public Duration executeTimeout() {
        return SessionConfigHelper.executeTimeout();
    }

    /**
     * The {@link Session} and {@link ConsoleSession} close timeout. By default, is {@code PT5s}. The default can be
     * overridden via the system property {@value SessionImplConfig#DEEPHAVEN_SESSION_CLOSE_TIMEOUT}.
     */
    @Default
    public Duration closeTimeout() {
        return SessionConfigHelper.closeTimeout();
    }

    public interface Builder {
        Builder authenticationTypeAndValue(String authenticationTypeAndValue);

        Builder scheduler(ScheduledExecutorService scheduler);

        Builder delegateToBatch(boolean delegateToBatch);

        Builder mixinStacktrace(boolean mixinStacktrace);

        Builder executeTimeout(Duration executeTimeout);

        Builder closeTimeout(Duration closeTimeout);

        SessionConfig build();
    }
}

