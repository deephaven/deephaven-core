package io.deephaven.client.impl;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.grpc_api.DeephavenChannel;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

@Immutable
@BuildableStyle
public abstract class SessionImplConfig {

    public static Builder builder() {
        return ImmutableSessionImplConfig.builder();
    }

    public abstract ScheduledExecutorService executor();

    public abstract DeephavenChannel channel();

    /**
     * Whether the {@link Session} implementation will implement a batch {@link TableHandleManager}. By default, is
     * {@code false}. The default can be overridden via the system property {@code deephaven.session.batch}.
     *
     * @return true if the session will implement a batch manager, false if the session will implement a serial manager
     */
    @Default
    public boolean delegateToBatch() {
        return Boolean.getBoolean("deephaven.session.batch");
    }

    /**
     * Whether the default batch {@link TableHandleManager} will use mix-in more relevant stacktraces. By default, is
     * {@code false}. The default can be overridden via the system property {@code deephaven.session.batch.stacktraces}.
     *
     * @return true if the default batch manager will mix-in stacktraces, false otherwise
     */
    @Default
    public boolean mixinStacktrace() {
        return Boolean.getBoolean("deephaven.session.batch.stacktraces");
    }

    /**
     * The session execute timeout. By default, is {@code PT1m}. The default can be overridden via the system property
     * {@code deephaven.session.executeTimeout}.
     *
     * @return the session execute timeout
     */
    @Default
    public Duration executeTimeout() {
        return Duration.parse(System.getProperty("deephaven.session.executeTimeout", "PT1m"));
    }

    /**
     * The {@link Session} and {@link ConsoleSession} close timeout. By default, is {@code PT5s}. The default can be
     * overridden via the system property {@code deephaven.session.closeTimeout}.
     *
     * @return the close timeout
     */
    @Default
    public Duration closeTimeout() {
        return Duration.parse(System.getProperty("deephaven.session.closeTimeout", "PT5s"));
    }

    public final SessionImpl createSession() {
        return SessionImpl.create(this);
    }

    public final CompletableFuture<SessionImpl> createSessionFuture() {
        return SessionImpl.createFuture(this);
    }

    public interface Builder {

        Builder executor(ScheduledExecutorService executor);

        Builder channel(DeephavenChannel channel);

        Builder delegateToBatch(boolean delegateToBatch);

        Builder mixinStacktrace(boolean mixinStacktrace);

        Builder closeTimeout(Duration closeTimeout);

        SessionImplConfig build();
    }
}
