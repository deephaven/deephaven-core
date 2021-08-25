package io.deephaven.client.impl;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.proto.backplane.grpc.SessionServiceGrpc.SessionServiceBlockingStub;
import io.deephaven.proto.backplane.grpc.SessionServiceGrpc.SessionServiceStub;
import io.deephaven.proto.backplane.grpc.TableServiceGrpc.TableServiceStub;
import io.deephaven.proto.backplane.script.grpc.ConsoleServiceGrpc.ConsoleServiceStub;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

@Immutable
@BuildableStyle
public abstract class SessionImplConfig {

    private static final boolean DELEGATE_TO_BATCH_DEFAULT =
            Boolean.getBoolean("deephaven.session.batch");

    private static final boolean MIXIN_STACKTRACE_DEFAULT =
            Boolean.getBoolean("deephaven.session.batch.stacktraces");

    public static Builder builder() {
        return ImmutableSessionImplConfig.builder();
    }

    public abstract ScheduledExecutorService executor();

    public abstract SessionServiceStub sessionService();

    public abstract TableServiceStub tableService();

    public abstract ConsoleServiceStub consoleService();

    /**
     * Whether the {@link Session} implementation will implement a batch {@link TableHandleManager}. By default, is
     * {@code false}. The default can be overridden via the system property {@code deephaven.session.batch}.
     *
     * @return true if the session will implement a batch manager, false if the session will implement a serial manager
     */
    @Default
    public boolean delegateToBatch() {
        return DELEGATE_TO_BATCH_DEFAULT;
    }

    /**
     * Whether the default batch {@link TableHandleManager} will use mix-in more relevant stacktraces. By default, is
     * {@code false}. The default can be overridden via the system property {@code deephaven.session.batch.stacktraces}.
     *
     * @return true if the default batch manager will mix-in stacktraces, false otherwise
     */
    @Default
    public boolean mixinStacktrace() {
        return MIXIN_STACKTRACE_DEFAULT;
    }

    public final SessionImpl createSession(SessionServiceBlockingStub stubBlocking) {
        return SessionImpl.create(this, stubBlocking);
    }

    public final CompletableFuture<SessionImpl> createSessionFuture() {
        return SessionImpl.createFuture(this);
    }

    public interface Builder {

        Builder executor(ScheduledExecutorService executor);

        Builder sessionService(SessionServiceStub sessionService);

        Builder tableService(TableServiceStub tableService);

        Builder consoleService(ConsoleServiceStub consoleService);

        Builder delegateToBatch(boolean delegateToBatch);

        Builder mixinStacktrace(boolean mixinStacktrace);

        SessionImplConfig build();
    }
}
