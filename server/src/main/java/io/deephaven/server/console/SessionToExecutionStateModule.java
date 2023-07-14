package io.deephaven.server.console;

import dagger.Module;
import dagger.Provides;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.util.ScriptSession;
import io.deephaven.server.auth.AuthorizationProvider;

/**
 * Deprecated: use {@link ExecutionContextModule} instead.
 */
@Deprecated(since = "0.26.0", forRemoval = true)
@Module
public interface SessionToExecutionStateModule {
    @Provides
    static ExecutionContext bindExecutionContext(ScriptSession session, AuthorizationProvider authProvider) {
        return ExecutionContextModule.bindExecutionContext(session, authProvider);
    }
}
