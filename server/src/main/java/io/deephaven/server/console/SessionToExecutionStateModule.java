package io.deephaven.server.console;

import dagger.Module;
import dagger.Provides;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.util.ScriptSession;

@Module
public class SessionToExecutionStateModule {
    @Provides
    ExecutionContext bindExecutionContext(ScriptSession session) {
        return session.getExecutionContext();
    }
}
