package io.deephaven.server.console;

import dagger.Module;
import dagger.Provides;
import io.deephaven.auth.AuthContext;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.util.ScriptSession;

@Module
public class SessionToExecutionStateModule {
    @Provides
    ExecutionContext bindExecutionContext(ScriptSession session) {
        // TODO: users should be able to provide an auth context that is used for start up scripts, etc
        return session.getExecutionContext().withAuthContext(new AuthContext.SuperUser());
    }
}
