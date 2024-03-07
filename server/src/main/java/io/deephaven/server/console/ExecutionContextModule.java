//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.console;

import dagger.Module;
import dagger.Provides;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.util.ScriptSession;
import io.deephaven.server.auth.AuthorizationProvider;

@Module
public interface ExecutionContextModule {
    @Provides
    static ExecutionContext bindExecutionContext(ScriptSession session, AuthorizationProvider authProvider) {
        return session.getExecutionContext().withAuthContext(authProvider.getInstanceAuthContext());
    }
}
