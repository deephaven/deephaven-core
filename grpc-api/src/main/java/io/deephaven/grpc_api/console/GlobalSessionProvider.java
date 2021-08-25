package io.deephaven.grpc_api.console;

import io.deephaven.db.util.ScriptSession;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class GlobalSessionProvider {
    private volatile ScriptSession globalSession;

    @Inject
    public GlobalSessionProvider() {}

    public ScriptSession getGlobalSession() {
        return globalSession;
    }

    public synchronized void initializeGlobalScriptSession(final ScriptSession globalSession) {
        if (this.globalSession != null) {
            throw new IllegalStateException("global session already initialized");
        }
        this.globalSession = globalSession;
    }
}
