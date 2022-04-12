package io.deephaven.server.console;

import io.deephaven.engine.util.ScriptSession;

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
