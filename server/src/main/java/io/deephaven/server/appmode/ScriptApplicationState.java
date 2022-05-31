package io.deephaven.server.appmode;

import io.deephaven.appmode.ApplicationState;
import io.deephaven.engine.util.ScriptSession;

public class ScriptApplicationState extends ApplicationState {

    private final ScriptSession scriptSession;

    public ScriptApplicationState(final ScriptSession scriptSession,
            final Listener listener,
            final String id,
            final String name) {
        super(listener, id, name);
        this.scriptSession = scriptSession;
    }

    @Override
    public synchronized <T> void setField(String name, T value, String description) {
        super.setField(name, scriptSession.unwrapObject(value), description);
    }

    @Override
    public synchronized <T> void setField(String name, T value) {
        super.setField(name, scriptSession.unwrapObject(value));
    }
}
