package io.deephaven.grpc_api.appmode;

import io.deephaven.appmode.ApplicationState;
import io.deephaven.db.util.ScriptSession;

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
    public synchronized <T> void setCustomField(String type, String name, T value) {
        super.setCustomField(type, name, scriptSession.unwrapObject(value));
    }

    @Override
    public synchronized <T> void setCustomField(String type, String name, T value, String description) {
        super.setCustomField(type, name, scriptSession.unwrapObject(value), description);
    }

    @Override
    public synchronized <T> void setField(String name, T value) {
        super.setField(name, scriptSession.unwrapObject(value));
    }
}
