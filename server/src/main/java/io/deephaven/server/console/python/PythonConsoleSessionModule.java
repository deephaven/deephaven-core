package io.deephaven.server.console.python;

import dagger.Module;
import dagger.Provides;
import dagger.multibindings.IntoMap;
import dagger.multibindings.StringKey;
import io.deephaven.engine.util.PythonDeephavenSession;
import io.deephaven.engine.util.ScriptSession;
import io.deephaven.plugin.type.ObjectTypeLookup;

import java.io.IOException;
import java.io.UncheckedIOException;

@Module
public class PythonConsoleSessionModule {
    @Provides
    @IntoMap
    @StringKey("python")
    ScriptSession bindScriptSession(PythonDeephavenSession pythonSession) {
        return pythonSession;
    }

    @Provides
    PythonDeephavenSession bindPythonSession(ObjectTypeLookup lookup, final ScriptSession.Listener listener) {
        try {
            return new PythonDeephavenSession(lookup, listener, true, true);
        } catch (IOException e) {
            // can't happen since we pass false
            throw new UncheckedIOException(e);
        }
    }
}
