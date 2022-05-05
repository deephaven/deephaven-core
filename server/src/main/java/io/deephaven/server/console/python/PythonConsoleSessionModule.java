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
import java.util.concurrent.TimeoutException;

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
            throw new UncheckedIOException("Unable to run python startup scripts", e);
        } catch (InterruptedException | TimeoutException e) {
            throw new IllegalStateException("Unable to start a python session: ", e);
        }
    }
}
