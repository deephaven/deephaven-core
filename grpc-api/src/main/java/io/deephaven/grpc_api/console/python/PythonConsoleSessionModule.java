package io.deephaven.grpc_api.console.python;

import dagger.Module;
import dagger.Provides;
import dagger.multibindings.IntoMap;
import dagger.multibindings.StringKey;
import io.deephaven.db.util.PythonDeephavenSession;
import io.deephaven.db.util.ScriptSession;

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
    PythonDeephavenSession bindPythonSession() {
        try {
            return new PythonDeephavenSession(true, true);
        } catch (IOException e) {
            // can't happen since we pass false
            throw new UncheckedIOException(e);
        }
    }
}
