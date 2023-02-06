/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.console.python;

import dagger.Module;
import dagger.Provides;
import dagger.multibindings.IntoMap;
import dagger.multibindings.StringKey;
import io.deephaven.engine.util.PythonEvaluatorJpy;
import io.deephaven.engine.util.ScriptSession;
import io.deephaven.integrations.python.PythonDeephavenSession;
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
    PythonDeephavenSession bindPythonSession(ObjectTypeLookup lookup, final ScriptSession.Listener listener,
            PythonEvaluatorJpy pythonEvaluator) {
        try {
            return new PythonDeephavenSession(lookup, listener, true, pythonEvaluator);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to run python startup scripts", e);
        }
    }
}
