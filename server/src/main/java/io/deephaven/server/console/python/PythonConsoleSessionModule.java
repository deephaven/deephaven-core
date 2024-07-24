//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.console.python;

import dagger.Module;
import dagger.Provides;
import dagger.multibindings.IntoMap;
import dagger.multibindings.StringKey;
import io.deephaven.engine.updategraph.OperationInitializer;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph;
import io.deephaven.engine.util.PythonEvaluatorJpy;
import io.deephaven.engine.util.ScriptSession;
import io.deephaven.integrations.python.PythonDeephavenSession;
import io.deephaven.plugin.type.ObjectTypeLookup;
import io.deephaven.server.console.ScriptSessionCacheInit;
import io.deephaven.util.thread.ThreadInitializationFactory;

import javax.inject.Named;
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
    PythonDeephavenSession bindPythonSession(
            @Named(PeriodicUpdateGraph.DEFAULT_UPDATE_GRAPH_NAME) final UpdateGraph updateGraph,
            final ThreadInitializationFactory threadInitializationFactory,
            final OperationInitializer operationInitializer,
            final ObjectTypeLookup lookup,
            final ScriptSession.Listener listener,
            final PythonEvaluatorJpy pythonEvaluator,
            final ScriptSessionCacheInit ignored) {
        try {
            return new PythonDeephavenSession(
                    updateGraph, operationInitializer, threadInitializationFactory, lookup, listener,
                    true, pythonEvaluator);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to run python startup scripts", e);
        }
    }
}
