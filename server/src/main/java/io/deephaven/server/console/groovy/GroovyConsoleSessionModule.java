//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.console.groovy;

import dagger.Module;
import dagger.Provides;
import dagger.multibindings.IntoMap;
import dagger.multibindings.StringKey;
import io.deephaven.engine.updategraph.OperationInitializer;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph;
import io.deephaven.engine.util.GroovyDeephavenSession;
import io.deephaven.engine.util.GroovyDeephavenSession.RunScripts;
import io.deephaven.engine.util.ScriptSession;
import io.deephaven.plugin.type.ObjectTypeLookup;
import io.deephaven.server.console.ScriptSessionCacheInit;

import javax.inject.Named;
import java.io.IOException;
import java.io.UncheckedIOException;

@Module(includes = InitScriptsModule.ServiceLoader.class)
public class GroovyConsoleSessionModule {
    @Provides
    @IntoMap
    @StringKey("groovy")
    ScriptSession bindScriptSession(final GroovyDeephavenSession groovySession) {
        return groovySession;
    }

    @Provides
    GroovyDeephavenSession bindGroovySession(
            @Named(PeriodicUpdateGraph.DEFAULT_UPDATE_GRAPH_NAME) final UpdateGraph updateGraph,
            final OperationInitializer operationInitializer,
            final ObjectTypeLookup lookup,
            final ScriptSession.Listener listener,
            final RunScripts runScripts,
            final ScriptSessionCacheInit ignored) {
        try {
            return GroovyDeephavenSession.of(updateGraph, operationInitializer, lookup, listener, runScripts);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
