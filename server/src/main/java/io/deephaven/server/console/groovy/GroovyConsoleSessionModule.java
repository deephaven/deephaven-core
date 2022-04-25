package io.deephaven.server.console.groovy;

import dagger.Module;
import dagger.Provides;
import dagger.multibindings.IntoMap;
import dagger.multibindings.StringKey;
import io.deephaven.engine.util.GroovyDeephavenSession;
import io.deephaven.engine.util.GroovyDeephavenSession.RunScripts;
import io.deephaven.engine.util.ScriptSession;
import io.deephaven.plugin.type.ObjectTypeLookup;

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
    GroovyDeephavenSession bindGroovySession(ObjectTypeLookup lookup, final ScriptSession.Listener listener,
            final RunScripts runScripts) {
        try {
            return new GroovyDeephavenSession(lookup, listener, runScripts, true);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
