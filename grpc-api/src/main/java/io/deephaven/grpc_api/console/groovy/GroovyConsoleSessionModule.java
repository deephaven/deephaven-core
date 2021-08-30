package io.deephaven.grpc_api.console.groovy;

import dagger.Module;
import dagger.Provides;
import dagger.multibindings.IntoMap;
import dagger.multibindings.StringKey;
import io.deephaven.db.util.GroovyDeephavenSession;
import io.deephaven.db.util.GroovyDeephavenSession.RunScripts;
import io.deephaven.db.util.ScriptSession;

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
    GroovyDeephavenSession bindGroovySession(final ScriptSession.Listener listener, final RunScripts runScripts) {
        try {
            return new GroovyDeephavenSession(listener, runScripts, true);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
