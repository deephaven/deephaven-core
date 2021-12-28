package io.deephaven.server.console.groovy;

import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import dagger.multibindings.IntoSet;
import io.deephaven.engine.util.GroovyDeephavenSession.Base;
import io.deephaven.engine.util.GroovyDeephavenSession.CountMetrics;
import io.deephaven.engine.util.GroovyDeephavenSession.InitScript;
import io.deephaven.engine.util.GroovyDeephavenSession.PerformanceQueries;
import io.deephaven.engine.util.GroovyDeephavenSession.RunScripts;

import java.util.Set;

public class InitScriptsModule {

    @Module
    public interface Explicit {
        @Binds
        @IntoSet
        InitScript bindsDbScripts(Base impl);

        @Binds
        @IntoSet
        InitScript bindsCountMetricsScripts(CountMetrics impl);

        @Binds
        @IntoSet
        InitScript bindsPerformanceQueriesScripts(PerformanceQueries impl);

        @Provides
        static RunScripts providesRunScriptLogic(Set<InitScript> scripts) {
            return RunScripts.of(scripts);
        }
    }

    @Module
    public interface ServiceLoader {

        @Provides
        static RunScripts providesRunScriptLogic() {
            return RunScripts.serviceLoader();
        }
    }

    @Module
    public interface OldConfig {

        @Provides
        static RunScripts providesRunScriptLogic() {
            return RunScripts.oldConfiguration();
        }
    }
}
