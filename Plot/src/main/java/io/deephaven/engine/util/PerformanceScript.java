package io.deephaven.engine.util;

import com.google.auto.service.AutoService;
import io.deephaven.engine.util.GroovyDeephavenSession.InitScript;
import javax.inject.Inject;

@AutoService(InitScript.class)
public class PerformanceScript implements InitScript {

    @Inject
    public PerformanceScript() {}

    @Override
    public String getScriptPath() {
        return "groovy/1-performance.groovy";
    }

    @Override
    public int priority() {
        return 1;
    }
}
