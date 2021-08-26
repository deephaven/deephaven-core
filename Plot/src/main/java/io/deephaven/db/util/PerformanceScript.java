package io.deephaven.db.util;

import io.deephaven.db.util.GroovyDeephavenSession.InitScript;
import javax.inject.Inject;

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
