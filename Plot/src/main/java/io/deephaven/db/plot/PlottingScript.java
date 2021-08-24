package io.deephaven.db.plot;

import io.deephaven.db.util.GroovyDeephavenSession.InitScript;
import javax.inject.Inject;

public class PlottingScript implements InitScript {

    @Inject
    public PlottingScript() {}

    @Override
    public String getScriptPath() {
        return "groovy/3-plotting.groovy";
    }

    @Override
    public int priority() {
        return 3;
    }
}
