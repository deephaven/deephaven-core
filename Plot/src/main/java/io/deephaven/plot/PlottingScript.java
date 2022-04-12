package io.deephaven.plot;

import com.google.auto.service.AutoService;
import io.deephaven.engine.util.GroovyDeephavenSession.InitScript;
import javax.inject.Inject;

@AutoService(InitScript.class)
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
