package io.deephaven.jpy;

import java.time.Duration;
import java.util.Objects;
import org.jpy.CreateModule;
import org.junit.rules.ExternalResource;

public class PythonResource extends ExternalResource {

    public static PythonResource of(JpyConfig config) {
        return new PythonResource(new JpyConfigExt(config), Duration.ofMillis(10));
    }

    public static PythonResource ofSysProps() {
        return of(JpyConfigSource.sysProps().asJpyConfig());
    }

    private final JpyConfigExt config;
    private final Duration cleanupTimeout;
    private boolean initialized;

    public PythonResource(JpyConfigExt config, Duration cleanupTimeout) {
        this.config = Objects.requireNonNull(config, "config");
        this.cleanupTimeout = cleanupTimeout;
        this.initialized = false;
    }

    @Override
    protected void before() {
        // Note: junit doesn't natively support the notion of a "run once per JVM", thus we need to
        // keep track of our own initialization status.
        // This is unnecessary if forkEvery = 1.
        if (!initialized) {
            initialized = true;
            config.initPython();
        }
    }

    public StartStopRule startStopRule() {
        return new StartStopRule();
    }

    public class StartStopRule extends ExternalResource {

        private CreateModule createModule; // common / useful tool for a lot of tests

        public CreateModule getCreateModule() {
            return createModule;
        }

        @Override
        protected void before() {
            config.startPython();
            createModule = CreateModule.create();
        }

        @Override
        protected void after() {
            createModule.close();
            config.stopPython(cleanupTimeout);
        }
    }
}
