//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.jpy;

import org.jpy.CreateModule;
import org.junit.rules.ExternalResource;

import java.util.Objects;

public class PythonResource extends ExternalResource {

    public static PythonResource of(JpyConfig config) {
        return new PythonResource(new JpyConfigExt(config));
    }

    public static PythonResource ofSysProps() {
        return of(JpyConfigSource.sysProps().asJpyConfig());
    }

    private final JpyConfigExt config;
    private boolean initialized;
    private CreateModule createModule; // common / useful tool for a lot of tests

    public PythonResource(JpyConfigExt config) {
        this.config = Objects.requireNonNull(config, "config");
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
            config.startPython();
            createModule = CreateModule.create();
        }
    }

    public CreateModule getCreateModule() {
        return createModule;
    }
}
