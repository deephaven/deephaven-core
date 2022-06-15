/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.console.python;

import dagger.Module;
import dagger.Provides;
import io.deephaven.engine.util.PythonEvaluatorJpy;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

@Module
public interface PythonGlobalScopeCopyModule {

    @Provides
    static PythonEvaluatorJpy providePythonEvaluatorJpy() {
        try {
            return PythonEvaluatorJpy.withGlobalCopy();
        } catch (IOException | InterruptedException | TimeoutException e) {
            throw new IllegalStateException("Unable to start a python session: ", e);
        }
    }
}
