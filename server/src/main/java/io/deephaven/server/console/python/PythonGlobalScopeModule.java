//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.console.python;

import dagger.Module;
import dagger.Provides;
import io.deephaven.engine.util.PythonEvaluatorJpy;
import io.deephaven.server.log.LogInit;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

@Module
public interface PythonGlobalScopeModule {

    @Provides
    static PythonEvaluatorJpy providePythonEvaluatorJpy(LogInit ignoredLogInit) {
        // Before we can initialize python and set up our logging redirect from sys.out/err, we ensure that Java's
        // System fields are reassigned.
        try {
            PythonEvaluatorJpy jpy = PythonEvaluatorJpy.withGlobals();
            PythonImportInitializer.init();
            return jpy;
        } catch (IOException | InterruptedException | TimeoutException e) {
            throw new IllegalStateException("Unable to start a python session: ", e);
        }
    }
}
