package io.deephaven.server.console.python;

import dagger.Module;
import dagger.Provides;
import io.deephaven.engine.util.PythonEvaluatorJpy;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

@Module
public class PythonGlobalScopeModule {

    @Provides
    PythonEvaluatorJpy providePythonEvaluatorJpy() {
        try {
            return PythonEvaluatorJpy.withGlobals();
        } catch (IOException | InterruptedException | TimeoutException e) {
            throw new IllegalStateException("Unable to start a python session: ", e);
        }
    }
}
