package io.deephaven.server.console.python;

import dagger.Module;
import dagger.Provides;
import io.deephaven.engine.util.PythonEvaluatorJpy;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

@Module
public class PythonGlobalScopeCopyModule {

    @Provides
    PythonEvaluatorJpy providePythonEvaluatorJpy() {
        try {
            return PythonEvaluatorJpy.withGlobalCopy();
        } catch (IOException | InterruptedException | TimeoutException e) {
            throw new IllegalStateException("Unable to start a python session: ", e);
        }
    }
}
