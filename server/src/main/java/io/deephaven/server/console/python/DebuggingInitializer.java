//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.console.python;

import io.deephaven.configuration.Configuration;
import io.deephaven.engine.util.PythonEvaluatorJpy;
import io.deephaven.util.thread.ThreadInitializationFactory;
import org.jpy.PyLib;
import org.jpy.PyModule;
import org.jpy.PyObject;

import javax.inject.Inject;
import javax.inject.Provider;
import java.io.Closeable;

/**
 * If python is configured as the script language for this server, ensures that threads which may invoke python code
 * will be able to be debugged. If python is disabled, this does nothing.
 */
public class DebuggingInitializer implements ThreadInitializationFactory {
    private final Provider<PythonEvaluatorJpy> pythonProvider;

    @Inject
    public DebuggingInitializer(Provider<PythonEvaluatorJpy> pythonProvider) {
        this.pythonProvider = pythonProvider;
    }

    @Override
    public Runnable createInitializer(Runnable runnable) {
        if (!"python".equals(Configuration.getInstance().getStringWithDefault("deephaven.console.type", null))) {
            // python not enabled, don't accidentally start it
            return runnable;
        }

        // Ensure python has finished running before we create and return the runnable wrapper
        pythonProvider.get();

        return () -> {
            DeephavenModule py_deephaven = (DeephavenModule) PyModule.importModule("deephaven_internal.java_threads")
                    .createProxy(PyLib.CallableKind.FUNCTION, DeephavenModule.class);
            // First call in to create a custom function that has the same name as the Java thread (plus a prefix)
            PyObject runnableResult = py_deephaven.create_thread_entry(Thread.currentThread().getName());
            // runnable.run();
            // Invoke that function directly from Java, so that we have only this one initial frame
            runnableResult.call("__call__", runnable);
        };
    }

    interface DeephavenModule extends Closeable {
        /**
         * Creates a new function that will initialize a thread in python, including creating a simple frame
         * 
         * @param threadName the name of the java thread
         * @return a callable PyObject
         */
        PyObject create_thread_entry(String threadName);
    }

}
