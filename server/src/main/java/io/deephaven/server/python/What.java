package io.deephaven.server.python;

import org.jpy.PyModule;
import org.jpy.PyObject;

public class What {
    public static void markServerReady() {
        // noinspection EmptyTryBlock,unused
        try (
                final PyModule deephavenJpyModule = PyModule.importModule("deephaven_internal");
                final PyObject obj = deephavenJpyModule.callMethod("server_ready")) {
            // empty
        }
    }
}
