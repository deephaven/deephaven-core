//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.engine.table.impl.UpdateErrorReporter;
import io.deephaven.util.datastructures.WeakIdentityHashSet;

import java.io.IOException;

/**
 * When we get an error from a table in the listener tree, we want to send an appropriate command to the clients
 * indicating that something has gone wrong with the table.
 */
public class AsyncClientErrorNotifier {

    private static volatile UpdateErrorReporter reporter = null;
    private static final WeakIdentityHashSet<Throwable> knownErrors = new WeakIdentityHashSet.Synchronized<>();

    public static UpdateErrorReporter setReporter(UpdateErrorReporter reporter) {
        final UpdateErrorReporter old = AsyncClientErrorNotifier.reporter;
        AsyncClientErrorNotifier.reporter = reporter;
        return old;
    }

    public static void reportError(Throwable t) throws IOException {
        final UpdateErrorReporter localReporter = reporter;
        if (localReporter != null && knownErrors.add(t)) {
            localReporter.reportUpdateError(t);
        }
    }
}
