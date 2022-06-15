/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.locations.local;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.engine.table.impl.locations.TableDataException;
import org.jetbrains.annotations.NotNull;

import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

public class PrivilegedFileAccessUtil {

    /**
     * Wrap a filesystem operation (or series of them) as a privileged action.
     *
     * @param operation The operation to wrap
     */
    public static void doFilesystemAction(@NotNull final Runnable operation) {
        try {
            AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                operation.run();
                return null;
            });
        } catch (final PrivilegedActionException pae) {
            if (pae.getException() instanceof TableDataException) {
                throw (TableDataException) pae.getException();
            } else {
                throw new UncheckedDeephavenException(pae.getException());
            }
        }
    }
}
