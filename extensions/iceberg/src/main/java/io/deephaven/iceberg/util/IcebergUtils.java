//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.apache.iceberg.catalog.TableIdentifier;
import org.jetbrains.annotations.NotNull;

public class IcebergUtils {

    /**
     * Implementation of the {@link Comparable#compareTo} interface for {@link TableIdentifier} instances. Note that
     * this method assumes that namespace and name are not {@code null}, and that there are just two fields in the
     * {@link TableIdentifier} class.
     *
     * @return a negative integer, zero, or a positive integer as the first argument is less than, equal to, or greater
     *         than the second.
     */
    public static int compareTo(@NotNull final TableIdentifier ti1, @NotNull final TableIdentifier ti2) {
        int result = ti1.namespace().toString().compareTo(ti2.namespace().toString());
        if (result == 0) {
            result = ti1.name().compareTo(ti2.name());
        }
        return result;
    }
}
