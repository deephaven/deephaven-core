//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Comparator;

public class IcebergUtils {

    public static final class TableIdentifierComparator implements Comparator<TableIdentifier> {

        /**
         * Compare two {@link TableIdentifier} instances.
         * <p>
         * Note that this method assumes:
         * <ul>
         * <li>There are just two fields {@link TableIdentifier#namespace()} and {@link TableIdentifier#name()} in
         * {@link TableIdentifier} class, and both are not null for the objects being compared.</li>
         * <li>There is just a single field {@link Namespace#levels()} in the {@link Namespace} class.</li>
         * </ul>
         * <p>
         * {@inheritDoc}
         *
         * @param ti1 the first object to be compared.
         * @param ti2 the second object to be compared.
         * @return a negative integer, zero, or a positive integer as the first argument is less than, equal to, or
         *         greater than the second, respectively.
         */
        @Override
        public int compare(@NotNull final TableIdentifier ti1, @NotNull final TableIdentifier ti2) {
            int result = Arrays.compare(ti1.namespace().levels(), ti2.namespace().levels());
            if (result == 0) {
                result = ti1.name().compareTo(ti2.name());
            }
            return result;
        }
    }
}
