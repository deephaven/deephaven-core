//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.io.FileIO;

/**
 * Tools for accessing tables in the Iceberg table format.
 */
public abstract class IcebergTools {
    @SuppressWarnings("unused")
    public static IcebergCatalogAdapter createAdapter(
            final Catalog catalog,
            final FileIO fileIO) {
        return new IcebergCatalogAdapter(catalog, fileIO);
    }
}
