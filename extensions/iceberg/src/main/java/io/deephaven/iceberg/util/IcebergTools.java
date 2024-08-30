//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Catalog;

/**
 * Tools for accessing tables in the Iceberg table format.
 */
public abstract class IcebergTools {
    @SuppressWarnings("unused")
    public static IcebergCatalogAdapter createAdapter(final Catalog catalog) {
        Configuration conf = new Configuration();
        return new IcebergCatalogAdapter(catalog);
    }
}
