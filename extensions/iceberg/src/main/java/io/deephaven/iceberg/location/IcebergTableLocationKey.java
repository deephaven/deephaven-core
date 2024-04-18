//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.location;

import io.deephaven.engine.table.impl.locations.TableLocationKey;
import io.deephaven.parquet.table.location.ParquetTableLocationKey;

/**
 * {@link TableLocationKey} implementation for use with data stored in Iceberg tables.
 */
public interface IcebergTableLocationKey extends TableLocationKey {
    /**
     * Get the read instructions for the location.
     *
     * @return the read instructions
     */
    Object readInstructions();

    /**
     * Verify that a reader for the file can be created successfully. Delegates to
     * {@link ParquetTableLocationKey#verifyFileReader()} for parquet files.
     */
    boolean verifyFileReader();
}
