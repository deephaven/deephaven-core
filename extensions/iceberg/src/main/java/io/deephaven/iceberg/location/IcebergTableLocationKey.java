//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.location;

import io.deephaven.engine.table.impl.locations.ImmutableTableLocationKey;
import io.deephaven.engine.table.impl.locations.TableLocationKey;

/**
 * {@link TableLocationKey} implementation for use with data stored in Iceberg tables.
 */
public interface IcebergTableLocationKey extends ImmutableTableLocationKey {
    /**
     * Get the read instructions for the location.
     *
     * @return the read instructions
     */
    Object readInstructions();
}
