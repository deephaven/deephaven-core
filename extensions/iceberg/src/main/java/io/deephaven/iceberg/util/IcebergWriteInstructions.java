//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.immutables.value.Value.Default;

/**
 * This class provides instructions intended for writing Iceberg tables. The default values documented in this class may
 * change in the future. As such, callers may wish to explicitly set the values.
 */
public abstract class IcebergWriteInstructions implements IcebergBaseInstructions {
    /**
     * While writing to an iceberg table, whether to create the iceberg table if it does not exist, defaults to
     * {@code false}.
     */
    @Default
    public boolean createTableIfNotExist() {
        return false;
    }

    /**
     * While writing to an iceberg table, whether to verify that the partition spec and schema of the table being
     * written is consistent with the iceberg table; defaults to {@code false}.
     */
    @Default
    public boolean verifySchema() {
        return false;
    }

    public interface Builder<INSTRUCTIONS_BUILDER> extends IcebergBaseInstructions.Builder<INSTRUCTIONS_BUILDER> {
        @SuppressWarnings("unused")
        INSTRUCTIONS_BUILDER createTableIfNotExist(boolean createTableIfNotExist);

        @SuppressWarnings("unused")
        INSTRUCTIONS_BUILDER verifySchema(boolean verifySchema);
    }
}
