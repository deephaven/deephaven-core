//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.engine.table.Table;
import org.apache.iceberg.Schema;
import org.immutables.value.Value;

import java.util.List;

/**
 * This class provides instructions intended for writing Iceberg tables. The default values documented in this class may
 * change in the future. As such, callers may wish to explicitly set the values.
 */
public abstract class IcebergWriteInstructions implements IcebergBaseInstructions {
    /**
     * The Deephaven tables to be written. All tables should have the same definition, else a {@link #tableDefinition()
     * table definition} should be provided.
     */
    public abstract List<Table> dhTables();

    /**
     * The partition paths where each table will be written. For example, if the table is partitioned by "year" and
     * "month", the partition path could be "year=2021/month=01".
     * <p>
     * Users must provide partition path for each table in {@link #dhTables()} in the same order if writing to a
     * partitioned table. For writing to a non-partitioned tables, this should be an empty list.
     */
    public abstract List<String> partitionPaths();

    /**
     * Whether to update the schema of the iceberg table if it does not match the schema derived from the Deephaven
     * table. To generate the final schema, we take a union of the existing and the new schema with the rules specified
     * in {@link org.apache.iceberg.UpdateSchema#unionByNameWith(Schema)}.
     * <p>
     * Disabled by default. When disabled, the schema will not be updated and the write will fail if the schema does not
     * match.
     * <p>
     * Note that we do not support updating the partition spec.
     */
    @Value.Default
    public boolean updateSchema() {
        return false;
    }

    // @formatter:off
    interface Builder<INSTRUCTIONS_BUILDER extends IcebergWriteInstructions.Builder<INSTRUCTIONS_BUILDER>>
                extends IcebergBaseInstructions.Builder<INSTRUCTIONS_BUILDER> {
    // @formatter:on
        INSTRUCTIONS_BUILDER addDhTables(Table element);

        INSTRUCTIONS_BUILDER addDhTables(Table... elements);

        INSTRUCTIONS_BUILDER addAllDhTables(Iterable<? extends Table> elements);

        // TODO Discuss about the API for partition paths, and add tests
        INSTRUCTIONS_BUILDER addPartitionPaths(String element);

        INSTRUCTIONS_BUILDER addPartitionPaths(String... elements);

        INSTRUCTIONS_BUILDER addAllPartitionPaths(Iterable<String> elements);

        INSTRUCTIONS_BUILDER updateSchema(boolean updateSchema);
    }

    @Value.Check
    final void countCheckPartitionPaths() {
        if (!partitionPaths().isEmpty() && partitionPaths().size() != dhTables().size()) {
            throw new IllegalArgumentException("Partition path must be provided for each table");
        }
    }
}
