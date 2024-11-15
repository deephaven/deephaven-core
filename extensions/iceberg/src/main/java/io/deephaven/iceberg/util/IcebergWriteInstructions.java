//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
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
    public abstract List<Table> tables();

    /**
     * The partition paths where each table will be written. For example, if the table is partitioned by "year" and
     * "month", the partition path could be "year=2021/month=01".
     * <p>
     * Users must provide partition path for each table in {@link #tables()} in the same order if writing to a
     * partitioned table. For writing to a non-partitioned tables, this should be an empty list.
     */
    public abstract List<String> partitionPaths();

    /**
     * Returns {@link #tableDefinition()} if present, else the definition of the first table in {@link #tables()}.
     */
    final TableDefinition tableDefinitionOrFirst() {
        return tableDefinition().orElse(tables().get(0).getDefinition());
    }

    // @formatter:off
    interface Builder<INSTRUCTIONS_BUILDER extends IcebergWriteInstructions.Builder<INSTRUCTIONS_BUILDER>>
                extends IcebergBaseInstructions.Builder<INSTRUCTIONS_BUILDER> {
    // @formatter:on
        INSTRUCTIONS_BUILDER addTables(Table element);

        INSTRUCTIONS_BUILDER addTables(Table... elements);

        INSTRUCTIONS_BUILDER addAllTables(Iterable<? extends Table> elements);

        INSTRUCTIONS_BUILDER addPartitionPaths(String element);

        INSTRUCTIONS_BUILDER addPartitionPaths(String... elements);

        INSTRUCTIONS_BUILDER addAllPartitionPaths(Iterable<String> elements);
    }


    @Value.Check
    final void validateTables() {
        countCheckTables();
        verifySameDefinition();
    }

    final void countCheckTables() {
        if (tables().isEmpty()) {
            throw new IllegalArgumentException("At least one table must be provided");
        }
    }

    final void verifySameDefinition() {
        if (tableDefinition().isEmpty()) {
            // Verify that all tables have the same definition
            final List<Table> tables = tables();
            final int numTables = tables.size();
            final TableDefinition firstDefinition = tables.get(0).getDefinition();
            for (int idx = 1; idx < numTables; idx++) {
                if (!firstDefinition.equals(tables.get(idx).getDefinition())) {
                    throw new IllegalArgumentException(
                            "All Deephaven tables must have the same definition, else table definition should be " +
                                    "provided when writing multiple tables with different definitions");
                }
            }
        }
    }

    @Value.Check
    final void countCheckPartitionPaths() {
        if (!partitionPaths().isEmpty() && partitionPaths().size() != tables().size()) {
            throw new IllegalArgumentException("Partition path must be provided for each table");
        }
    }
}
