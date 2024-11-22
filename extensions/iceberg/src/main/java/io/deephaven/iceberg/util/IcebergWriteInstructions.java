//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.engine.table.Table;
import org.immutables.value.Value;
import org.immutables.value.Value.Immutable;

import java.util.List;

/**
 * This class provides instructions intended for writing deephaven tables as partitions to Iceberg tables.
 */
@Immutable
@BuildableStyle
public abstract class IcebergWriteInstructions {

    public static Builder builder() {
        return ImmutableIcebergWriteInstructions.builder();
    }

    /**
     * The Deephaven tables to be written.
     * <p>
     * All tables must have the same table definition as definition for non-partitioning columns specified in the
     * {@link IcebergTableWriter}. For example, if an iceberg table is partitioned by "year" and "month" and has a
     * non-partitioning column "data," then {@link IcebergTableWriter} should be configured with a definition that
     * includes all three columns: "year," "month," and "data." But, the tables provided here should only include the
     * non-partitioning column, such as "data."
     */
    public abstract List<Table> tables();

    /**
     * The partition paths where each table will be written. For example, if the table is partitioned by "year" and
     * "month", the partition path could be "year=2021/month=01".
     * <p>
     * If writing to a partitioned iceberg table, users must provide partition path for each table in {@link #tables()}
     * in the same order. Else, this should be an empty list.
     */
    public abstract List<String> partitionPaths();

    // @formatter:off
    public interface Builder {
    // @formatter:on
        Builder addTables(Table element);

        Builder addTables(Table... elements);

        Builder addAllTables(Iterable<? extends Table> elements);

        Builder addPartitionPaths(String element);

        Builder addPartitionPaths(String... elements);

        Builder addAllPartitionPaths(Iterable<String> elements);

        IcebergWriteInstructions build();
    }

    @Value.Check
    final void countCheckTables() {
        if (tables().isEmpty()) {
            throw new IllegalArgumentException("At least one table must be provided");
        }
    }

    @Value.Check
    final void countCheckPartitionPaths() {
        if (!partitionPaths().isEmpty() && partitionPaths().size() != tables().size()) {
            throw new IllegalArgumentException("Partition path must be provided for each table, partitionPaths.size()="
                    + partitionPaths().size() + ", tables.size()=" + tables().size());
        }
    }
}
