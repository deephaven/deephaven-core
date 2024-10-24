//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.engine.table.Table;
import org.immutables.value.Value;

import java.util.List;

@Value.Immutable
@BuildableStyle
public abstract class IcebergWriteDataFiles {
    /**
     * The Deephaven tables to be written. All tables should have the same definition, else a table definition should be
     * provided in the {@link #instructions()}.
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
     * The instructions for customizations while writing, defaults to {@link IcebergParquetWriteInstructions#DEFAULT}.
     */
    @Value.Default
    public IcebergWriteInstructions instructions() {
        return IcebergParquetWriteInstructions.DEFAULT;
    }

    public static Builder builder() {
        return ImmutableIcebergWriteDataFiles.builder();
    }

    public interface Builder {
        Builder addDhTables(Table element);

        Builder addDhTables(Table... elements);

        Builder addAllDhTables(Iterable<? extends Table> elements);

        // TODO think about the API for partition paths
        Builder addPartitionPaths(String element);

        Builder addPartitionPaths(String... elements);

        Builder addAllPartitionPaths(Iterable<String> elements);

        Builder instructions(IcebergWriteInstructions instructions);

        IcebergWriteDataFiles build();
    }

    @Value.Check
    final void countCheckPartitionPaths() {
        if (!partitionPaths().isEmpty() && partitionPaths().size() != dhTables().size()) {
            throw new IllegalArgumentException("Partition path must be provided for each table");
        }
    }
}
