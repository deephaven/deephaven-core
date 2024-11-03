//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.engine.table.Table;
import io.deephaven.util.annotations.InternalUseOnly;
import org.immutables.value.Value;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * This class provides instructions intended for writing Iceberg tables. The default values documented in this class may
 * change in the future. As such, callers may wish to explicitly set the values.
 */
public abstract class IcebergWriteInstructions implements IcebergBaseInstructions {
    // @formatter:off
    /**
     * Specifies whether to verify that the partition spec and schema of the table being written are consistent with the
     * Iceberg table.
     *
     * <p>Verification behavior differs based on the operation type:</p>
     * <ul>
     *   <li><strong>Appending Data or Writing Data Files:</strong> Verification is enabled by default. It ensures that:
     *     <ul>
     *       <li>All columns from the Deephaven table are present in the Iceberg table and have compatible types.</li>
     *       <li>All required columns in the Iceberg table are present in the Deephaven table.</li>
     *       <li>The set of partitioning columns in both the Iceberg and Deephaven tables are identical.</li>
     *     </ul>
     *   </li>
     *   <li><strong>Overwriting Data:</strong> Verification is disabled by default. When enabled, it ensures that the
     *   schema and partition spec of the table being written are identical to those of the Iceberg table.</li>
     * </ul>
     */
    public abstract Optional<Boolean> verifySchema();
    // @formatter:on

    /**
     * A one-to-one {@link Map map} from Deephaven to Iceberg column names to use when writing deephaven tables to
     * Iceberg tables.
     */
    // TODO Please suggest better name for this method, on the read side its just called columnRenames
    public abstract Map<String, String> dhToIcebergColumnRenames();

    /**
     * The inverse map of {@link #dhToIcebergColumnRenames()}.
     */
    @InternalUseOnly
    @Value.Lazy
    public Map<String, String> icebergToDhColumnRenames() {
        return dhToIcebergColumnRenames().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
    }

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

    // @formatter:off
    interface Builder<INSTRUCTIONS extends IcebergWriteInstructions, INSTRUCTIONS_BUILDER
            extends IcebergWriteInstructions.Builder<INSTRUCTIONS, INSTRUCTIONS_BUILDER>>
                extends IcebergBaseInstructions.Builder<INSTRUCTIONS, INSTRUCTIONS_BUILDER> {
    // @formatter:on
        INSTRUCTIONS_BUILDER verifySchema(boolean verifySchema);

        INSTRUCTIONS_BUILDER putDhToIcebergColumnRenames(String key, String value);

        INSTRUCTIONS_BUILDER putAllDhToIcebergColumnRenames(Map<String, ? extends String> entries);

        INSTRUCTIONS_BUILDER addDhTables(Table element);

        INSTRUCTIONS_BUILDER addDhTables(Table... elements);

        INSTRUCTIONS_BUILDER addAllDhTables(Iterable<? extends Table> elements);

        // TODO Discuss about the API for partition paths, and add tests
        INSTRUCTIONS_BUILDER addPartitionPaths(String element);

        INSTRUCTIONS_BUILDER addPartitionPaths(String... elements);

        INSTRUCTIONS_BUILDER addAllPartitionPaths(Iterable<String> elements);
    }

    @Value.Check
    final void checkColumnRenamesUnique() {
        if (dhToIcebergColumnRenames().size() != dhToIcebergColumnRenames().values().stream().distinct().count()) {
            throw new IllegalArgumentException("Duplicate values in column renames, values must be unique");
        }
    }

    @Value.Check
    final void countCheckPartitionPaths() {
        if (!partitionPaths().isEmpty() && partitionPaths().size() != dhTables().size()) {
            throw new IllegalArgumentException("Partition path must be provided for each table");
        }
    }
}
