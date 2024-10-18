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
public abstract class IcebergOverwrite {
    /**
     * The identifier string for the Iceberg table to overwrite
     */
    public abstract String tableIdentifier();

    /**
     * The Deephaven tables to overwrite with. All tables should have the same definition, else a table definition
     * should be provided in the {@link #instructions()}. An empty list will overwrite with an empty table.
     */
    public abstract List<Table> dhTables();

    /**
     * The instructions for customizations while writing, defaults to {@link IcebergParquetWriteInstructions#DEFAULT}.
     */
    @Value.Default
    public IcebergWriteInstructions instructions() {
        return IcebergParquetWriteInstructions.DEFAULT;
    }

    public static Builder builder() {
        return ImmutableIcebergOverwrite.builder();
    }

    public interface Builder {
        Builder tableIdentifier(String tableIdentifier);

        Builder addDhTables(Table element);

        Builder addDhTables(Table... elements);

        Builder addAllDhTables(Iterable<? extends Table> elements);

        Builder instructions(IcebergWriteInstructions instructions);

        IcebergOverwrite build();
    }
}
