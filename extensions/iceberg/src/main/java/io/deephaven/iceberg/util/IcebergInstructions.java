//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.extensions.s3.S3Instructions;
import io.deephaven.parquet.table.ParquetInstructions;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

import java.util.Map;
import java.util.Optional;

/**
 * This class provides instructions intended for reading Iceberg catalogs and tables. The default values documented in
 * this class may change in the future. As such, callers may wish to explicitly set the values.
 */
@Immutable
@BuildableStyle
public abstract class IcebergInstructions {
    public static Builder builder() {
        return ImmutableIcebergInstructions.builder();
    }

    /**
     * The {@link TableDefinition} to use when reading Iceberg data files.
     */
    public abstract Optional<TableDefinition> tableDefinition();

    /**
     * The {@link S3Instructions} to use for reading the Iceberg data files. This is mutually exclusive with
     * {@link #parquetInstructions()} which can be used to provide custom instructions for reading the data files.
     */
    public abstract Optional<S3Instructions> s3Instructions();

    /**
     * The {@link ParquetInstructions} to use for reading the Iceberg data files.
     */
    public abstract Optional<ParquetInstructions> parquetInstructions();

    /**
     * The {@link Map} to use for reading the Iceberg data files.
     */
    public abstract Map<String, String> columnRenameMap();

    public interface Builder {
        @SuppressWarnings("unused")
        Builder tableDefinition(TableDefinition tableDefinition);

        @SuppressWarnings("unused")
        Builder s3Instructions(S3Instructions s3Instructions);

        @SuppressWarnings("unused")
        Builder parquetInstructions(ParquetInstructions parquetInstructions);

        @SuppressWarnings("unused")
        Builder putColumnRenameMap(String key, String value);

        @SuppressWarnings("unused")
        Builder putAllColumnRenameMap(Map<String, ? extends String> entries);

        IcebergInstructions build();
    }

    @Check
    final void checkInstructions() {
        if (s3Instructions().isPresent() && parquetInstructions().isPresent()) {
            throw new IllegalArgumentException("Only one of s3Instructions or parquetInstructions may be provided");
        }
    }
}
