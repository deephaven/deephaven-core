//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.extensions.s3.S3Instructions;
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
     * The {@link S3Instructions} to use for reading the Iceberg data files.
     */
    public abstract Optional<S3Instructions> s3Instructions();

    /**
     * A {@link Map map} of rename instructions from Iceberg to Deephaven column names to use when reading the Iceberg
     * data files.
     */
    public abstract Map<String, String> columnRename();

    public interface Builder {
        @SuppressWarnings("unused")
        Builder tableDefinition(TableDefinition tableDefinition);

        @SuppressWarnings("unused")
        Builder s3Instructions(S3Instructions s3Instructions);

        @SuppressWarnings("unused")
        Builder putColumnRename(String key, String value);

        @SuppressWarnings("unused")
        Builder putAllColumnRename(Map<String, ? extends String> entries);

        IcebergInstructions build();
    }
}
