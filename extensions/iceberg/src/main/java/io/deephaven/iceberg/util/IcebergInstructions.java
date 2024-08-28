//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Immutable;

/**
 * This class provides instructions intended for reading Iceberg catalogs and tables. The default values documented in
 * this class may change in the future. As such, callers may wish to explicitly set the values.
 */
@Immutable
@BuildableStyle
// TODO I propose renaming, but this will be breaking change:
// IcebergInstructions -> IcebergReadInstructions
// IcebergBaseInstructions -> IcebergInstructions
public abstract class IcebergInstructions implements IcebergBaseInstructions {
    /**
     * The default {@link IcebergInstructions} to use when reading Iceberg data files. Providing this will use system
     * defaults for cloud provider-specific parameters.
     */
    @SuppressWarnings("unused")
    public static final IcebergInstructions DEFAULT = builder().build();

    public static Builder builder() {
        return ImmutableIcebergInstructions.builder();
    }

    public interface Builder extends IcebergBaseInstructions.Builder<Builder> {
        IcebergInstructions build();
    }
}
