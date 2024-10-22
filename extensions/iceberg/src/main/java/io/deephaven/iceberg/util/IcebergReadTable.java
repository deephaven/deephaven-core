//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value;

/**
 * Instructions for reading an Iceberg table as a Deephaven table.
 */
@Value.Immutable
@BuildableStyle
public abstract class IcebergReadTable extends IcebergReadOperationsBase {

    public static Builder builder() {
        return ImmutableIcebergReadTable.builder();
    }

    public interface Builder extends IcebergReadOperationsBase.Builder<IcebergReadTable, Builder> {
    }
}
