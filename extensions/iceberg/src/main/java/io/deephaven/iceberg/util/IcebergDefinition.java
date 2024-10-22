//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.engine.table.TableDefinition;
import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value;

/**
 * Instructions for reading {@link TableDefinition} for an Iceberg table.
 */
@Value.Immutable
@BuildableStyle
public abstract class IcebergDefinition extends IcebergReadOperationsBase {

    public static Builder builder() {
        return ImmutableIcebergDefinition.builder();
    }

    public interface Builder extends IcebergReadOperationsBase.Builder<IcebergDefinition, Builder> {
    }
}
