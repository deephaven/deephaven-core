//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.iceberg.internal.PartitionSpecHelper;
import io.deephaven.iceberg.internal.SchemaHelper;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types.NestedField;
import org.immutables.value.Value;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

@Value.Immutable
@BuildableStyle
public abstract class ColumnInstructions {

    public static ColumnInstructions partitionField(int partitionFieldId) {
        return ImmutableColumnInstructions.builder().partitionFieldId(partitionFieldId).build();
    }

    public static ColumnInstructions schemaFieldPath(FieldPath fieldPath) {
        return ImmutableColumnInstructions.builder().schemaFieldPath(fieldPath).build();
    }

    abstract OptionalInt partitionFieldId();

    /**
     * The path to the Iceberg {@link NestedField} associated with this column.
     */
    abstract Optional<FieldPath> schemaFieldPath();

    PartitionField partitionField(PartitionSpec spec) throws SchemaHelper.PathException {
        return PartitionSpecHelper.get(spec, partitionFieldId().orElseThrow());
    }

    List<NestedField> schemaFieldPath(Schema schema) throws SchemaHelper.PathException {
        return schemaFieldPath().orElseThrow().resolve(schema);
    }



    // Note: very likely there will be additions here to support future additions; codecs, conversions, etc.

    @Value.Check
    final void checkType() {
        if (partitionFieldId().isPresent() == schemaFieldPath().isPresent()) {
            throw new IllegalArgumentException(
                    "ColumnInstructions must have exactly one of partitionFieldId or schemaFieldPath");
        }
    }
}
