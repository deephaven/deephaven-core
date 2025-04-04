//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.internal;

import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;

import java.util.Optional;

public class PartitionSpecHelper {

    public static Optional<PartitionField> find(PartitionSpec spec, int partitionFieldId) {
        PartitionField found = null;
        for (final PartitionField partitionField : spec.fields()) {
            if (partitionField.fieldId() == partitionFieldId) {
                if (found != null) {
                    throw new IllegalStateException(String
                            .format("Found multiple partition fields with partition field id %d", partitionFieldId));
                }
                found = partitionField;
            }
        }
        return Optional.ofNullable(found);
    }

    public static PartitionField get(PartitionSpec spec, int partitionFieldId) throws SchemaHelper.PathException {
        final Optional<PartitionField> partitionField = find(spec, partitionFieldId);
        if (partitionField.isEmpty()) {
            throw new SchemaHelper.PathException(
                    String.format("Unable to find partition field id %d", partitionFieldId));
        }
        return partitionField.get();
    }

    public static Optional<PartitionField> findIdentityForSchemaFieldId(PartitionSpec spec, int schemaFieldId) {
        for (final PartitionField partitionField : spec.fields()) {
            if (!partitionField.transform().isIdentity()) {
                continue;
            }
            if (partitionField.sourceId() == schemaFieldId) {
                return Optional.of(partitionField);
            }
        }
        return Optional.empty();
    }


}
