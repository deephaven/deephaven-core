//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package org.apache.iceberg;

import org.apache.iceberg.transforms.Transform;

public class PartitionFieldHack {

    public static PartitionField of(int sourceId, int fieldId, String name, Transform<?, ?> transform) {
        return new PartitionField(sourceId, fieldId, name, transform);
    }
}
