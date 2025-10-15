//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class InferenceInstructionsTest {
    private static Schema simpleSchema(org.apache.iceberg.types.Type type) {
        return new Schema(
                Types.NestedField.optional(42, "F1", type),
                Types.NestedField.required(43, "F2", type));
    }

    @Test
    void equalityWithDifferentSchemaInstances() {
        final Schema s1 = simpleSchema(Types.IntegerType.get());
        final Schema s2 = simpleSchema(Types.IntegerType.get());

        // This is Iceberg implementation decision
        assertThat(s1).isNotEqualTo(s2);
        assertThat(s2).isNotEqualTo(s1);

        final InferenceInstructions i1 = InferenceInstructions.of(s1);
        final InferenceInstructions i2 = InferenceInstructions.of(s2);

        assertThat(i1).isEqualTo(i2);
        assertThat(i2).isEqualTo(i1);
        assertThat(i1).hasSameHashCodeAs(i2);
    }
}
