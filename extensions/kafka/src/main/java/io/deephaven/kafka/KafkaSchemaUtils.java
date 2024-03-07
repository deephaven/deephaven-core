//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.kafka;

import org.apache.avro.Schema;

import java.util.List;

public class KafkaSchemaUtils {
    public static Schema getEffectiveSchema(final String fieldName, final Schema fieldSchema) {
        if (fieldSchema.getType() != Schema.Type.UNION) {
            return fieldSchema;
        }
        final List<Schema> unionTypes = fieldSchema.getTypes();
        final int unionSize = unionTypes.size();
        if (unionSize == 0) {
            throw new IllegalArgumentException("empty union " + fieldName);
        }
        if (unionSize != 2) {
            // For unions of more than 2 we will just give generic records back;
            // the caller can detect this happened by checking the output schema is same as input.
            return fieldSchema;
        }
        final Schema unionField0 = unionTypes.get(0);
        final Schema unionField1 = unionTypes.get(1);
        final Schema.Type unionType0 = unionField0.getType();
        final Schema.Type unionType1 = unionField1.getType();
        if (unionType1 == Schema.Type.NULL) {
            return unionField0;
        }
        if (unionType0 == Schema.Type.NULL) {
            return unionField1;
        }
        // For unions of 2 that are not simple allowing nulls we want to give generic records back;
        // the caller can detect this happened by checking the output schema is same as input.
        return fieldSchema;
    }
}
