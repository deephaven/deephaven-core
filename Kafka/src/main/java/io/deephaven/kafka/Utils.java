package io.deephaven.kafka;

import org.apache.avro.Schema;

import java.util.List;

public class Utils {
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
            throw new UnsupportedOperationException(
                    "Union " + fieldName + " with more than 2 fields not supported");
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
        throw new UnsupportedOperationException(
                "Union " + fieldName + " not supported; only unions with NULL are supported at this time.");
    }
}
