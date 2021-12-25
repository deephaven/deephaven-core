package io.deephaven.kafka.ingest;

import io.deephaven.kafka.Utils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.util.regex.Pattern;

public class GenericRecordUtil {
    /**
     * Given a composite (by nesting) field name, composed of individual names separate with the given separator, obtain
     * a {@code String[]} path with the individual field names.
     * 
     * @param fieldName A composite (by nesting) field name.
     * @param separator The separator pattern used for composing {@code fieldName}
     * @return A {@code String[]} path with the individual field names for {@code fieldName}
     */
    public static String[] getFieldPath(final String fieldName, final Pattern separator) {
        if (fieldName == null) {
            return null;
        }
        return separator.split(fieldName);
    }

    /**
     * Get the value out of a Generic Record for a given field path (including nesting).
     * 
     * @param record A {@code GenericRecord} from which to extract a value.
     * @param fieldPath An array of individual field names, in order according to nesting from the root representing a
     *        path to an individual desired field
     * @return The value in {@code record} for the given {@code fieldPath}
     */
    public static Object getPath(final GenericRecord record, final String[] fieldPath) {
        if (record == null) {
            return null;
        }
        GenericRecord parentRecord = record;
        int i = 0;
        while (i < fieldPath.length - 1) {
            parentRecord = (GenericRecord) parentRecord.get(fieldPath[i]);
            ++i;
        }
        return parentRecord.get(fieldPath[i]);
    }

    private static String buildPartialFieldName(final String[] fieldPath, final int k) {
        final StringBuilder sb = new StringBuilder("(");
        for (int i = 0; i < k; ++i) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(fieldPath[i]);
        }
        sb.append(")");
        return sb.toString();
    }

    /**
     * Get the schema for a particular field path (including nesting).
     * 
     * @param schema A {@code Schema}, for the root where to start nested field navigation.
     * @param fieldPath An array of individual field names, in order according to nesting from the root representing a
     *        path to an individual desired field
     * @return The schema for the given {@code fieldPath}
     */
    public static Schema getFieldSchema(final Schema schema, final String[] fieldPath) {
        int i = 0;
        Schema s = schema;
        while (i < fieldPath.length) {
            final String fieldName = fieldPath[i];
            final Schema.Field field = s.getField(fieldName);
            if (field == null) {
                throw new IllegalArgumentException("Can't find field for path " + buildPartialFieldName(fieldPath, i));
            }
            s = Utils.getEffectiveSchema(fieldName, field.schema());
            ++i;
        }
        return s;
    }
}
