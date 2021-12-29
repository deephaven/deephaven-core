package io.deephaven.kafka.ingest;

import io.deephaven.kafka.KafkaSchemaUtils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.util.Arrays;
import java.util.regex.Pattern;

public class GenericRecordUtil {
    /**
     * Given a composite (by nesting) field name, composed of individual names separate with the given separator, obtain
     * a {@code String[]} path with the individual field names.
     * 
     * @param fieldName A composite (by nesting) field name.
     * @param separator The separator pattern used for composing {@code fieldName}
     * @param schema An avro schema.
     * @return A {@code String[]} path with the individual field names for {@code fieldName}
     */
    public static int[] getFieldPath(final String fieldName, final Pattern separator, final Schema schema) {
        if (fieldName == null) {
            return null;
        }
        final String[] strFieldPath = separator.split(fieldName);
        final int[] fieldPath = new int[strFieldPath.length];
        Schema currSchema = schema;
        int i = 0;
        while (true) {
            final Schema.Field currField = currSchema.getField(strFieldPath[i]);
            fieldPath[i] = currField.pos();
            ++i;
            if (i == fieldPath.length) {
                break;
            }
            currSchema = currField.schema();
        }
        return fieldPath;
    }

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
     * @param fieldPath An array of individual field position indices, in order according to nesting from the root
     *        representing a path to an individual desired field
     * @return The value in {@code record} for the given {@code fieldPath}
     */
    public static Object getPath(final GenericRecord record, final int[] fieldPath) {
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

    /**
     * Get the schema for a particular field path (including nesting).
     * 
     * @param schema A {@link org.apache.avro.Schema} to start nested field navigation from
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
                final String partialFieldName = Arrays.toString(Arrays.copyOf(fieldPath, i + 1));
                throw new IllegalArgumentException("Can't find field for path " + partialFieldName);
            }
            s = KafkaSchemaUtils.getEffectiveSchema(fieldName, field.schema());
            ++i;
        }
        return s;
    }
}
