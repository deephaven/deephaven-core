//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.kafka.ingest;

import io.deephaven.engine.table.TableDefinition;
import io.deephaven.chunk.*;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.*;
import java.util.function.IntFunction;
import java.util.regex.Pattern;

/**
 * Convert an Avro {@link GenericRecord} to Deephaven rows.
 * <p>
 * Each GenericRecord produces a single row of output, according to the maps of Table column names to Avro field names
 * for the keys and values.
 */
public class GenericRecordChunkAdapter extends MultiFieldChunkAdapter {
    private GenericRecordChunkAdapter(
            final TableDefinition definition,
            final IntFunction<ChunkType> chunkTypeForIndex,
            final Map<String, String> fieldNamesToColumnNames,
            final Pattern separator,
            final Schema schema,
            final boolean allowNulls) {
        super(definition, chunkTypeForIndex, fieldNamesToColumnNames, allowNulls, (fieldPathStr, chunkType,
                dataType, componentType) -> GenericRecordChunkAdapter.makeFieldCopier(schema, fieldPathStr, separator,
                        chunkType,
                        dataType, componentType));
    }

    /**
     * Create a GenericRecordChunkAdapter.
     *
     * @param definition the definition of the output table
     * @param chunkTypeForIndex a function from column index to chunk type
     * @param columns a map from Avro field paths to Deephaven column names
     * @param separator separator for composite fields names
     * @param schema the Avro schema for our input
     * @param allowNulls true if null records should be allowed, if false then an ISE is thrown
     * @return a GenericRecordChunkAdapter for the given definition and column mapping
     */
    public static GenericRecordChunkAdapter make(
            final TableDefinition definition,
            final IntFunction<ChunkType> chunkTypeForIndex,
            final Map<String, String> columns,
            final Pattern separator,
            final Schema schema,
            final boolean allowNulls) {
        return new GenericRecordChunkAdapter(
                definition, chunkTypeForIndex, columns, separator, schema, allowNulls);
    }

    private static Schema getFieldSchema(final Schema schema, final String fieldPathStr, Pattern separator) {
        final String[] fieldPath = GenericRecordUtil.getFieldPath(fieldPathStr, separator);
        final Schema fieldSchema = GenericRecordUtil.getFieldSchema(schema, fieldPath);
        return fieldSchema;
    }

    private static LogicalType getLogicalType(final Schema schema, final String fieldPathStr, Pattern separator) {
        final Schema fieldSchema = getFieldSchema(schema, fieldPathStr, separator);
        final LogicalType logicalType = fieldSchema.getLogicalType();
        return logicalType;
    }

    private static LogicalType getArrayTypeLogicalType(final Schema schema, final String fieldPathStr,
            Pattern separator) {
        final Schema fieldSchema = getFieldSchema(schema, fieldPathStr, separator);
        final LogicalType logicalType = fieldSchema.getElementType().getLogicalType();
        return logicalType;
    }

    private static FieldCopier makeFieldCopier(
            final Schema schema,
            final String fieldPathStr,
            final Pattern separator,
            final ChunkType chunkType,
            final Class<?> dataType,
            final Class<?> componentType) {
        switch (chunkType) {
            case Char:
                return new GenericRecordCharFieldCopier(fieldPathStr, separator, schema);
            case Byte:
                if (dataType == Boolean.class || dataType == boolean.class) {
                    return new GenericRecordBooleanFieldCopier(fieldPathStr, separator, schema);
                }
                return new GenericRecordByteFieldCopier(fieldPathStr, separator, schema);
            case Short:
                return new GenericRecordShortFieldCopier(fieldPathStr, separator, schema);
            case Int:
                return new GenericRecordIntFieldCopier(fieldPathStr, separator, schema);
            case Long:
                if (dataType == Instant.class) {
                    final LogicalType logicalType = getLogicalType(schema, fieldPathStr, separator);
                    if (logicalType == null) {
                        throw new IllegalArgumentException(
                                "Can not map field without a logical type to Instant: field=" + fieldPathStr);
                    }
                    if (logicalType instanceof LogicalTypes.TimestampMillis) {
                        return new GenericRecordLongFieldCopierWithMultiplier(fieldPathStr, separator, schema,
                                1000_000L);
                    }
                    if (logicalType instanceof LogicalTypes.TimestampMicros) {
                        return new GenericRecordLongFieldCopierWithMultiplier(fieldPathStr, separator, schema, 1000L);
                    }
                    throw new IllegalArgumentException(
                            "Can not map field with unknown logical type to Instant: field=" + fieldPathStr
                                    + ", logical type=" + logicalType);

                }
                return new GenericRecordLongFieldCopier(fieldPathStr, separator, schema);
            case Float:
                return new GenericRecordFloatFieldCopier(fieldPathStr, separator, schema);
            case Double:
                return new GenericRecordDoubleFieldCopier(fieldPathStr, separator, schema);
            case Object:
                if (dataType == String.class) {
                    return new GenericRecordStringFieldCopier(fieldPathStr, separator, schema);
                }
                if (dataType == BigDecimal.class) {
                    final String[] fieldPath = GenericRecordUtil.getFieldPath(fieldPathStr, separator);
                    final Schema fieldSchema = GenericRecordUtil.getFieldSchema(schema, fieldPath);
                    final LogicalType logicalType = fieldSchema.getLogicalType();
                    if (logicalType instanceof LogicalTypes.Decimal) {
                        final LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) logicalType;
                        return new GenericRecordBigDecimalFieldCopier(
                                fieldPathStr, separator, schema,
                                decimalType.getPrecision(), decimalType.getScale());
                    }
                    throw new IllegalArgumentException(
                            "Can not map field with non matching logical type to BigDecimal: " +
                                    "field=" + fieldPathStr + ", logical type=" + logicalType);
                }
                if (dataType.isArray()) {
                    if (Instant.class.isAssignableFrom(componentType)) {
                        final LogicalType logicalType = getArrayTypeLogicalType(schema, fieldPathStr, separator);
                        if (logicalType == null) {
                            throw new IllegalArgumentException(
                                    "Can not map field without a logical type to Instant[]: field=" + fieldPathStr);
                        }
                        if (logicalType instanceof LogicalTypes.TimestampMillis) {
                            return new GenericRecordInstantArrayFieldCopier(fieldPathStr, separator, schema,
                                    1000_000L);
                        }
                        if (logicalType instanceof LogicalTypes.TimestampMicros) {
                            return new GenericRecordInstantArrayFieldCopier(fieldPathStr, separator, schema, 1000L);
                        }
                        throw new IllegalArgumentException(
                                "Can not map field with unknown logical type to Instant[]: field=" + fieldPathStr
                                        + ", logical type=" + logicalType);

                    }
                    return new GenericRecordArrayFieldCopier(fieldPathStr, separator, schema, componentType);
                }
                // The GenericContainer.class.isAssignableFrom(dataType) case is also covered by the generic object
                // field copier.
                return new GenericRecordObjectFieldCopier(fieldPathStr, separator, schema);
        }
        throw new IllegalArgumentException("Can not convert field of type " + dataType);
    }
}
