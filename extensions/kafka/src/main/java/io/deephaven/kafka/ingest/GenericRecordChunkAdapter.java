/*
 * Copyright (c) 2016-2020 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.kafka.ingest;

import io.deephaven.engine.table.TableDefinition;
import io.deephaven.time.DateTime;
import io.deephaven.chunk.*;
import io.deephaven.kafka.Utils;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.util.*;
import java.util.function.IntFunction;

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
            final String separator,
            final Schema schema,
            final boolean allowNulls) {
        super(definition, chunkTypeForIndex, fieldNamesToColumnNames, allowNulls, (fieldName, chunkType,
                dataType) -> GenericRecordChunkAdapter.makeFieldCopier(schema, fieldName, separator, chunkType,
                        dataType));
    }

    /**
     * Create a GenericRecordChunkAdapter.
     *
     * @param definition the definition of the output table
     * @param chunkTypeForIndex a function from column index to chunk type
     * @param columns a map from Avro field names to Deephaven column names
     * @param separator separator for composite fields names
     * @param schema the Avro schema for our input
     * @param allowNulls true if null records should be allowed, if false then an ISE is thrown
     * @return a GenericRecordChunkAdapter for the given definition and column mapping
     */
    public static GenericRecordChunkAdapter make(
            final TableDefinition definition,
            final IntFunction<ChunkType> chunkTypeForIndex,
            final Map<String, String> columns,
            final String separator,
            final Schema schema,
            final boolean allowNulls) {
        return new GenericRecordChunkAdapter(
                definition, chunkTypeForIndex, columns, separator, schema, allowNulls);
    }

    private static FieldCopier makeFieldCopier(
            final Schema schema,
            final String fieldName,
            final String separator,
            final ChunkType chunkType,
            final Class<?> dataType) {
        switch (chunkType) {
            case Char:
                return new GenericRecordCharFieldCopier(fieldName, separator);
            case Byte:
                if (dataType == Boolean.class || dataType == boolean.class) {
                    return new GenericRecordBooleanFieldCopier(fieldName);
                }
                return new GenericRecordByteFieldCopier(fieldName, separator);
            case Short:
                return new GenericRecordShortFieldCopier(fieldName, separator);
            case Int:
                return new GenericRecordIntFieldCopier(fieldName, separator);
            case Long:
                if (dataType == DateTime.class) {
                    final String[] fieldPath = GenericRecordUtil.getFieldPath(fieldName, separator);
                    final Schema fieldSchema = GenericRecordUtil.getFieldSchema(schema, fieldPath);
                    final LogicalType logicalType = fieldSchema.getLogicalType();
                    if (logicalType == null) {
                        throw new IllegalArgumentException(
                                "Can not map field without a logical type to DateTime: field=" + fieldName);
                    }
                    if (logicalType instanceof LogicalTypes.TimestampMillis) {
                        // micros to nanos
                        return new GenericRecordLongFieldCopierWithMultiplier(fieldName, 1000L);
                    }
                    if (logicalType instanceof LogicalTypes.TimestampMicros) {
                        // millis to nanos
                        return new GenericRecordLongFieldCopierWithMultiplier(fieldName, 1000L * 1000L);
                    }
                    throw new IllegalArgumentException(
                            "Can not map field with unknown logical type to DateTime: field=" + fieldName
                                    + ", type=" + logicalType);

                }
                return new GenericRecordLongFieldCopier(fieldName, separator);
            case Float:
                return new GenericRecordFloatFieldCopier(fieldName, separator);
            case Double:
                return new GenericRecordDoubleFieldCopier(fieldName, separator);
            case Object:
                if (dataType == String.class) {
                    return new GenericRecordStringFieldCopier(fieldName, separator);
                }
                final Schema.Field field = schema.getField(fieldName);
                if (field != null) {
                    final String[] fieldPath = GenericRecordUtil.getFieldPath(fieldName, separator);
                    final Schema fieldSchema = GenericRecordUtil.getFieldSchema(schema, fieldPath);
                    final LogicalType logicalType = fieldSchema.getLogicalType();
                    if (logicalType instanceof LogicalTypes.Decimal) {
                        final LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) logicalType;
                        return new GenericRecordBigDecimalFieldCopier(
                                fieldName, decimalType.getPrecision(), decimalType.getScale());
                    }
                }
                return new GenericRecordObjectFieldCopier(fieldName, separator);
        }
        throw new IllegalArgumentException("Can not convert field of type " + dataType);
    }
}
