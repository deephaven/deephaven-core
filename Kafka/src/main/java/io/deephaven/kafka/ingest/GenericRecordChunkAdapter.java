/*
 * Copyright (c) 2016-2020 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.kafka.ingest;

import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.v2.sources.chunk.*;
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
            final Schema schema,
            final boolean allowNulls) {
        super(definition, chunkTypeForIndex, fieldNamesToColumnNames, allowNulls, (fieldName, chunkType,
                dataType) -> GenericRecordChunkAdapter.makeFieldCopier(schema, fieldName, chunkType, dataType));
    }

    /**
     * Create a GenericRecordChunkAdapter.
     *
     * @param definition the definition of the output table
     * @param chunkTypeForIndex a function from column index to chunk type
     * @param columns a map from Avro field names to Deephaven column names
     * @param schema the Avro schema for our input
     * @param allowNulls true if null records should be allowed, if false then an ISE is thrown
     * @return a GenericRecordChunkAdapter for the given definition and column mapping
     */
    public static GenericRecordChunkAdapter make(
            final TableDefinition definition,
            final IntFunction<ChunkType> chunkTypeForIndex,
            final Map<String, String> columns,
            final Schema schema,
            final boolean allowNulls) {
        return new GenericRecordChunkAdapter(
                definition, chunkTypeForIndex, columns, schema, allowNulls);
    }

    private static FieldCopier makeFieldCopier(
            final Schema schema, final String fieldName, final ChunkType chunkType, final Class<?> dataType) {
        switch (chunkType) {
            case Char:
                return new GenericRecordCharFieldCopier(fieldName);
            case Byte:
                if (dataType == Boolean.class || dataType == boolean.class) {
                    return new GenericRecordBooleanFieldCopier(fieldName);
                }
                return new GenericRecordByteFieldCopier(fieldName);
            case Short:
                return new GenericRecordShortFieldCopier(fieldName);
            case Int:
                return new GenericRecordIntFieldCopier(fieldName);
            case Long:
                if (dataType == DBDateTime.class) {
                    final Schema.Field field = schema.getField(fieldName);
                    if (field != null) {
                        Schema fieldSchema = Utils.getEffectiveSchema(fieldName, field.schema());
                        final LogicalType logicalType = fieldSchema.getLogicalType();
                        if (logicalType == null) {
                            throw new IllegalArgumentException(
                                    "Can not map field without a logical type to DBDateTime: field=" + fieldName);
                        }
                        if (LogicalTypes.timestampMicros().equals(logicalType)) {
                            // micros to nanos
                            return new GenericRecordLongFieldCopierWithMultiplier(fieldName, 1000L);
                        } else if (LogicalTypes.timestampMillis().equals(logicalType)) {
                            // millis to nanos
                            return new GenericRecordLongFieldCopierWithMultiplier(fieldName, 1000000L);
                        }
                        throw new IllegalArgumentException(
                                "Can not map field with unknown logical type to DBDateTime: field=" + fieldName
                                        + ", type=" + logicalType);

                    } else {
                        throw new IllegalArgumentException(
                                "Can not map field not in schema to DBDateTime: field=" + fieldName);
                    }
                }
                return new GenericRecordLongFieldCopier(fieldName);
            case Float:
                return new GenericRecordFloatFieldCopier(fieldName);
            case Double:
                return new GenericRecordDoubleFieldCopier(fieldName);
            case Object:
                if (dataType == String.class) {
                    return new GenericRecordStringFieldCopier(fieldName);
                } else {
                    return new GenericRecordObjectFieldCopier(fieldName);
                }
        }
        throw new IllegalArgumentException("Can not convert field of type " + dataType);
    }
}
