/*
 * Copyright (c) 2016-2020 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.kafka.ingest;

import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.v2.sources.chunk.*;
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
            final boolean allowNulls) {
        super(definition, chunkTypeForIndex, fieldNamesToColumnNames, allowNulls, GenericRecordChunkAdapter::makeFieldCopier);
    }
    /**
     * Create a GenericRecordChunkAdapter.
     *
     * @param definition        the definition of the output table
     * @param chunkTypeForIndex a function from column index to chunk type
     * @param columns           a map from Avro field names to Deephaven column names
     * @param allowNulls        true if null records should be allowed, if false then an ISE is thrown
     * @return a GenericRecordChunkAdapter for the given definition and column mapping
     */
    public static GenericRecordChunkAdapter make(
            final TableDefinition definition,
            final IntFunction<ChunkType> chunkTypeForIndex,
            final Map<String, String> columns,
            final boolean allowNulls) {
        return new GenericRecordChunkAdapter(
                definition, chunkTypeForIndex, columns, allowNulls);
    }

    private static FieldCopier makeFieldCopier(String fieldName, ChunkType chunkType, Class<?> dataType) {
        switch (chunkType) {
            case Char:
                return new GenericRecordCharFieldCopier(fieldName);
            case Byte:
                if (dataType == Boolean.class) {
                    return new GenericRecordBooleanFieldCopier(fieldName);
                }
                return new GenericRecordByteFieldCopier(fieldName);
            case Short:
                return new GenericRecordShortFieldCopier(fieldName);
            case Int:
                return new GenericRecordIntFieldCopier(fieldName);
            case Long:
                if (dataType == DBDateTime.class) {
                    throw new UnsupportedOperationException();
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
