//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.kafka.ingest;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.chunk.ChunkType;

import java.time.Instant;
import java.util.Map;
import java.util.function.IntFunction;

public class JsonNodeChunkAdapter extends MultiFieldChunkAdapter {
    private JsonNodeChunkAdapter(
            final TableDefinition definition,
            final IntFunction<ChunkType> chunkTypeForIndex,
            final Map<String, String> jsonPointerStrToColumnNames,
            final boolean allowNulls) {
        super(definition, chunkTypeForIndex, jsonPointerStrToColumnNames, allowNulls,
                JsonNodeChunkAdapter::makeFieldCopier);
    }

    /**
     * Create a JsonRecordChunkAdapter.
     *
     * @param definition the definition of the output table
     * @param chunkTypeForIndex a function from column index to chunk type
     * @param jsonPointerStrToColumnNames a map from JSON pointer strings to Deephaven column names
     * @param allowNulls true if null records should be allowed, if false then an ISE is thrown
     * @return a JsonRecordChunkAdapter for the given definition and column mapping
     */
    public static JsonNodeChunkAdapter make(
            final TableDefinition definition,
            final IntFunction<ChunkType> chunkTypeForIndex,
            final Map<String, String> jsonPointerStrToColumnNames,
            final boolean allowNulls) {
        return new JsonNodeChunkAdapter(
                definition, chunkTypeForIndex, jsonPointerStrToColumnNames, allowNulls);
    }

    private static FieldCopier makeFieldCopier(
            final String fieldName, final ChunkType chunkType, final Class<?> dataType, final Class<?> componentType) {
        switch (chunkType) {
            case Char:
                return new JsonNodeCharFieldCopier(fieldName);
            case Byte:
                if (dataType == Boolean.class) {
                    return new JsonNodeBooleanFieldCopier(fieldName);
                }
                return new JsonNodeByteFieldCopier(fieldName);
            case Short:
                return new JsonNodeShortFieldCopier(fieldName);
            case Int:
                return new JsonNodeIntFieldCopier(fieldName);
            case Long:
                if (dataType == Instant.class) {
                    return new JsonNodeInstantFieldCopier(fieldName);
                }
                return new JsonNodeLongFieldCopier(fieldName);
            case Float:
                return new JsonNodeFloatFieldCopier(fieldName);
            case Double:
                return new JsonNodeDoubleFieldCopier(fieldName);
            case Object:
                if (dataType == String.class) {
                    return new JsonNodeStringFieldCopier(fieldName);
                }
                if (dataType.isAssignableFrom(com.fasterxml.jackson.databind.JsonNode.class)) {
                    return new JsonNodeJsonNodeFieldCopier(fieldName);
                }
                throw new UncheckedDeephavenException("Type " + dataType.getSimpleName() + " not supported for JSON");
        }
        throw new IllegalArgumentException("Can not convert field of type " + dataType);
    }
}
