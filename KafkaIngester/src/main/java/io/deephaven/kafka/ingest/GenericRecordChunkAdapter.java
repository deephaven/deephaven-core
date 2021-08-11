/*
 * Copyright (c) 2016-2020 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.kafka.ingest;

import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import io.deephaven.db.tables.TableDefinition;
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
public class GenericRecordChunkAdapter implements KeyOrValueProcessor {
    private final boolean allowNulls;

    private final int[] chunkOffsets;
    private final GenericRecordFieldCopier[] fieldCopiers;

    private GenericRecordChunkAdapter(
            final TableDefinition definition,
            final IntFunction<ChunkType> chunkTypeForIndex,
            final Map<String, String> columns,
            final boolean allowNulls) {
        this.allowNulls = allowNulls;

        final String[] columnNames = definition.getColumnNamesArray();
        final Class<?>[] columnTypes = definition.getColumnTypesArray();

        final TObjectIntMap<String> deephavenColumnNameToIndex = new TObjectIntHashMap<>();
        for (int ii = 0; ii < columnNames.length; ++ii) {
            deephavenColumnNameToIndex.put(columnNames[ii], ii);
        }

        chunkOffsets = new int[columns.size()];
        fieldCopiers = new GenericRecordFieldCopier[columns.size()];

        int col = 0;
        for (Map.Entry<String, String> avroDeephavenNamePair : columns.entrySet()) {
            final int deephavenColumnIndex = deephavenColumnNameToIndex.get(avroDeephavenNamePair.getValue());
            if (deephavenColumnIndex == deephavenColumnNameToIndex.getNoEntryValue()) {
                throw new IllegalArgumentException("Column not found in Deephaven table: " + deephavenColumnIndex);
            }

            chunkOffsets[col] = deephavenColumnIndex;
            fieldCopiers[col++] = GenericRecordFieldCopier.make(avroDeephavenNamePair.getKey(), chunkTypeForIndex.apply(deephavenColumnIndex), columnTypes[deephavenColumnIndex]);
        }
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

    @Override
    public void handleChunk(ObjectChunk<Object, Attributes.Values> inputChunk, WritableChunk<Attributes.Values>[] publisherChunks) {
        if (!allowNulls) {
            for (int ii = 0; ii < inputChunk.size(); ++ii) {
                if (inputChunk.get(ii) == null) {
                    throw new KafkaIngesterException("Null records are not permitted");
                }
            }
        }
        for (int cc = 0; cc < chunkOffsets.length; ++cc) {
            final WritableChunk<Attributes.Values> publisherChunk = publisherChunks[chunkOffsets[cc]];
            final int existingSize = publisherChunk.size();
            publisherChunk.setSize(existingSize + inputChunk.size());
            fieldCopiers[cc].copyField(inputChunk, publisherChunk, 0, existingSize, inputChunk.size());
        }
    }
}
