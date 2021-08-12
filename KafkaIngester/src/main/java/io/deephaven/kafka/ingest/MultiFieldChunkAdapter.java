/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.kafka.ingest;

import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.ChunkType;
import io.deephaven.db.v2.sources.chunk.ObjectChunk;
import io.deephaven.db.v2.sources.chunk.WritableChunk;

import java.util.Map;
import java.util.function.IntFunction;

public class MultiFieldChunkAdapter implements KeyOrValueProcessor {
    private final boolean allowNulls;

    private final int[] chunkOffsets;
    private final FieldCopier[] fieldCopiers;

    protected MultiFieldChunkAdapter(
            final TableDefinition definition,
            final IntFunction<ChunkType> chunkTypeForIndex,
            final Map<String, String> columns,
            final boolean allowNulls,
            final FieldCopier.Factory fieldCopierFactory) {
        this.allowNulls = allowNulls;

        final String[] columnNames = definition.getColumnNamesArray();
        final Class<?>[] columnTypes = definition.getColumnTypesArray();

        final TObjectIntMap<String> deephavenColumnNameToIndex = new TObjectIntHashMap<>();
        for (int ii = 0; ii < columnNames.length; ++ii) {
            deephavenColumnNameToIndex.put(columnNames[ii], ii);
        }

        chunkOffsets = new int[columns.size()];
        fieldCopiers = new FieldCopier[columns.size()];

        int col = 0;
        for (Map.Entry<String, String> fieldToColumn : columns.entrySet()) {
            final int deephavenColumnIndex = deephavenColumnNameToIndex.get(fieldToColumn.getValue());
            if (deephavenColumnIndex == deephavenColumnNameToIndex.getNoEntryValue()) {
                throw new IllegalArgumentException("Column not found in Deephaven table: " + deephavenColumnIndex);
            }

            chunkOffsets[col] = deephavenColumnIndex;
            fieldCopiers[col++] = fieldCopierFactory.make(fieldToColumn.getKey(), chunkTypeForIndex.apply(deephavenColumnIndex), columnTypes[deephavenColumnIndex]);
        }
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
