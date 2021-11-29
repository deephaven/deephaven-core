/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.kafka.ingest;

import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;

import java.util.Map;
import java.util.function.IntFunction;

public class MultiFieldChunkAdapter implements KeyOrValueProcessor {
    private final boolean allowNulls;

    private final int[] chunkOffsets;
    private final FieldCopier[] fieldCopiers;

    protected MultiFieldChunkAdapter(
            final TableDefinition definition,
            final IntFunction<ChunkType> chunkTypeForIndex,
            final Map<String, String> fieldNamesToColumnNames,
            final boolean allowNulls,
            final FieldCopier.Factory fieldCopierFactory) {
        this.allowNulls = allowNulls;

        final String[] columnNames = definition.getColumnNamesArray();
        final Class<?>[] columnTypes = definition.getColumnTypesArray();

        final TObjectIntMap<String> deephavenColumnNameToIndex = new TObjectIntHashMap<>(columnNames.length, 0.5f, -1);
        for (int ii = 0; ii < columnNames.length; ++ii) {
            deephavenColumnNameToIndex.put(columnNames[ii], ii);
        }

        chunkOffsets = new int[fieldNamesToColumnNames.size()];
        fieldCopiers = new FieldCopier[fieldNamesToColumnNames.size()];

        int col = 0;
        for (Map.Entry<String, String> fieldToColumn : fieldNamesToColumnNames.entrySet()) {
            final int deephavenColumnIndex = deephavenColumnNameToIndex.get(fieldToColumn.getValue());
            if (deephavenColumnIndex == deephavenColumnNameToIndex.getNoEntryValue()) {
                throw new IllegalArgumentException("Column not found in Deephaven table: " + deephavenColumnIndex);
            }

            chunkOffsets[col] = deephavenColumnIndex;
            fieldCopiers[col++] = fieldCopierFactory.make(fieldToColumn.getKey(),
                    chunkTypeForIndex.apply(deephavenColumnIndex), columnTypes[deephavenColumnIndex]);
        }
    }


    @Override
    public void handleChunk(ObjectChunk<Object, Values> inputChunk,
            WritableChunk<Values>[] publisherChunks) {
        if (!allowNulls) {
            for (int ii = 0; ii < inputChunk.size(); ++ii) {
                if (inputChunk.get(ii) == null) {
                    throw new KafkaIngesterException("Null records are not permitted");
                }
            }
        }
        for (int cc = 0; cc < chunkOffsets.length; ++cc) {
            final WritableChunk<Values> publisherChunk = publisherChunks[chunkOffsets[cc]];
            final int existingSize = publisherChunk.size();
            publisherChunk.setSize(existingSize + inputChunk.size());
            fieldCopiers[cc].copyField(inputChunk, publisherChunk, 0, existingSize, inputChunk.size());
        }
    }
}
