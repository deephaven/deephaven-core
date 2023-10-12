/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.ingest;

import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;

import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

public class MultiFieldChunkAdapter implements KeyOrValueProcessor {

    public static int[] chunkOffsets(
            final TableDefinition definition,
            final Map<String, String> fieldNamesToColumnNames) {
        final String[] columnNames = definition.getColumnNamesArray();
        final TObjectIntMap<String> deephavenColumnNameToIndex = new TObjectIntHashMap<>(columnNames.length, 0.5f, -1);
        for (int ii = 0; ii < columnNames.length; ++ii) {
            deephavenColumnNameToIndex.put(columnNames[ii], ii);
        }
        final int[] chunkOffsets = new int[fieldNamesToColumnNames.size()];
        int col = 0;
        for (String columnName : fieldNamesToColumnNames.values()) {
            final int deephavenColumnIndex = deephavenColumnNameToIndex.get(columnName);
            if (deephavenColumnIndex == deephavenColumnNameToIndex.getNoEntryValue()) {
                throw new IllegalArgumentException("Column not found in Deephaven table: " + deephavenColumnIndex);
            }
            chunkOffsets[col++] = deephavenColumnIndex;
        }
        return chunkOffsets;
    }

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
        final List<ColumnDefinition<?>> columns = definition.getColumns();

        final TObjectIntMap<String> deephavenColumnNameToIndex = new TObjectIntHashMap<>(columnNames.length, 0.5f, -1);
        for (int ii = 0; ii < columnNames.length; ++ii) {
            deephavenColumnNameToIndex.put(columnNames[ii], ii);
        }

        chunkOffsets = new int[fieldNamesToColumnNames.size()];
        fieldCopiers = new FieldCopier[fieldNamesToColumnNames.size()];

        int col = 0;
        for (Map.Entry<String, String> fieldToColumn : fieldNamesToColumnNames.entrySet()) {
            final String columnName = fieldToColumn.getValue();
            final int deephavenColumnIndex = deephavenColumnNameToIndex.get(columnName);
            if (deephavenColumnIndex == deephavenColumnNameToIndex.getNoEntryValue()) {
                throw new IllegalArgumentException("Column not found in Deephaven table: " + deephavenColumnIndex);
            }

            chunkOffsets[col] = deephavenColumnIndex;
            final ColumnDefinition<?> colDef = columns.get(deephavenColumnIndex);
            fieldCopiers[col++] = fieldCopierFactory.make(fieldToColumn.getKey(),
                    chunkTypeForIndex.apply(deephavenColumnIndex), colDef.getDataType(), colDef.getComponentType());
        }
    }


    @Override
    public void handleChunk(ObjectChunk<Object, Values> inputChunk, WritableChunk<Values>[] publisherChunks) {
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
