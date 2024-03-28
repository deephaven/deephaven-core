//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequence.Iterator;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ChunkSource.FillContext;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.BaseTable.ListenerImpl;
import io.deephaven.stream.StreamToBlinkTableAdapter;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;

public final class TableToJsonStreamPublisher extends ListenerImpl {

    public static Table of(Table parent, String columnName, JsonStreamPublisherOptions options) {
        // todo: make builder for this?
        // todo: initial snapshot
        final String name = UUID.randomUUID().toString();
        final ColumnSource<Source> jsonColumnSource = parent.getColumnSource(columnName, Source.class);
        final JsonStreamPublisher publisher = options.execute();
        final TableDefinition tableDefinition = publisher.tableDefinition(JsonTableOptions::toColumnName);
        final StreamToBlinkTableAdapter adapter = new StreamToBlinkTableAdapter(tableDefinition, publisher,
                parent.getUpdateGraph(), name, Map.of(), true);
        final Table result = adapter.table();
        final TableToJsonStreamPublisher listener = new TableToJsonStreamPublisher(name, parent, (BaseTable<?>) result,
                publisher, options.chunkSize(), jsonColumnSource);
        parent.addUpdateListener(listener);
        return result;
    }

    private final JsonStreamPublisher publisher;
    private final int chunkSize;
    private final ColumnSource<Source> jsonColumnSource;

    private TableToJsonStreamPublisher(
            String description,
            Table parent,
            BaseTable<?> dependent,
            JsonStreamPublisher publisher,
            int chunkSize,
            ColumnSource<Source> jsonColumnSource) {
        super(description, parent, dependent);
        this.publisher = Objects.requireNonNull(publisher);
        this.chunkSize = chunkSize;
        this.jsonColumnSource = Objects.requireNonNull(jsonColumnSource);
    }

    @Override
    public void onUpdate(TableUpdate upstream) {
        final RowSet added = upstream.added();
        final Queue<Source> queue = new ArrayBlockingQueue<>(1, false);
        try (
                final WritableObjectChunk<Source, Values> dest = WritableObjectChunk.makeWritableChunk(chunkSize);
                final FillContext context = jsonColumnSource.makeFillContext(chunkSize);
                final Iterator it = added.getRowSequenceIterator()) {
            while (it.hasMore()) {
                final RowSequence rowSeq = it.getNextRowSequenceWithLength(chunkSize);
                jsonColumnSource.fillChunk(context, dest, rowSeq);
                final List<Source> sources = listView(dest.asWritableObjectChunk(), 0, rowSeq.intSize());
                queue.add(Source.of(sources));
                publisher.execute(Runnable::run, queue);
            }
        }
    }

    private static <T> List<T> listView(WritableObjectChunk<T, ?> chunk, int offset, int length) {
        final List<T> list = Arrays.asList(chunk.array());
        final int arrayOffset = chunk.arrayOffset();
        return list.subList(arrayOffset + offset, arrayOffset + offset + length);
    }
}
