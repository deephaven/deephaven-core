package io.deephaven.stream;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequence.Iterator;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ChunkSource.FillContext;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.util.TableTools;

import java.util.Objects;

/**
 * An adapter around {@link StreamConsumer} that allows the easy consumption of data from a {@link Table}.
 */
public final class TableStreamConsumer {

    private final StreamConsumer consumer;
    private final TableDefinition definition;
    private final boolean sync;
    private final int chunkSize;

    /**
     * Creates a new table stream consumer.
     *
     * @param consumer the stream consumer
     * @param definition the table definition
     * @param chunkSize the chunk size
     * @param sync whether chunked additions to the consumer need to be synchronous
     */
    public TableStreamConsumer(StreamConsumer consumer, TableDefinition definition, int chunkSize, boolean sync) {
        this.consumer = Objects.requireNonNull(consumer);
        this.definition = Objects.requireNonNull(definition);
        this.sync = sync;
        this.chunkSize = chunkSize;
    }

    public StreamConsumer consumer() {
        return consumer;
    }

    public TableDefinition definition() {
        return definition;
    }

    public boolean sync() {
        return sync;
    }

    public int chunkSize() {
        return chunkSize;
    }

    public void add(Table newData) {
        // todo: do we need copy?
        // io.deephaven.engine.table.impl.util.AppendOnlyArrayBackedMutableTable.processPendingTable
        try (final WritableRowSet wrs = newData.getRowSet().copy()) {
            addInternal(newData, wrs.getRowSequenceIterator());
        }
    }

    public void add(Table newData, RowSet rowSet) {
        addInternal(newData, rowSet.getRowSequenceIterator());
    }

    public void add(Table newData, RowSequence rowSequence) {
        addInternal(newData, rowSequence.getRowSequenceIterator());
    }

    public void acceptFailure(Exception cause) {
        consumer.acceptFailure(cause);
    }

    private void addInternal(Table newData, Iterator it) {
        definition.checkMutualCompatibility(newData.getDefinition());
        // todo: this is what BaseArrayBackedMutableTable does - do we need select()?
        newData = newData.isRefreshing() ? TableTools.emptyTable(1).snapshot(newData) : newData.select();
        if (sync) {
            synchronized (consumer) {
                fillAndHandoff(newData, it);
            }
        } else {
            fillAndHandoff(newData, it);
        }
    }

    private void fillAndHandoff(Table newData, Iterator it) {
        while (it.hasMore()) {
            final WritableChunk[] chunks =
                    StreamToTableAdapter.makeChunksForDefinition(definition, chunkSize);
            final RowSequence seq = it.getNextRowSequenceWithLength(chunkSize);
            int columnIx = 0;
            final java.util.Iterator<ColumnSource<Object>> srcIt = definition
                    .getColumnStream()
                    .map(ColumnDefinition::getName)
                    .map(newData::getColumnSource)
                    .iterator();
            while (srcIt.hasNext()) {
                final ColumnSource<?> src = srcIt.next();
                try (final FillContext fillContext = src.makeFillContext(chunkSize)) {
                    src.fillChunk(fillContext, chunks[columnIx], seq);
                }
                ++columnIx;
            }
            consumer.accept(chunks);
        }
    }
}
