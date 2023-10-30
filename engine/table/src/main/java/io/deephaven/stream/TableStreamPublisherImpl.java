package io.deephaven.stream;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequence.Iterator;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ChunkSource.FillContext;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.NotificationStepSource;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot.SnapshotFunction;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot.State;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.SafeCloseableArray;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

class TableStreamPublisherImpl implements StreamPublisher {

    private final String name;
    private final TableDefinition definition;
    private final Runnable onFlushCallback;
    private final Runnable onShutdownCallback;
    private final int chunkSize;
    private StreamConsumer consumer;

    TableStreamPublisherImpl(String name, TableDefinition definition, Runnable onFlushCallback,
            Runnable onShutdownCallback, int chunkSize) {
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("chunkSize must be positive");
        }
        this.name = Objects.requireNonNull(name);
        this.definition = Objects.requireNonNull(definition);
        this.onFlushCallback = onFlushCallback;
        this.onShutdownCallback = onShutdownCallback;
        this.chunkSize = chunkSize;
    }

    @Override
    public void register(@NotNull StreamConsumer consumer) {
        if (this.consumer != null) {
            throw new IllegalStateException("Can not register multiple StreamConsumers.");
        }
        this.consumer = Objects.requireNonNull(consumer);
    }

    public TableDefinition definition() {
        return definition;
    }

    public void add(Table table) {
        // Note: we expect an exception in FillChunks / io.deephaven.engine.table.Table#getColumnSource if the table is
        // incompatible instead of a heavier-weight
        // io.deephaven.engine.table.TableDefinition checkCompatibility / checkMutualCompatibility
        // we could do here.
        final FillChunks fillChunks = new FillChunks(table);
        try (final SafeCloseable ignored =
                ExecutionContext.getContext().withUpdateGraph(table.getUpdateGraph()).open()) {
            ConstructSnapshot.callDataSnapshotFunction(
                    TableStreamPublisherImpl.class.getSimpleName() + "-" + name,
                    ConstructSnapshot.makeSnapshotControl(false, table.isRefreshing(), (NotificationStepSource) table),
                    fillChunks);
        }
        consumer.accept(fillChunks.outstandingChunks);
    }

    public void publishFailure(Throwable e) {
        consumer.acceptFailure(e);
    }

    @Override
    public void flush() {
        // even though this impl itself doesn't hold onto any data, the publisher may be batching and want to call #add
        if (onFlushCallback != null) {
            onFlushCallback.run();
        }
    }

    @Override
    public void shutdown() {
        if (onShutdownCallback != null) {
            onShutdownCallback.run();
        }
    }

    private class FillChunks implements SnapshotFunction {
        private final Table table;
        private final ColumnSource<?>[] sources;
        private final List<WritableChunk<Values>[]> outstandingChunks;

        public FillChunks(Table table) {
            this.table = Objects.requireNonNull(table);
            this.sources = new ColumnSource[definition.numColumns()];
            // sources is in the same order as definition columns
            int i = 0;
            for (ColumnDefinition<?> column : definition.getColumns()) {
                sources[i++] = ReinterpretUtils.maybeConvertToPrimitive(
                        table.getColumnSource(column.getName(), column.getDataType(), column.getComponentType()));
            }
            this.outstandingChunks = new ArrayList<>();
        }

        @Override
        public boolean call(boolean usePrev, long beforeClockValue) {
            final State state = ConstructSnapshot.state();
            // We are calling reset() before returning false, even though it is not technically necessary with our
            // call to reset() here. That said, the reset() before returning false saves a little bit of time during the
            // next snapshot attempt which improves our chances of getting a consistent snapshot.
            //
            // Even though we are calling reset() before we return false, we still need to check it here as it's
            // possible this function returned true, but the construct snapshot implementation re-invokes us because it
            // was an inconsistent snapshot.
            reset();
            final RowSet rowSet = usePrev
                    ? table.getRowSet().prev()
                    : table.getRowSet();
            final long initialSize = rowSet.size();
            final int numColumns = sources.length;
            final FillContext[] fillContexts = new FillContext[numColumns];
            try (
                    final SharedContext sharedContext = numColumns > 1 ? SharedContext.makeSharedContext() : null;
                    final SafeCloseable ignored = new SafeCloseableArray<>(fillContexts);
                    final Iterator rows = rowSet.getRowSequenceIterator()) {
                {
                    final int fillContextSize = (int) Math.min(chunkSize, initialSize);
                    for (int i = 0; i < numColumns; i++) {
                        fillContexts[i] = sources[i].makeFillContext(fillContextSize, sharedContext);
                    }
                }
                long remaining = initialSize;
                while (rows.hasMore()) {
                    if (sharedContext != null) {
                        sharedContext.reset();
                    }
                    assertTrue(state, remaining > 0, "remaining > 0");
                    final RowSequence rowSeq = rows.getNextRowSequenceWithLength(chunkSize);
                    final int rowSeqSize = rowSeq.intSize();
                    assertTrue(state, rowSeqSize == Math.min(chunkSize, remaining),
                            "rowSeqSize == Math.min(chunkSize, remaining)");
                    remaining -= rowSeqSize;
                    final WritableChunk<Values>[] sinks =
                            StreamChunkUtils.makeChunksForDefinition(definition, rowSeqSize);
                    // Note: adding to list ASAP to ensure they get cleaned up via #reset if there is an exception
                    // during filling.
                    outstandingChunks.add(sinks);
                    for (int i = 0; i < numColumns; ++i) {
                        if (usePrev) {
                            sources[i].fillPrevChunk(fillContexts[i], sinks[i], rowSeq);
                        } else {
                            sources[i].fillChunk(fillContexts[i], sinks[i], rowSeq);
                        }
                        if (state.concurrentAttemptInconsistent()) {
                            reset();
                            return false;
                        }
                    }
                }
                assertTrue(state, remaining == 0, "remaining == 0");
            }
            return true;
        }

        private void assertTrue(State state, boolean condition, String message) {
            if (!condition) {
                reset();
                // We are really hoping this throws an error.
                state.failIfConcurrentAttemptInconsistent();
                // This is bad.
                throw new Error("Found broken assertion not due to inconsistent attempt: " + message);
            }
        }

        private void reset() {
            SafeCloseable.closeAll(outstandingChunks.stream().flatMap(Stream::of));
            outstandingChunks.clear();
        }
    }
}
