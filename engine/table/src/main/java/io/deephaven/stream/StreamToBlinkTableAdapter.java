/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.stream;

import gnu.trove.list.array.TLongArrayList;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.table.impl.sources.chunkcolumnsource.ChunkColumnSource;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.MultiException;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;

import java.lang.ref.WeakReference;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Adapter for converting streams of data into columnar Deephaven {@link Table tables} that conform to
 * {@link Table#BLINK_TABLE_ATTRIBUTE blink table} semantics.
 *
 * @implNote The constructor publishes {@code this} to the {@link PeriodicUpdateGraph} and thus cannot be subclassed.
 */
public class StreamToBlinkTableAdapter
        implements StreamConsumer, Runnable, NotificationQueue.Dependency, SafeCloseable {

    private static final Logger log = LoggerFactory.getLogger(StreamToBlinkTableAdapter.class);

    private static final int CHUNK_SIZE = ArrayBackedColumnSource.BLOCK_SIZE;

    private final TableDefinition tableDefinition;
    private final UpdateSourceRegistrar updateSourceRegistrar;
    private final String name;

    private final WeakReference<QueryTable> tableRef;
    private final TrackingWritableRowSet rowSet;
    private final SwitchColumnSource<?>[] switchSources;

    /** To start out when we have no data, we use null value column sources which are cheap and singletons. */
    private final NullValueColumnSource<?>[] nullColumnSources;

    /**
     * The {@link StreamPublisher} for this {@link StreamConsumer}. Need not be volatile or guarded by a lock, since we
     * are sure to assign <em>before</em> becoming a source with our {@link #updateSourceRegistrar}.
     */
    private StreamPublisher publisher;

    // We accumulate data into buffer from the ingester thread; capture it into current on the UGP thread; move it into
    // prev after one cycle, and then the cycle after that we clear out the chunks and reuse them for the buffers.
    // They all start out null in the constructor.
    private ChunkColumnSource<?>[] bufferChunkSources;
    private ChunkColumnSource<?>[] currentChunkSources;
    private ChunkColumnSource<?>[] prevChunkSources;

    /** A list of failures that have occurred. */
    private List<Throwable> enqueuedFailures;

    private volatile QueryTable table;

    private final AtomicBoolean alive = new AtomicBoolean(true);

    public StreamToBlinkTableAdapter(
            @NotNull final TableDefinition tableDefinition,
            @NotNull final UpdateSourceRegistrar updateSourceRegistrar,
            @NotNull final String name) {
        this(tableDefinition, streamPublisher, updateSourceRegistrar, name, Map.of());
    }

    public StreamToBlinkTableAdapter(
            @NotNull final TableDefinition tableDefinition,
            @NotNull final StreamPublisher streamPublisher,
            @NotNull final UpdateSourceRegistrar updateSourceRegistrar,
            @NotNull final String name,
            @NotNull final Map<String, Object> extraAttributes) {
        this.tableDefinition = tableDefinition;
        this.updateSourceRegistrar = updateSourceRegistrar;
        this.name = name;

        nullColumnSources = makeNullColumnSources(tableDefinition);

        final LinkedHashMap<String, ColumnSource<?>> visibleSources = new LinkedHashMap<>();
        switchSources = makeSwitchSources(tableDefinition, nullColumnSources, visibleSources);

        rowSet = RowSetFactory.empty().toTracking();

        table = new QueryTable(rowSet, visibleSources) {
            {
                setFlat();
                setRefreshing(true);
                setAttribute(Table.BLINK_TABLE_ATTRIBUTE, Boolean.TRUE);
                for (Entry<String, Object> e : extraAttributes.entrySet()) {
                    setAttribute(e.getKey(), e.getValue());
                }
                addParentReference(StreamToBlinkTableAdapter.this);
            }

            @Override
            public void destroy() {
                StreamToBlinkTableAdapter.this.close();
            }
        };
        tableRef = new WeakReference<>(table);
    }

    /**
     * Create an array of chunks suitable for passing to our accept method.
     *
     * @param size the size of the chunks
     * @return an array of writable chunks
     */
    public WritableChunk<?>[] makeChunksForDefinition(int size) {
        return StreamChunkUtils.makeChunksForDefinition(tableDefinition, size);
    }

    @NotNull
    private static ChunkColumnSource<?>[] makeChunkSources(TableDefinition tableDefinition) {
        final TLongArrayList offsets = new TLongArrayList();
        return tableDefinition.getColumnStream().map(cd -> makeChunkSourceForColumn(offsets, cd))
                .toArray(ChunkColumnSource[]::new);
    }

    @NotNull
    private static ChunkColumnSource<?> makeChunkSourceForColumn(TLongArrayList offsets, ColumnDefinition<?> cd) {
        final Class<?> replacementType = StreamChunkUtils.replacementType(cd.getDataType());
        if (replacementType != null) {
            return ChunkColumnSource.make(ChunkType.fromElementType(replacementType), replacementType, null, offsets);
        } else {
            return ChunkColumnSource.make(ChunkType.fromElementType(cd.getDataType()), cd.getDataType(),
                    cd.getComponentType(), offsets);
        }
    }

    @Override
    public void register(@NotNull final StreamPublisher publisher) {
        if (this.publisher != null) {
            throw new IllegalStateException(String.format(
                    "Can not register multiple stream publisher: %s already registered, attempted to re-register %s",
                    this.publisher, publisher));
        }

        log.info().append("Registering ").append(StreamToBlinkTableAdapter.class.getSimpleName()).append('-')
                .append(name)
                .endl();

        this.publisher = publisher;
        publisher.register(this);

        updateSourceRegistrar.addSource(this);
    }

    @Override
    public ChunkType chunkType(final int columnIndex) {
        return StreamChunkUtils.chunkTypeForColumnIndex(tableDefinition, columnIndex);
    }

    @Override
    public WritableChunk<Values>[] getChunksToFill() {
        return StreamChunkUtils.makeChunksForDefinition(tableDefinition, CHUNK_SIZE);
    }

    @NotNull
    private static NullValueColumnSource<?>[] makeNullColumnSources(TableDefinition tableDefinition) {
        return tableDefinition.getColumnStream().map(StreamToBlinkTableAdapter::makeNullValueColumnSourceFromDefinition)
                .toArray(NullValueColumnSource<?>[]::new);
    }

    private static NullValueColumnSource<?> makeNullValueColumnSourceFromDefinition(ColumnDefinition<?> cd) {
        final Class<?> replacementType = StreamChunkUtils.replacementType(cd.getDataType());
        if (replacementType != null) {
            return NullValueColumnSource.getInstance(replacementType, null);
        } else {
            return NullValueColumnSource.getInstance(cd.getDataType(), cd.getComponentType());
        }
    }

    @NotNull
    private static SwitchColumnSource<?>[] makeSwitchSources(TableDefinition definition,
            NullValueColumnSource<?>[] wrapped, Map<String, ColumnSource<?>> visibleSourcesMap) {
        final SwitchColumnSource<?>[] switchSources = new SwitchColumnSource[wrapped.length];
        final List<ColumnDefinition<?>> columns = definition.getColumns();
        for (int ii = 0; ii < wrapped.length; ++ii) {
            final ColumnDefinition<?> columnDefinition = columns.get(ii);
            final SwitchColumnSource<?> switchSource =
                    new SwitchColumnSource<>(wrapped[ii], StreamToBlinkTableAdapter::maybeClearChunkColumnSource);

            final ColumnSource<?> visibleSource;
            if (columnDefinition.getDataType() == Instant.class) {
                // noinspection unchecked
                visibleSource = new LongAsInstantColumnSource((ColumnSource<Long>) switchSource);
            } else if (columnDefinition.getDataType() == Boolean.class) {
                // noinspection unchecked
                visibleSource = new ByteAsBooleanColumnSource((ColumnSource<Byte>) switchSource);
            } else {
                visibleSource = switchSource;
            }
            switchSources[ii] = switchSource;
            visibleSourcesMap.put(columnDefinition.getName(), visibleSource);
        }
        return switchSources;
    }

    private static void maybeClearChunkColumnSource(ColumnSource<?> cs) {
        if (cs instanceof ChunkColumnSource) {
            ((ChunkColumnSource<?>) cs).clear();
        }
    }

    /**
     * Return the {@link Table#BLINK_TABLE_ATTRIBUTE blink} {@link Table table} that this adapter is producing, and
     * ensure that this StreamToBlinkTableAdapter no longer enforces strong reachability of the result. May return
     * {@code null} if invoked more than once and the initial caller does not enforce strong reachability of the result.
     *
     * @return The resulting blink table
     */
    public Table table() {
        final QueryTable localTable = tableRef.get();
        table = null;
        return localTable;
    }

    @Override
    public void close() {
        if (alive.compareAndSet(true, false)) {
            log.info().append("Deregistering ").append(StreamToBlinkTableAdapter.class.getSimpleName()).append('-')
                    .append(name)
                    .endl();
            updateSourceRegistrar.removeSource(this);
            publisher.shutdown();
        }
    }

    @Override
    public void run() {
        synchronized (this) {
            // If we have an enqueued failure we want to process it first, before we allow the streamPublisher to flush
            // itself.
            if (enqueuedFailures != null) {
                deliverFailure(MultiException.maybeWrapInMultiException(
                        "Multiple errors encountered while ingesting stream",
                        enqueuedFailures.toArray(new Throwable[0])));
                return;
            }
        }
        final TableUpdate downstream;
        try {
            downstream = doRefresh();
        } catch (Exception e) {
            log.error().append("Error refreshing ").append(StreamToBlinkTableAdapter.class.getSimpleName()).append('-')
                    .append(name).append(": ").append(e).endl();
            deliverFailure(e);
            return;
        }
        deliverUpdate(downstream);
    }

    private void deliverUpdate(final TableUpdate downstream) {
        final QueryTable localTable = tableRef.get();
        if (localTable != null) {
            if (downstream != null) {
                try {
                    localTable.notifyListeners(downstream);
                } catch (Exception e) {
                    // Defer error delivery until the next cycle
                    enqueuedFailures.add(e);
                }
            }
            return;
        }
        // noinspection EmptyTryBlock
        try (final SafeCloseable ignored1 = this;
                final SafeCloseable ignored2 = downstream == null ? null : downstream::release) {
        }
    }

    private void deliverFailure(@NotNull final Throwable failure) {
        try (final SafeCloseable ignored = this) {
            final QueryTable localTable = tableRef.get();
            if (localTable != null) {
                localTable.notifyListenersOnError(failure, null);
            }
        }
    }

    private TableUpdate doRefresh() {
        publisher.flush();
        // Switch columns, update RowSet, deliver notification

        final long oldSize = rowSet.size();
        final long newSize;

        final ChunkColumnSource<?>[] capturedBufferSources;
        synchronized (this) {
            newSize = bufferChunkSources == null ? 0 : bufferChunkSources[0].getSize();

            if (oldSize == 0 && newSize == 0) {
                return null;
            }

            capturedBufferSources = bufferChunkSources;
            bufferChunkSources = prevChunkSources;
        }

        if (capturedBufferSources == null) {
            // null out our current values
            for (int ii = 0; ii < switchSources.length; ++ii) {
                // noinspection unchecked,rawtypes
                switchSources[ii].setNewCurrent((ColumnSource) nullColumnSources[ii]);
            }
        } else {
            for (int ii = 0; ii < switchSources.length; ++ii) {
                // noinspection unchecked,rawtypes
                switchSources[ii].setNewCurrent((ColumnSource) capturedBufferSources[ii]);
            }
        }

        prevChunkSources = currentChunkSources;
        currentChunkSources = capturedBufferSources;

        if (oldSize < newSize) {
            rowSet.insertRange(oldSize, newSize - 1);
        } else if (oldSize > newSize) {
            rowSet.removeRange(newSize, oldSize - 1);
        }

        return new TableUpdateImpl(
                RowSetFactory.flat(newSize),
                RowSetFactory.flat(oldSize),
                RowSetFactory.empty(),
                RowSetShiftData.EMPTY,
                ModifiedColumnSet.EMPTY);
    }

    @SafeVarargs
    @Override
    public final void accept(@NotNull final WritableChunk<Values>... data) {
        if (!alive.get()) {
            return;
        }
        // Accumulate data into buffered column sources
        synchronized (this) {
            if (enqueuedFailures != null) {
                // If we'll never deliver these chunks, dispose of them immediately.
                SafeCloseable.closeAll(data);
                return;
            }
            if (bufferChunkSources == null) {
                bufferChunkSources = makeChunkSources(tableDefinition);
            }
            if (data.length != bufferChunkSources.length) {
                throw new IllegalStateException("StreamConsumer data length = " + data.length + " chunks, expected "
                        + bufferChunkSources.length);
            }
            for (int ii = 0; ii < data.length; ++ii) {
                Assert.eq(data[0].size(), "data[0].size()", data[ii].size(), "data[ii].size()");
                bufferChunkSources[ii].addChunk(data[ii]);
            }
        }
    }

    @Override
    public void acceptFailure(@NotNull Throwable cause) {
        if (!alive.get()) {
            return;
        }
        synchronized (this) {
            if (enqueuedFailures == null) {
                enqueuedFailures = new ArrayList<>();
            }
            enqueuedFailures.add(cause);
        }
        // Defer closing until the error has been delivered
    }

    @Override
    public boolean satisfied(final long step) {
        return updateSourceRegistrar.satisfied(step);
    }

    @Override
    public UpdateGraph getUpdateGraph() {
        return updateSourceRegistrar.getUpdateGraph();
    }
}
