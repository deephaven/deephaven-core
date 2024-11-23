//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.stream;

import gnu.trove.list.array.TLongArrayList;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.sources.ByteAsBooleanColumnSource;
import io.deephaven.engine.table.impl.sources.LongAsInstantColumnSource;
import io.deephaven.engine.table.impl.sources.NullValueColumnSource;
import io.deephaven.engine.table.impl.sources.SwitchColumnSource;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.table.impl.sources.chunkcolumnsource.ChunkColumnSource;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.MultiException;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import java.lang.ref.WeakReference;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

/**
 * Adapter for converting streams of data into columnar Deephaven {@link Table tables} that conform to
 * {@link Table#BLINK_TABLE_ATTRIBUTE blink table} semantics.
 *
 * @implNote The constructor publishes {@code this} to an {@link UpdateSourceRegistrar} and thus cannot be subclassed.
 */
public class StreamToBlinkTableAdapter
        implements StreamConsumer, Runnable, NotificationQueue.Dependency, SafeCloseable {

    private static final Logger log = LoggerFactory.getLogger(StreamToBlinkTableAdapter.class);

    private final TableDefinition tableDefinition;
    private final StreamPublisher streamPublisher;
    private final UpdateSourceRegistrar updateSourceRegistrar;
    private final String name;

    private final WeakReference<QueryTable> tableRef;
    private final TrackingWritableRowSet rowSet;
    private final SwitchColumnSource<?>[] switchSources;

    /** To start out when we have no data, we use null value column sources which are cheap and singletons. */
    private final NullValueColumnSource<?>[] nullColumnSources;

    // We accumulate data into buffer from the ingester thread; capture it into current on the UGP thread; move it into
    // prev after one cycle, and then the cycle after that we clear out the chunks and reuse them for the buffers.
    // They all start out null in the constructor.
    private ChunkColumnSource<?>[] bufferChunkSources;
    private ChunkColumnSource<?>[] currentChunkSources;
    private ChunkColumnSource<?>[] prevChunkSources;

    /**
     * A list of failures that have occurred. Access should be synchronized on {@code this}.
     */
    private final List<Throwable> enqueuedFailures = new ArrayList<>();

    private volatile QueryTable table;

    private final AtomicBoolean alive = new AtomicBoolean(true);
    private final AtomicBoolean initialized = new AtomicBoolean(false);

    /**
     * Construct the adapter with {@code initialize == true} and without extra attributes.
     *
     * <p>
     * Equivalent to
     * {@code new StreamToBlinkTableAdapter(tableDefinition, streamPublisher, updateSourceRegistrar, name, Map.of(), true)}.
     *
     * @param tableDefinition the table definition
     * @param streamPublisher the stream publisher
     * @param updateSourceRegistrar the update source registrar
     * @param name the name
     */
    public StreamToBlinkTableAdapter(
            @NotNull final TableDefinition tableDefinition,
            @NotNull final StreamPublisher streamPublisher,
            @NotNull final UpdateSourceRegistrar updateSourceRegistrar,
            @NotNull final String name) {
        this(tableDefinition, streamPublisher, updateSourceRegistrar, name, Map.of(), true);
    }

    /**
     * Construct the adapter with {@code initialize == true}.
     *
     * <p>
     * Equivalent to
     * {@code new StreamToBlinkTableAdapter(tableDefinition, streamPublisher, updateSourceRegistrar, name, extraAttributes, true)}.
     *
     * @param tableDefinition the table definition
     * @param streamPublisher the stream publisher
     * @param updateSourceRegistrar the update source registrar
     * @param name the name
     * @param extraAttributes the extra attributes to set on the resulting table
     */
    public StreamToBlinkTableAdapter(
            @NotNull final TableDefinition tableDefinition,
            @NotNull final StreamPublisher streamPublisher,
            @NotNull final UpdateSourceRegistrar updateSourceRegistrar,
            @NotNull final String name,
            @NotNull final Map<String, Object> extraAttributes) {
        this(tableDefinition, streamPublisher, updateSourceRegistrar, name, extraAttributes, true);
    }

    /**
     * Construct the adapter.
     *
     * @param tableDefinition the table definition
     * @param streamPublisher the stream publisher
     * @param updateSourceRegistrar the update source registrar
     * @param name the name
     * @param extraAttributes the extra attributes to set on the resulting table
     * @param initialize if the constructor should invoke {@link #initialize()}; if {@code false}, the caller is
     *        responsible for invoking {@link #initialize()}.
     */
    public StreamToBlinkTableAdapter(
            @NotNull final TableDefinition tableDefinition,
            @NotNull final StreamPublisher streamPublisher,
            @NotNull final UpdateSourceRegistrar updateSourceRegistrar,
            @NotNull final String name,
            @NotNull final Map<String, Object> extraAttributes,
            boolean initialize) {
        this.tableDefinition = tableDefinition;
        this.streamPublisher = streamPublisher;
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
                // Ensure that the UpdateSourceRegistrar remains alive while the blink table does.
                if (updateSourceRegistrar instanceof LivenessReferent) {
                    manage((LivenessReferent) updateSourceRegistrar);
                }
            }

            @OverridingMethodsMustInvokeSuper
            @Override
            public void destroy() {
                super.destroy();
                StreamToBlinkTableAdapter.this.close();
            }
        };
        tableRef = new WeakReference<>(table);
        if (initialize) {
            initialize();
        }
    }

    /**
     * Initialize this adapter by invoking {@link StreamPublisher#register(StreamConsumer)} and
     * {@link UpdateSourceRegistrar#addSource(Runnable)} with {@code this}. Must be called once <b>if and only if</b>
     * {@code this} was constructed with {@code initialize == false}.
     *
     * @see #StreamToBlinkTableAdapter(TableDefinition, StreamPublisher, UpdateSourceRegistrar, String, Map, boolean)
     */
    public void initialize() {
        if (!initialized.compareAndSet(false, true)) {
            throw new IllegalStateException("Must not call StreamToBlinkTableAdapter#initialize more than once");
        }
        log.info().append("Registering ").append(StreamToBlinkTableAdapter.class.getSimpleName()).append('-')
                .append(name)
                .endl();
        streamPublisher.register(this);
        updateSourceRegistrar.addSource(this);
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

    private synchronized void clearChunkColumnSources() {
        SafeCloseable.closeAll(
                Stream.of(bufferChunkSources, currentChunkSources, prevChunkSources)
                        .filter(Objects::nonNull)
                        .flatMap(Arrays::stream)
                        .map(ccs -> ccs::clear));
        bufferChunkSources = currentChunkSources = prevChunkSources = null;
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

    /**
     * Checks whether {@code this} is alive; if {@code false}, the publisher should stop publishing new data and release
     * any related resources as soon as practicable since publishing won't have any downstream effects.
     *
     * <p>
     * Once this is {@code false}, it will always remain {@code false}. For more prompt notifications, publishers may
     * prefer to respond to {@link StreamPublisher#shutdown()}.
     *
     * @return if this is alive
     */
    public boolean isAlive() {
        return alive.get();
    }

    @Override
    public void close() {
        if (alive.compareAndSet(true, false)) {
            log.info().append("Deregistering ").append(StreamToBlinkTableAdapter.class.getSimpleName()).append('-')
                    .append(name)
                    .endl();
            updateSourceRegistrar.removeSource(this);
            streamPublisher.shutdown();
            getUpdateGraph().runWhenIdle(this::clearChunkColumnSources);
        }
    }

    @Override
    public void run() {
        // If we have an enqueued failure we want to process it first, before we allow the streamPublisher to flush
        // itself.
        if (deliverFailures()) {
            return;
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
        if (downstream == null) {
            return;
        }
        deliverUpdate(downstream);
    }

    private synchronized boolean deliverFailures() {
        if (enqueuedFailures.isEmpty()) {
            return false;
        }
        deliverFailure(MultiException.maybeWrapInMultiException(
                "Multiple errors encountered while ingesting stream",
                enqueuedFailures));
        return true;
    }

    private void deliverUpdate(@Nullable final TableUpdate downstream) {
        final QueryTable localTable = tableRef.get();
        if (localTable != null) {
            if (downstream != null) {
                try {
                    localTable.notifyListeners(downstream);
                } catch (Exception e) {
                    // Defer error delivery until the next cycle
                    enqueueFailure(e);
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
        streamPublisher.flush();
        // Switch columns, update RowSet, deliver notification

        final long oldSize = rowSet.size();
        final long newSize;

        final ChunkColumnSource<?>[] capturedBufferSources;
        synchronized (this) {
            // streamPublisher.flush() may have called acceptFailure
            if (deliverFailures()) {
                return null;
            }
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
        accept(List.<WritableChunk<Values>[]>of(data));
    }

    /**
     * {@inheritDoc}
     *
     * <p>
     * Ensures that the blink table sees the full collection of chunks in a single cycle.
     *
     * @param data A collection of per-column {@link WritableChunk chunks} of {@link Values values}. All chunks in each
     *        element must have the same {@link WritableChunk#size() size}, but different elements may have differing
     *        chunk sizes.
     */
    @Override
    public final void accept(@NotNull Collection<WritableChunk<Values>[]> data) {
        if (!alive.get()) {
            // If we'll never deliver these chunks, dispose of them immediately.
            SafeCloseable.closeAll(data.stream().flatMap(Stream::of));
            return;
        }
        // Accumulate data into buffered column sources
        synchronized (this) {
            if (!enqueuedFailures.isEmpty()) {
                // If we'll never deliver these chunks, dispose of them immediately.
                SafeCloseable.closeAll(data.stream().flatMap(Stream::of));
                return;
            }
            if (bufferChunkSources == null) {
                bufferChunkSources = makeChunkSources(tableDefinition);
            }
            for (WritableChunk<Values>[] chunks : data) {
                if (chunks.length != bufferChunkSources.length) {
                    throw new IllegalStateException(
                            "StreamConsumer data length = " + chunks.length + " chunks, expected "
                                    + bufferChunkSources.length);
                }
                for (int ii = 0; ii < chunks.length; ++ii) {
                    Assert.eq(chunks[0].size(), "data[0].size()", chunks[ii].size(), "data[ii].size()");
                    bufferChunkSources[ii].addChunk(chunks[ii]);
                }
            }
        }
    }

    @Override
    public void acceptFailure(@NotNull final Throwable cause) {
        if (!alive.get()) {
            return;
        }
        enqueueFailure(cause);
        // Defer closing until the error has been delivered
    }

    private void enqueueFailure(@NotNull final Throwable cause) {
        synchronized (this) {
            enqueuedFailures.add(cause);
        }
    }

    @Override
    public boolean satisfied(final long step) {
        return updateSourceRegistrar.satisfied(step);
    }

    @Override
    public UpdateGraph getUpdateGraph() {
        return updateSourceRegistrar.getUpdateGraph();
    }

    @Override
    public LogOutput append(@NotNull final LogOutput logOutput) {
        return logOutput.append("StreamToBlinkTableAdapter[").append(name).append(']');
    }
}
