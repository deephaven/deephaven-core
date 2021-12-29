package io.deephaven.stream;

import gnu.trove.list.array.TLongArrayList;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.time.DateTime;
import io.deephaven.engine.table.ModifiedColumnSet;
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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Adapter for converting streams of data into columnar Deephaven {@link Table tables}.
 */
public class StreamToTableAdapter implements SafeCloseable, StreamConsumer, Runnable {

    private static final Logger log = LoggerFactory.getLogger(StreamToTableAdapter.class);

    private final TableDefinition tableDefinition;
    private final StreamPublisher streamPublisher;
    private final UpdateSourceRegistrar updateSourceRegistrar;
    private final String name;

    private final QueryTable table;
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

    /** A list of failures that have occurred. */
    private List<Exception> enqueuedFailure;

    private volatile Runnable shutdownCallback;
    private volatile boolean alive = true;

    public StreamToTableAdapter(@NotNull final TableDefinition tableDefinition,
            @NotNull final StreamPublisher streamPublisher,
            @NotNull final UpdateSourceRegistrar updateSourceRegistrar,
            @NotNull final String name) {
        this.tableDefinition = tableDefinition;
        this.streamPublisher = streamPublisher;
        this.updateSourceRegistrar = updateSourceRegistrar;
        this.name = name;
        streamPublisher.register(this);
        log.info().append("Registering ").append(StreamToTableAdapter.class.getSimpleName()).append('-').append(name)
                .endl();
        updateSourceRegistrar.addSource(this);

        nullColumnSources = makeNullColumnSources(tableDefinition);

        final LinkedHashMap<String, ColumnSource<?>> visibleSources = new LinkedHashMap<>();
        switchSources = makeSwitchSources(tableDefinition, nullColumnSources, visibleSources);

        rowSet = RowSetFactory.empty().toTracking();

        table = new QueryTable(rowSet, visibleSources) {
            {
                setFlat();
                setRefreshing(true);
                setAttribute(Table.STREAM_TABLE_ATTRIBUTE, Boolean.TRUE);
                addParentReference(StreamToTableAdapter.this);
            }

            @Override
            public void destroy() {
                StreamToTableAdapter.this.close();
            }
        };
    }

    /**
     * Set a callback to be invoked when this StreamToTableAdapter will no longer deliver new data to downstream
     * consumers.
     *
     * @param shutdownCallback The callback
     */
    public void setShutdownCallback(Runnable shutdownCallback) {
        this.shutdownCallback = shutdownCallback;
    }

    /**
     * Create an array of chunks suitable for passing to our accept method.
     *
     * @param size the size of the chunks
     * @return an array of writable chunks
     */
    public WritableChunk[] makeChunksForDefinition(int size) {
        return makeChunksForDefinition(tableDefinition, size);
    }

    /**
     * Return the ChunkType for a given column index.
     *
     * @param idx the column index to get the ChunkType for
     * @return the ChunkType for the specified column
     */
    public ChunkType chunkTypeForIndex(int idx) {
        return chunkTypeForColumn(tableDefinition.getColumns()[idx]);
    }

    /**
     * Make output chunks for the specified table definition.
     *
     * @param definition the definition to make chunks for
     * @param size the size of the returned chunks
     * @return an array of writable chunks
     */
    public static WritableChunk[] makeChunksForDefinition(TableDefinition definition, int size) {
        return definition.getColumnStream().map(cd -> makeChunk(cd, size)).toArray(WritableChunk[]::new);
    }

    @NotNull
    private static ChunkColumnSource<?>[] makeChunkSources(TableDefinition tableDefinition) {
        final TLongArrayList offsets = new TLongArrayList();
        return tableDefinition.getColumnStream().map(cd -> makeChunkSourceForColumn(offsets, cd))
                .toArray(ChunkColumnSource[]::new);
    }

    @NotNull
    private static ChunkColumnSource<?> makeChunkSourceForColumn(TLongArrayList offsets, ColumnDefinition<?> cd) {
        final Class<?> replacementType = replacementType(cd.getDataType());
        if (replacementType != null) {
            return ChunkColumnSource.make(ChunkType.fromElementType(replacementType), replacementType, null, offsets);
        } else {
            return ChunkColumnSource.make(ChunkType.fromElementType(cd.getDataType()), cd.getDataType(),
                    cd.getComponentType(), offsets);
        }
    }

    @NotNull
    private static WritableChunk<?> makeChunk(ColumnDefinition<?> cd, int size) {
        final ChunkType chunkType = chunkTypeForColumn(cd);
        WritableChunk<Any> returnValue = chunkType.makeWritableChunk(size);
        returnValue.setSize(0);
        return returnValue;
    }

    private static ChunkType chunkTypeForColumn(ColumnDefinition<?> cd) {
        final Class<?> replacementType = replacementType(cd.getDataType());
        final Class<?> useType = replacementType != null ? replacementType : cd.getDataType();
        final ChunkType chunkType = ChunkType.fromElementType(useType);
        return chunkType;
    }

    @NotNull
    private static NullValueColumnSource<?>[] makeNullColumnSources(TableDefinition tableDefinition) {
        return tableDefinition.getColumnStream().map(StreamToTableAdapter::makeNullValueColumnSourceFromDefinition)
                .toArray(NullValueColumnSource<?>[]::new);
    }

    private static NullValueColumnSource<?> makeNullValueColumnSourceFromDefinition(ColumnDefinition<?> cd) {
        final Class<?> replacementType = replacementType(cd.getDataType());
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
        final ColumnDefinition<?>[] columns = definition.getColumns();
        for (int ii = 0; ii < wrapped.length; ++ii) {
            final SwitchColumnSource<?> switchSource =
                    new SwitchColumnSource<>(wrapped[ii], StreamToTableAdapter::maybeClearChunkColumnSource);

            final ColumnSource<?> visibleSource;
            if (columns[ii].getDataType() == DateTime.class) {
                // noinspection unchecked
                visibleSource = new LongAsDateTimeColumnSource((ColumnSource<Long>) switchSource);
            } else if (columns[ii].getDataType() == Boolean.class) {
                // noinspection unchecked
                visibleSource = new ByteAsBooleanColumnSource((ColumnSource<Byte>) switchSource);
            } else {
                visibleSource = switchSource;
            }
            switchSources[ii] = switchSource;
            visibleSourcesMap.put(columns[ii].getName(), visibleSource);
        }
        return switchSources;
    }

    private static void maybeClearChunkColumnSource(ColumnSource<?> cs) {
        if (cs instanceof ChunkColumnSource) {
            ((ChunkColumnSource<?>) cs).clear();
        }
    }

    /**
     * We change the inner columns to long and byte for DateTime and Boolean, respectively. We expect our ingesters to
     * pass us these primitive chunks for those types.
     *
     * @param columnType the type of the outer column
     *
     * @return the type of the inner column
     */
    private static Class<?> replacementType(Class<?> columnType) {
        if (columnType == DateTime.class) {
            return long.class;
        } else if (columnType == Boolean.class) {
            return byte.class;
        } else {
            return null;
        }
    }

    /**
     * Return the stream table that this adapter is producing.
     *
     * @return the resultant stream table
     */
    public Table table() {
        return table;
    }

    @Override
    public void close() {
        if (!alive) {
            return;
        }
        synchronized (this) {
            if (!alive) {
                return;
            }
            alive = false;
            log.info().append("Deregistering ").append(StreamToTableAdapter.class.getSimpleName()).append('-')
                    .append(name)
                    .endl();
            updateSourceRegistrar.removeSource(this);
            final Runnable localShutdownCallback = shutdownCallback;
            if (localShutdownCallback != null) {
                localShutdownCallback.run();
            }
        }
    }

    @Override
    public void run() {
        try {
            doRefresh();
        } catch (Exception e) {
            log.error().append("Error refreshing ").append(StreamToTableAdapter.class.getSimpleName()).append('-')
                    .append(name).append(": ").append(e).endl();
            table.notifyListenersOnError(e, null);
            updateSourceRegistrar.removeSource(this);
        }
    }

    private void doRefresh() {
        synchronized (this) {
            // if we have an enqueued failure we want to process it first, before we allow the streamPublisher to flush
            // itself
            if (enqueuedFailure != null) {
                throw new UncheckedDeephavenException(
                        MultiException.maybeWrapInMultiException(
                                "Multiple errors encountered while ingesting stream",
                                enqueuedFailure.toArray(new Exception[0])));
            }
        }

        streamPublisher.flush();
        // Switch columns, update RowSet, deliver notification

        final long oldSize = rowSet.size();
        final long newSize;

        final ChunkColumnSource<?>[] capturedBufferSources;
        synchronized (this) {
            newSize = bufferChunkSources == null ? 0 : bufferChunkSources[0].getSize();

            if (oldSize == 0 && newSize == 0) {
                return;
            }

            capturedBufferSources = bufferChunkSources;
            bufferChunkSources = prevChunkSources;
        }

        if (capturedBufferSources == null) {
            // null out our current values
            for (int ii = 0; ii < switchSources.length; ++ii) {
                switchSources[ii].setNewCurrent((ColumnSource) nullColumnSources[ii]);
            }
        } else {
            for (int ii = 0; ii < switchSources.length; ++ii) {
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

        table.notifyListeners(new TableUpdateImpl(RowSetFactory.flat(newSize),
                RowSetFactory.flat(oldSize), RowSetFactory.empty(),
                RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY));
    }

    @SafeVarargs
    @Override
    public final void accept(@NotNull final WritableChunk<Values>... data) {
        if (!alive) {
            return;
        }
        // Accumulate data into buffered column sources
        synchronized (this) {
            if (bufferChunkSources == null) {
                bufferChunkSources = makeChunkSources(tableDefinition);
            }
            if (data.length != bufferChunkSources.length) {
                // TODO: Our error handling should be better when in the ingester thread; since it seems proper to kill
                // the ingester, and also notify downstream tables
                // https://github.com/deephaven/deephaven-core/issues/934
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
    public void acceptFailure(@NotNull Exception cause) {
        if (!alive) {
            return;
        }
        synchronized (this) {
            if (enqueuedFailure == null) {
                enqueuedFailure = new ArrayList<>();
            }
            enqueuedFailure.add(cause);
        }
        close();
    }
}
