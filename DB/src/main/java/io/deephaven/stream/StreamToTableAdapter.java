package io.deephaven.stream;

import gnu.trove.list.array.TLongArrayList;
import io.deephaven.DeephavenException;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.verify.Assert;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.live.LiveTable;
import io.deephaven.db.tables.live.LiveTableRegistrar;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.v2.DynamicTable;
import io.deephaven.db.v2.ModifiedColumnSet;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.db.v2.ShiftAwareListener;
import io.deephaven.db.v2.sources.*;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.ChunkType;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.db.v2.sources.chunkcolumnsource.ChunkColumnSource;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.IndexShiftData;
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
public class StreamToTableAdapter implements SafeCloseable, LiveTable, StreamConsumer {

    private final TableDefinition tableDefinition;
    private final StreamPublisher streamPublisher;
    private final LiveTableRegistrar liveTableRegistrar;

    private final QueryTable table;
    private final Index index;
    private final SwitchColumnSource<?>[] switchSources;

    /** To start out when we have no data, we use null value column sources which are cheap and singletons. */
    private final NullValueColumnSource<?>[] nullColumnSources;

    // we accumulate data into buffer from the ingester thread; capture it into current on the LTM thread; move it into
    // prev after one cycle, and then then the cycle after that we clear out the chunks and reuse them for the buffers
    // they all start out null in the constructor
    private ChunkColumnSource<?>[] bufferChunkSources;
    private ChunkColumnSource<?>[] currentChunkSources;
    private ChunkColumnSource<?>[] prevChunkSources;

    // a list of failures that have occurred
    private List<Exception> enqueuedFailure;

    public StreamToTableAdapter(@NotNull final TableDefinition tableDefinition,
            @NotNull final StreamPublisher streamPublisher,
            @NotNull final LiveTableRegistrar liveTableRegistrar) {
        this.tableDefinition = tableDefinition;
        this.streamPublisher = streamPublisher;
        this.liveTableRegistrar = liveTableRegistrar;
        streamPublisher.register(this);
        liveTableRegistrar.addTable(this);

        nullColumnSources = makeNullColumnSources(tableDefinition);

        final LinkedHashMap<String, ColumnSource<?>> visibleSources = new LinkedHashMap<>();
        switchSources = makeSwitchSources(tableDefinition, nullColumnSources, visibleSources);

        index = Index.FACTORY.getEmptyIndex();

        table = new QueryTable(index, visibleSources) {
            {
                setFlat();
                setRefreshing(true);
                setAttribute(Table.STREAM_TABLE_ATTRIBUTE, Boolean.TRUE);
            }
        };
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
        WritableChunk<Attributes.Any> returnValue = chunkType.makeWritableChunk(size);
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
            if (columns[ii].getDataType() == DBDateTime.class) {
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
     * We change the inner columns to long and byte for DBDateTime and Boolean, respectively. We expect our ingesters to
     * pass us these primitive chunks for those types.
     *
     * @param columnType the type of the outer column
     *
     * @return the type of the inner column
     */
    private static Class<?> replacementType(Class<?> columnType) {
        if (columnType == DBDateTime.class) {
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
    public DynamicTable table() {
        return table;
    }

    @Override
    public void close() {
        liveTableRegistrar.removeTable(this);
    }

    @Override
    public void refresh() {
        try {
            doRefresh();
        } catch (Exception e) {
            table.notifyListenersOnError(e, null);
            liveTableRegistrar.removeTable(this);
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
        // Switch columns, update index, deliver notification

        final long oldSize = index.size();
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
            index.insertRange(oldSize, newSize - 1);
        } else if (oldSize > newSize) {
            index.removeRange(newSize, oldSize - 1);
        }

        table.notifyListeners(new ShiftAwareListener.Update(Index.CURRENT_FACTORY.getFlatIndex(newSize),
                Index.CURRENT_FACTORY.getFlatIndex(oldSize), Index.CURRENT_FACTORY.getEmptyIndex(),
                IndexShiftData.EMPTY, ModifiedColumnSet.EMPTY));
    }

    @SafeVarargs
    @Override
    public final void accept(@NotNull final WritableChunk<Attributes.Values>... data) {
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
        synchronized (this) {
            if (enqueuedFailure == null) {
                enqueuedFailure = new ArrayList<>();
            }
            enqueuedFailure.add(cause);
        }
    }
}
