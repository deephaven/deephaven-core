package io.deephaven.stream;

import gnu.trove.list.array.TLongArrayList;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.live.LiveTable;
import io.deephaven.db.tables.live.LiveTableRegistrar;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.v2.ModifiedColumnSet;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.db.v2.ShiftAwareListener;
import io.deephaven.db.v2.sources.ByteAsBooleanColumnSource;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.LongAsDateTimeColumnSource;
import io.deephaven.db.v2.sources.SwitchColumnSource;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.ChunkType;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.db.v2.sources.chunkcolumnsource.CharChunkColumnSource;
import io.deephaven.db.v2.sources.chunkcolumnsource.ChunkColumnSource;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.IndexShiftData;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;

import java.util.LinkedHashMap;
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
    private ChunkColumnSource<?> [] bufferChunkSources;
    private ChunkColumnSource<?> [] currentChunkSources;
    private ChunkColumnSource<?> [] prevChunkSources;
    private final SwitchColumnSource<?> [] switchSources;
    private final ChunkType [] chunkTypes;
    private final Class<?>[] columnTypesArray;
    private final String[] columnNamesArray;

    public StreamToTableAdapter(@NotNull final TableDefinition tableDefinition,
                                @NotNull final StreamPublisher streamPublisher,
                                @NotNull final LiveTableRegistrar liveTableRegistrar) {
        this.tableDefinition = tableDefinition;
        this.streamPublisher = streamPublisher;
        this.liveTableRegistrar = liveTableRegistrar;
        streamPublisher.register(this);
        liveTableRegistrar.addTable(this);

        columnTypesArray = tableDefinition.getColumnTypesArray();
        columnNamesArray = tableDefinition.getColumnNamesArray();

        chunkTypes = tableDefinitionToChunkTypes(columnTypesArray);
        currentChunkSources = makeChunkSources(columnTypesArray, columnNamesArray);
        final LinkedHashMap<String, ColumnSource<?>> visibleSources = new LinkedHashMap<>();
        switchSources = makeSwitchSources(currentChunkSources, visibleSources);

        bufferChunkSources = makeChunkSources(columnTypesArray, columnNamesArray);

        index = Index.FACTORY.getEmptyIndex();

        table = new QueryTable(index, visibleSources);
    }

    @NotNull
    private ChunkColumnSource<?> [] makeChunkSources(Class<?>[] columnTypesArray, String[] columnNamesArray) {
        final ChunkColumnSource<?> [] sources = new ChunkColumnSource[columnNamesArray.length];
        final TLongArrayList offsets = new TLongArrayList();
        for (int ii = 0; ii < columnTypesArray.length; ++ii) {
            sources[ii]  = ChunkColumnSource.make(chunkTypes[ii], columnTypesArray[ii], offsets);
        }
        return sources;
    }

    @NotNull
    private SwitchColumnSource [] makeSwitchSources(ChunkColumnSource<?> [] wrapped, Map<String, ColumnSource<?>> visibleSourcesMap) {
        final SwitchColumnSource [] switchSources = new SwitchColumnSource[wrapped.length];
        final Map<String, SwitchColumnSource<?>> result = new LinkedHashMap<>();
        for (int ii = 0; ii < wrapped.length; ++ii) {
            final SwitchColumnSource<?> switchSource = new SwitchColumnSource<>(wrapped[ii]);
            final ColumnSource<?> visibleSource;
            if (columnTypesArray[ii] == DBDateTime.class) {
                visibleSource = new LongAsDateTimeColumnSource((ColumnSource<Long>)switchSource);
            } else if (columnTypesArray[ii] == Boolean.class) {
                visibleSource = new ByteAsBooleanColumnSource((ColumnSource<Byte>)switchSource);
            } else {
                visibleSource = switchSource;
            }
            switchSources[ii] = switchSource;
            visibleSourcesMap.put(columnNamesArray[ii], visibleSource);
        }
        return switchSources;
    }

    @NotNull
    private static ChunkType[] tableDefinitionToChunkTypes(Class<?>[] columnTypesArray) {
        final ChunkType [] result = new ChunkType[columnTypesArray.length];
        for (int ii = 0; ii < columnTypesArray.length; ++ii) {
            if (columnTypesArray[ii] == DBDateTime.class) {
                result[ii] = ChunkType.Long;
            }
            else if (columnTypesArray[ii] == Boolean.class) {
                result[ii] = ChunkType.Byte;
            } else {
                result[ii] = ChunkType.fromElementType(columnTypesArray[ii]);
            }
        }
        return result;
    }

    public Table table() {
        return table;
    }

    @Override
    public void close() {
        liveTableRegistrar.removeTable(this);
    }

    @Override
    public void refresh() {
        streamPublisher.flush();
        // Switch columns, update index, deliver notification

        final long oldSize = index.size();
        final long newSize;

        synchronized (this) {
            newSize = bufferChunkSources[0].getSize();

            ChunkColumnSource<?> [] oldPrev = prevChunkSources;

            for (int ii = 0; ii < switchSources.length; ++ii) {
                prevChunkSources[ii].clear();
                //noinspection unchecked
                switchSources[ii].setNewCurrent((ColumnSource)bufferChunkSources[ii]);
            }
            // current becomes prev, buffer becomes current, buffer is set to cleared prev for the next pass
            prevChunkSources = currentChunkSources;
            currentChunkSources = bufferChunkSources;
            bufferChunkSources = oldPrev;

            if (oldSize < newSize) {
                index.insertRange(oldSize, newSize - 1);
            } else if (oldSize > newSize) {
                index.removeRange(newSize, oldSize - 1);
            }
        }

        table.notifyListeners(new ShiftAwareListener.Update(Index.FACTORY.getFlatIndex(oldSize), Index.FACTORY.getFlatIndex(newSize), Index.FACTORY.getEmptyIndex(), IndexShiftData.EMPTY, ModifiedColumnSet.EMPTY));
    }

    @SafeVarargs
    @Override
    public final void accept(@NotNull final WritableChunk<Attributes.Values>... data) {
        // Accumulate data into buffered column sources
        synchronized (this) {
            if (bufferChunkSources == null) {
                bufferChunkSources = makeChunkSources(columnTypesArray, columnNamesArray);
            }
            if (data.length != bufferChunkSources.length) {
                throw new IllegalStateException();
            }
            for (int ii = 0; ii < data.length; ++ii) {
                bufferChunkSources[ii].addChunk(data[ii]);
            }
        }
    }
}
