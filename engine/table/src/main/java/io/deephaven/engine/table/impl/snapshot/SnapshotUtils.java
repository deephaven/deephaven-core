package io.deephaven.engine.table.impl.snapshot;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.chunkfillers.ChunkFiller;
import io.deephaven.engine.table.impl.select.VectorChunkAdapter;
import io.deephaven.engine.table.impl.sources.DelegatingColumnSource;
import io.deephaven.engine.table.impl.sources.SingleValueColumnSource;
import io.deephaven.engine.table.impl.util.ChunkUtils;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.vector.Vector;
import org.jetbrains.annotations.NotNull;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiFunction;

public class SnapshotUtils {
    /**
     * Creates a new columnSourceMap based on 'columns'.
     */
    public static <T> Map<String, T> createColumnSourceMap(Map<String, ? extends ColumnSource<?>> columns,
            BiFunction<Class<?>, Class<?>, T> factory) {
        final Map<String, T> result = new LinkedHashMap<>();
        for (final Map.Entry<String, ? extends ColumnSource<?>> entry : columns.entrySet()) {
            final String key = entry.getKey();
            final ColumnSource<?> cs = entry.getValue();
            final T newColumn = factory.apply(cs.getType(), cs.getComponentType());
            result.put(key, newColumn);
        }
        return result;
    }

    /**
     * For each name in stampColumns: - identify a stamp source (namely 'stampColumns.get(name)') - a row in that stamp
     * source (namely 'stampKey') - a stamp dest (namely the column identified by 'destColumns.get(name)') - a bunch of
     * destination rows (namely all the rows defined in 'destRowSet') Then "spray" that single source value over those
     * destination values.
     *
     * @param stampColumns The stamp columns that serve as the source data
     * @param stampKey The source key
     * @param destColumns The destination columns we are "spraying" to
     * @param destRowSet The keys in destColumns we want to write to
     */
    public static void copyStampColumns(@NotNull Map<String, ? extends ColumnSource<?>> stampColumns, long stampKey,
            @NotNull Map<String, ? extends WritableColumnSource<?>> destColumns, @NotNull RowSet destRowSet) {
        for (Map.Entry<String, ? extends ColumnSource<?>> entry : stampColumns.entrySet()) {
            final String name = entry.getKey();
            final ColumnSource<?> src = entry.getValue();

            // Fill the corresponding destination column
            final WritableColumnSource<?> dest = destColumns.get(name);
            final ChunkFiller destFiller = ChunkFiller.forChunkType(src.getChunkType());
            destFiller.fillFromSingleValue(src, stampKey, dest, destRowSet);
        }
    }

    /**
     * Like the above, but with a singleton destination. For each name in stampColumns: - identify a stamp source
     * (namely 'stampColumns.get(name)') - a row in that stamp source (namely 'stampKey') - a stamp dest (namely the
     * column identified by 'destColumns.get(name)') - a row in the destination (namely 'destKey') Then copy the source
     * value to the destination value.
     *
     * @param stampColumns The stamp columns that serve as the source data
     * @param stampKey The source key
     * @param destColumns The destination columns we are writing to
     */
    public static void copyStampColumns(@NotNull Map<String, ? extends ColumnSource<?>> stampColumns, long stampKey,
            @NotNull Map<String, SingleValueColumnSource<?>> destColumns) {
        for (Map.Entry<String, ? extends ColumnSource<?>> entry : stampColumns.entrySet()) {
            final String name = entry.getKey();
            final ColumnSource<?> src = entry.getValue();

            // Fill the corresponding destination column
            // noinspection rawtypes
            final SingleValueColumnSource dest = destColumns.get(name);
            // noinspection unchecked
            dest.set(src.get(stampKey));
        }
    }

    /**
     * For each name in srcColumns, copy all the data at srcColumns.get(name) (with a range of rows defined by
     * srcRowSet) to a column indicated by destColumns.get(name) (with a range of rows defined by destRowSet).
     *
     * @param srcColumns The stamp columns that serve as the source data
     * @param srcRowSet The keys in the srcColumns we are reading from
     * @param destColumns The destination columns we are writing to
     * @param destRowSet The keys in destColumns we want to write to
     */
    public static void copyDataColumns(@NotNull Map<String, ChunkSource.WithPrev<? extends Values>> srcColumns,
            @NotNull RowSet srcRowSet, @NotNull Map<String, ? extends WritableColumnSource<?>> destColumns,
            @NotNull RowSet destRowSet,
            boolean usePrev) {
        assert srcRowSet.size() == destRowSet.size();
        if (srcRowSet.isEmpty()) {
            return;
        }
        for (Map.Entry<String, ? extends ChunkSource.WithPrev<? extends Values>> entry : srcColumns.entrySet()) {
            final String name = entry.getKey();
            final ChunkSource.WithPrev<? extends Values> srcCs = entry.getValue();

            final WritableColumnSource<?> destCs = destColumns.get(name);
            destCs.ensureCapacity(destRowSet.lastRowKey() + 1);
            ChunkUtils.copyData(srcCs, srcRowSet, destCs, destRowSet, usePrev);
        }
    }

    @NotNull
    public static Map<String, ChunkSource.WithPrev<? extends Values>> generateSnapshotDataColumns(Table table) {
        final Map<String, ? extends ColumnSource<?>> sourceColumns = table.getColumnSourceMap();
        final Map<String, ChunkSource.WithPrev<? extends Values>> snapshotDataColumns =
                new LinkedHashMap<>(sourceColumns.size());

        for (Map.Entry<String, ? extends ColumnSource<?>> entry : sourceColumns.entrySet()) {
            final ColumnSource<?> columnSource = entry.getValue();
            final ChunkSource.WithPrev<? extends Values> maybeTransformed;
            if (Vector.class.isAssignableFrom(columnSource.getType())) {
                maybeTransformed = new VectorChunkAdapter<>(columnSource);
            } else {
                maybeTransformed = columnSource;
            }
            snapshotDataColumns.put(entry.getKey(), maybeTransformed);
        }
        return snapshotDataColumns;
    }

    @NotNull
    public static Map<String, ? extends ColumnSource<?>> generateTriggerStampColumns(Table table) {
        final Map<String, ? extends ColumnSource<?>> stampColumns = table.getColumnSourceMap();
        if (stampColumns.values().stream().noneMatch(cs -> Vector.class.isAssignableFrom(cs.getType()))) {
            return stampColumns;
        }
        final Map<String, ColumnSource<?>> triggerStampColumns = new LinkedHashMap<>(stampColumns.size());
        for (Map.Entry<String, ? extends ColumnSource<?>> entry : stampColumns.entrySet()) {
            triggerStampColumns.put(entry.getKey(), maybeTransformToDirectVectorColumnSource(entry.getValue()));
        }
        return triggerStampColumns;
    }

    @NotNull
    public static ColumnSource<?> maybeTransformToDirectVectorColumnSource(ColumnSource<?> columnSource) {
        if (!Vector.class.isAssignableFrom(columnSource.getType())) {
            return columnSource;
        }
        // noinspection unchecked
        final Class<Vector<?>> vectorType = (Class<Vector<?>>) columnSource.getType();
        // noinspection unchecked
        final ColumnSource<Vector<?>> underlyingVectorSource = (ColumnSource<Vector<?>>) columnSource;
        return new VectorDirectDelegatingColumnSource(vectorType, columnSource.getComponentType(),
                underlyingVectorSource);
    }

    /**
     * Handles only the get method for converting a Vector into a direct vector for our snapshot trigger columns.
     *
     * NOTE: THIS CLASS DOES NOT IMPLEMENT getChunk and fillChunk PROPERLY!
     */
    private static class VectorDirectDelegatingColumnSource extends DelegatingColumnSource<Vector<?>, Vector<?>> {
        public VectorDirectDelegatingColumnSource(Class<Vector<?>> vectorType,
                Class<?> componentType,
                ColumnSource<Vector<?>> underlyingVectorSource) {
            super(vectorType, componentType, underlyingVectorSource);
        }

        @Override
        public Vector<?> get(long index) {
            Vector<?> vector = super.get(index);
            if (vector == null)
                return null;
            return vector.getDirect();
        }

        @Override
        public Vector<?> getPrev(long index) {
            Vector<?> vector = super.getPrev(index);
            if (vector == null)
                return null;
            return vector.getDirect();
        }
    }
}
