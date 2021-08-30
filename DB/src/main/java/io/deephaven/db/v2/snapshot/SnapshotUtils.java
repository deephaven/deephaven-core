package io.deephaven.db.v2.snapshot;

import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.WritableSource;
import io.deephaven.db.v2.sources.chunk.util.chunkfillers.ChunkFiller;
import io.deephaven.db.v2.utils.ChunkUtils;
import io.deephaven.db.v2.utils.Index;
import org.jetbrains.annotations.NotNull;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiFunction;

public class SnapshotUtils {
    /**
     * Creates a new columnSourceMap based on 'columns'.
     */
    public static <T> Map<String, T> createColumnSourceMap(
        Map<String, ? extends ColumnSource> columns,
        BiFunction<Class, Class, T> factory) {
        final Map<String, T> result = new LinkedHashMap<>();
        for (final Map.Entry<String, ? extends ColumnSource> entry : columns.entrySet()) {
            final String key = entry.getKey();
            final ColumnSource cs = entry.getValue();
            final T newColumn = factory.apply(cs.getType(), cs.getComponentType());
            result.put(key, newColumn);
        }
        return result;
    }

    /**
     * For each name in stampColumns: - identify a stamp source (namely 'stampColumns.get(name)') -
     * a row in that stamp source (namely 'stampKey') - a stamp dest (namely the column identified
     * by 'destColumns.get(name)') - a bunch of destination rows (namely all the rows defined in
     * 'destIndex') Then "spray" that single source value over those destination values.
     *
     * @param stampColumns The stamp columns that serve as the source data
     * @param stampKey The source key
     * @param destColumns The destination columns we are "spraying" to
     * @param destIndex The keys in destColumns we want to write to
     */
    public static void copyStampColumns(@NotNull Map<String, ? extends ColumnSource> stampColumns,
        long stampKey,
        @NotNull Map<String, ? extends WritableSource> destColumns, @NotNull Index destIndex) {
        for (Map.Entry<String, ? extends ColumnSource> entry : stampColumns.entrySet()) {
            final String name = entry.getKey();
            final ColumnSource src = entry.getValue();

            // Fill the corresponding destination column
            final WritableSource dest = destColumns.get(name);
            final ChunkFiller destFiller = ChunkFiller.fromChunkType(src.getChunkType());
            destFiller.fillFromSingleValue(src, stampKey, dest, destIndex);
        }
    }

    /**
     * Like the above, but with a singleton destination. For each name in stampColumns: - identify a
     * stamp source (namely 'stampColumns.get(name)') - a row in that stamp source (namely
     * 'stampKey') - a stamp dest (namely the column identified by 'destColumns.get(name)') - a row
     * in the destination (namely 'destKey') Then copy the source value to the destination value.
     *
     * @param stampColumns The stamp columns that serve as the source data
     * @param stampKey The source key
     * @param destColumns The destination columns we are writing to to
     * @param destKey The key in destColumns we want to write to
     */
    public static void copyStampColumns(@NotNull Map<String, ? extends ColumnSource> stampColumns,
        long stampKey,
        @NotNull Map<String, ? extends WritableSource> destColumns, long destKey) {
        for (Map.Entry<String, ? extends ColumnSource> entry : stampColumns.entrySet()) {
            final String name = entry.getKey();
            final ColumnSource src = entry.getValue();

            // Fill the corresponding destination column
            final WritableSource dest = destColumns.get(name);
            dest.copy(src, stampKey, destKey);
        }
    }

    /**
     * For each name in srcColumns, copy all the data at srcColumns.get(name) (with a range of rows
     * defined by srcIndex) to a column indicated by destColumns.get(name) (with a range of rows
     * defined by destIndex).
     *
     * @param srcColumns The stamp columns that serve as the source data
     * @param srcIndex The keys in the srcColumns we are reading from
     * @param destColumns The destination columns we are writing to
     * @param destIndex The keys in destColumns we want to write to
     */
    public static void copyDataColumns(@NotNull Map<String, ? extends ColumnSource> srcColumns,
        @NotNull Index srcIndex, @NotNull Map<String, ? extends WritableSource> destColumns,
        @NotNull Index destIndex,
        boolean usePrev) {
        assert srcIndex.size() == destIndex.size();
        if (srcIndex.empty()) {
            return;
        }
        for (Map.Entry<String, ? extends ColumnSource> entry : srcColumns.entrySet()) {
            final String name = entry.getKey();
            final ColumnSource srcCs = entry.getValue();

            final WritableSource destCs = destColumns.get(name);
            destCs.ensureCapacity(destIndex.lastKey() + 1);
            ChunkUtils.copyData(srcCs, srcIndex, destCs, destIndex, usePrev);
        }
    }
}
