package io.deephaven.queryutil.dataadapter.datafetch.bulk.simple;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;

/**
 * @param <COL_TYPE> The data type of the column.
 * @param <ARR_TYPE> An array that can store values of a column of {@code COL_TYPE}. This is not checked. For
 *                   primitive columns, {@code COL_TYPE} is a boxed type but {@code ARR_TYPE} must be an array
 *                   of the primitive type.
 */
@Deprecated
public interface ChunkedArrPopulator<COL_TYPE, ARR_TYPE> {

    @SuppressWarnings("unchecked")
    static <COL_TYPE> ChunkedArrPopulator<COL_TYPE, ?> getChunkToArrPopulator(Class<COL_TYPE> colType) {
        if (byte.class.equals(colType)) {
            return (ChunkedArrPopulator<COL_TYPE, byte[]>) ChunkedArrPopulators.BYTE_ARR_POPULATOR;
        } else if (char.class.equals(colType)) {
            return (ChunkedArrPopulator<COL_TYPE, char[]>) ChunkedArrPopulators.CHAR_ARR_POPULATOR;
        } else if (short.class.equals(colType)) {
            return (ChunkedArrPopulator<COL_TYPE, short[]>) ChunkedArrPopulators.SHORT_ARR_POPULATOR;
        } else if (int.class.equals(colType)) {
            return (ChunkedArrPopulator<COL_TYPE, int[]>) ChunkedArrPopulators.INT_ARR_POPULATOR;
        } else if (float.class.equals(colType)) {
            return (ChunkedArrPopulator<COL_TYPE, float[]>) ChunkedArrPopulators.FLOAT_ARR_POPULATOR;
        } else if (long.class.equals(colType)) {
            return (ChunkedArrPopulator<COL_TYPE, long[]>) ChunkedArrPopulators.LONG_ARR_POPULATOR;
        } else if (double.class.equals(colType)) {
            return (ChunkedArrPopulator<COL_TYPE, double[]>) ChunkedArrPopulators.DOUBLE_ARR_POPULATOR;
        } else {
            return ChunkedArrPopulators.getObjChunkPopulator();
        }
    }

    void populateArrFromChunk(
            final ColumnSource<COL_TYPE> columnSource,
            final RowSequence rowSequence,
            final int len,
            final ChunkSource.GetContext context,
            final ARR_TYPE arr,
            final int arrOffset,
            final boolean usePrev
    );
}
