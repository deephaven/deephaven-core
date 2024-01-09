package io.deephaven.dataadapter.datafetch.bulk;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.dataadapter.ChunkToArrayUtil;
import io.deephaven.dataadapter.ContextHolder;

import java.time.Instant;


/**
 * Example expected output of io.deephaven.dataadapter.generator.TableDataRetrieverGenerator.
 */
class ExampleGeneratedMultiRowDataArrayRetriever extends AbstractGeneratedTableDataArrayRetriever {

    public ExampleGeneratedMultiRowDataArrayRetriever(ColumnSource<?>[] colSources) {
        super(colSources);

        if (!byte.class.isAssignableFrom(colSources[0].getType())) {
            throw new IllegalArgumentException("Column 0: Expected type byte, instead found type " +
                    colSources[0].getType().getCanonicalName());
        }
        if (!long.class.isAssignableFrom(colSources[1].getType())) {
            throw new IllegalArgumentException("Column 1: Expected type long, instead found type " +
                    colSources[1].getType().getCanonicalName());
        }
        if (!String.class.isAssignableFrom(colSources[2].getType())) {
            throw new IllegalArgumentException("Column 2: Expected type java.lang.String, instead found type " +
                    colSources[2].getType().getCanonicalName());
        }
        if (!java.time.Instant.class.isAssignableFrom(colSources[3].getType())) {
            throw new IllegalArgumentException(
                    "Column 3: Expected type java.time.Instant, instead found type " +
                            colSources[3].getType().getCanonicalName());
        }
    }

    @Override
    public final Object[] createDataArrays(final int len) {
        final int nCols = columnSources.length;
        final Object[] recordDataArrs = new Object[nCols];

        recordDataArrs[0] = new byte[len];
        recordDataArrs[1] = new long[len];
        recordDataArrs[2] = new String[len];
        recordDataArrs[3] = new java.time.Instant[len];

        return recordDataArrs;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    protected final void populateArrsForRowSequence(
            boolean usePrev,
            Object[] dataArrs,
            int arrIdx,
            ContextHolder contextHolder,
            RowSequence rowSequence,
            int rowSequenceSize) {
        Assert.eq(dataArrs.length, "dataArrs.length", columnSources.length, "recordColumnSources.length");

        ChunkToArrayUtil.populateArrFromChunk(
                columnSources[0],
                rowSequence,
                rowSequenceSize,
                contextHolder.getGetContext(0),
                (byte[]) dataArrs[0],
                arrIdx,
                usePrev);

        ChunkToArrayUtil.populateArrFromChunk(
                columnSources[1],
                rowSequence,
                rowSequenceSize,
                contextHolder.getGetContext(1),
                (long[]) dataArrs[1],
                arrIdx,
                usePrev);

        ChunkToArrayUtil.populateObjArrFromChunk(
                columnSources[2],
                rowSequence,
                rowSequenceSize,
                contextHolder.getGetContext(2),
                (String[]) dataArrs[2],
                arrIdx,
                usePrev);

        ChunkToArrayUtil.populateObjArrFromChunk(
                columnSources[3],
                rowSequence,
                rowSequenceSize,
                contextHolder.getGetContext(3),
                (Instant[]) dataArrs[3],
                arrIdx,
                usePrev);
    }

}
