/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharHelper and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.v2.sources;

import io.deephaven.engine.chunk.Attributes.OrderedRowKeys;
import io.deephaven.engine.chunk.LongChunk;
import io.deephaven.engine.chunk.WritableDoubleChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Random;

class DoubleHelper implements FillBenchmarkHelper {
    private final double[] doubleArray;
    private final DoubleArraySource doubleArraySource;
    private final WritableSource doubleSparseArraySource;

    private final ColumnSource.FillContext arrayContext;
    private final ColumnSource.FillContext sparseContext;

    DoubleHelper(Random random, int fullSize, int fetchSize) {
        doubleArray = new double[fullSize];
        for (int ii = 0; ii < doubleArray.length; ii++) {
            doubleArray[ii] = makeValue(random);
        }

        doubleSparseArraySource = new DoubleSparseArraySource();
        doubleArraySource = new DoubleArraySource();
        doubleArraySource.ensureCapacity(doubleArray.length);

        for (int ii = 0; ii < doubleArray.length; ii++) {
            doubleArraySource.set(ii, doubleArray[ii]);
            doubleSparseArraySource.set(ii, doubleArray[ii]);
        }

        arrayContext = doubleArraySource.makeFillContext(fetchSize);
        sparseContext = doubleSparseArraySource.makeFillContext(fetchSize);
    }

    @Override
    public void release() {
        arrayContext.close();
        sparseContext.close();
    }

    @Override
    public void getFromArray(Blackhole bh, int fetchSize, LongChunk<OrderedRowKeys> keys) {
        final WritableDoubleChunk result = WritableDoubleChunk.makeWritableChunk(fetchSize);
        for (int ii = 0; ii < keys.size(); ++ii) {
            result.set(ii, doubleArray[(int)keys.get(ii)]);
        }
        bh.consume(result);
    }

    @Override
    public void fillFromArrayBacked(Blackhole bh, int fetchSize, RowSequence rowSequence) {
        final WritableDoubleChunk result = WritableDoubleChunk.makeWritableChunk(fetchSize);

        doubleArraySource.fillChunk(arrayContext, result, rowSequence);

        bh.consume(result);
    }

    @Override
    public void fillFromSparse(Blackhole bh, int fetchSize, RowSequence rowSequence) {
        final WritableDoubleChunk result = WritableDoubleChunk.makeWritableChunk(fetchSize);

        doubleSparseArraySource.fillChunk(sparseContext, result, rowSequence);

        bh.consume(result);
    }

    private double makeValue(Random random) {
        // region makeValue
        return (double)(random.nextInt('Z' - 'A') + 'A');
        // region makeValue
    }
}
