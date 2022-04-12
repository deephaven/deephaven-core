/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharHelper and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.benchmark.engine.sources;

import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableFloatChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.sources.FloatArraySource;
import io.deephaven.engine.table.impl.sources.FloatSparseArraySource;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Random;

class FloatHelper implements FillBenchmarkHelper {
    private final float[] floatArray;
    private final FloatArraySource floatArraySource;
    private final WritableColumnSource floatSparseArraySource;

    private final ColumnSource.FillContext arrayContext;
    private final ColumnSource.FillContext sparseContext;

    FloatHelper(Random random, int fullSize, int fetchSize) {
        floatArray = new float[fullSize];
        for (int ii = 0; ii < floatArray.length; ii++) {
            floatArray[ii] = makeValue(random);
        }

        floatSparseArraySource = new FloatSparseArraySource();
        floatArraySource = new FloatArraySource();
        floatArraySource.ensureCapacity(floatArray.length);

        for (int ii = 0; ii < floatArray.length; ii++) {
            floatArraySource.set(ii, floatArray[ii]);
            floatSparseArraySource.set(ii, floatArray[ii]);
        }

        arrayContext = floatArraySource.makeFillContext(fetchSize);
        sparseContext = floatSparseArraySource.makeFillContext(fetchSize);
    }

    @Override
    public void release() {
        arrayContext.close();
        sparseContext.close();
    }

    @Override
    public void getFromArray(Blackhole bh, int fetchSize, LongChunk<OrderedRowKeys> keys) {
        final WritableFloatChunk result = WritableFloatChunk.makeWritableChunk(fetchSize);
        for (int ii = 0; ii < keys.size(); ++ii) {
            result.set(ii, floatArray[(int) keys.get(ii)]);
        }
        bh.consume(result);
    }

    @Override
    public void fillFromArrayBacked(Blackhole bh, int fetchSize, RowSequence rowSequence) {
        final WritableFloatChunk result = WritableFloatChunk.makeWritableChunk(fetchSize);

        floatArraySource.fillChunk(arrayContext, result, rowSequence);

        bh.consume(result);
    }

    @Override
    public void fillFromSparse(Blackhole bh, int fetchSize, RowSequence rowSequence) {
        final WritableFloatChunk result = WritableFloatChunk.makeWritableChunk(fetchSize);

        floatSparseArraySource.fillChunk(sparseContext, result, rowSequence);

        bh.consume(result);
    }

    private float makeValue(Random random) {
        // region makeValue
        return (float) (random.nextInt('Z' - 'A') + 'A');
        // region makeValue
    }
}
