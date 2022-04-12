/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharHelper and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.benchmark.engine.sources;

import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.sources.ByteArraySource;
import io.deephaven.engine.table.impl.sources.ByteSparseArraySource;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Random;

class ByteHelper implements FillBenchmarkHelper {
    private final byte[] byteArray;
    private final ByteArraySource byteArraySource;
    private final WritableColumnSource byteSparseArraySource;

    private final ColumnSource.FillContext arrayContext;
    private final ColumnSource.FillContext sparseContext;

    ByteHelper(Random random, int fullSize, int fetchSize) {
        byteArray = new byte[fullSize];
        for (int ii = 0; ii < byteArray.length; ii++) {
            byteArray[ii] = makeValue(random);
        }

        byteSparseArraySource = new ByteSparseArraySource();
        byteArraySource = new ByteArraySource();
        byteArraySource.ensureCapacity(byteArray.length);

        for (int ii = 0; ii < byteArray.length; ii++) {
            byteArraySource.set(ii, byteArray[ii]);
            byteSparseArraySource.set(ii, byteArray[ii]);
        }

        arrayContext = byteArraySource.makeFillContext(fetchSize);
        sparseContext = byteSparseArraySource.makeFillContext(fetchSize);
    }

    @Override
    public void release() {
        arrayContext.close();
        sparseContext.close();
    }

    @Override
    public void getFromArray(Blackhole bh, int fetchSize, LongChunk<OrderedRowKeys> keys) {
        final WritableByteChunk result = WritableByteChunk.makeWritableChunk(fetchSize);
        for (int ii = 0; ii < keys.size(); ++ii) {
            result.set(ii, byteArray[(int) keys.get(ii)]);
        }
        bh.consume(result);
    }

    @Override
    public void fillFromArrayBacked(Blackhole bh, int fetchSize, RowSequence rowSequence) {
        final WritableByteChunk result = WritableByteChunk.makeWritableChunk(fetchSize);

        byteArraySource.fillChunk(arrayContext, result, rowSequence);

        bh.consume(result);
    }

    @Override
    public void fillFromSparse(Blackhole bh, int fetchSize, RowSequence rowSequence) {
        final WritableByteChunk result = WritableByteChunk.makeWritableChunk(fetchSize);

        byteSparseArraySource.fillChunk(sparseContext, result, rowSequence);

        bh.consume(result);
    }

    private byte makeValue(Random random) {
        // region makeValue
        return (byte) (random.nextInt('Z' - 'A') + 'A');
        // region makeValue
    }
}
