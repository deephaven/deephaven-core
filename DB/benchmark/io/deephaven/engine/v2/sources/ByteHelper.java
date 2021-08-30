/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharHelper and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.v2.sources;

import io.deephaven.engine.structures.chunk.Attributes.OrderedKeyIndices;
import io.deephaven.engine.structures.chunk.LongChunk;
import io.deephaven.engine.structures.chunk.WritableByteChunk;
import io.deephaven.engine.structures.rowsequence.OrderedKeys;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Random;

class ByteHelper implements FillBenchmarkHelper {
    private final byte[] byteArray;
    private final ByteArraySource byteArraySource;
    private final WritableSource byteSparseArraySource;

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
    public void getFromArray(Blackhole bh, int fetchSize, LongChunk<OrderedKeyIndices> keys) {
        final WritableByteChunk result = WritableByteChunk.makeWritableChunk(fetchSize);
        for (int ii = 0; ii < keys.size(); ++ii) {
            result.set(ii, byteArray[(int)keys.get(ii)]);
        }
        bh.consume(result);
    }

    @Override
    public void fillFromArrayBacked(Blackhole bh, int fetchSize, OrderedKeys orderedKeys) {
        final WritableByteChunk result = WritableByteChunk.makeWritableChunk(fetchSize);

        byteArraySource.fillChunk(arrayContext, result, orderedKeys);

        bh.consume(result);
    }

    @Override
    public void fillFromSparse(Blackhole bh, int fetchSize, OrderedKeys orderedKeys) {
        final WritableByteChunk result = WritableByteChunk.makeWritableChunk(fetchSize);

        byteSparseArraySource.fillChunk(sparseContext, result, orderedKeys);

        bh.consume(result);
    }

    private byte makeValue(Random random) {
        // region makeValue
        return (byte)(random.nextInt('Z' - 'A') + 'A');
        // region makeValue
    }
}
