package io.deephaven.engine.v2.sources;

import io.deephaven.engine.v2.sources.chunk.Attributes.OrderedRowKeys;
import io.deephaven.engine.v2.sources.chunk.LongChunk;
import io.deephaven.engine.v2.sources.chunk.WritableCharChunk;
import io.deephaven.engine.structures.RowSequence;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Random;

class CharHelper implements FillBenchmarkHelper {
    private final char[] charArray;
    private final CharacterArraySource charArraySource;
    private final WritableSource charSparseArraySource;

    private final ColumnSource.FillContext arrayContext;
    private final ColumnSource.FillContext sparseContext;

    CharHelper(Random random, int fullSize, int fetchSize) {
        charArray = new char[fullSize];
        for (int ii = 0; ii < charArray.length; ii++) {
            charArray[ii] = makeValue(random);
        }

        charSparseArraySource = new CharacterSparseArraySource();
        charArraySource = new CharacterArraySource();
        charArraySource.ensureCapacity(charArray.length);

        for (int ii = 0; ii < charArray.length; ii++) {
            charArraySource.set(ii, charArray[ii]);
            charSparseArraySource.set(ii, charArray[ii]);
        }

        arrayContext = charArraySource.makeFillContext(fetchSize);
        sparseContext = charSparseArraySource.makeFillContext(fetchSize);
    }

    @Override
    public void release() {
        arrayContext.close();
        sparseContext.close();
    }

    @Override
    public void getFromArray(Blackhole bh, int fetchSize, LongChunk<OrderedRowKeys> keys) {
        final WritableCharChunk result = WritableCharChunk.makeWritableChunk(fetchSize);
        for (int ii = 0; ii < keys.size(); ++ii) {
            result.set(ii, charArray[(int)keys.get(ii)]);
        }
        bh.consume(result);
    }

    @Override
    public void fillFromArrayBacked(Blackhole bh, int fetchSize, RowSequence rowSequence) {
        final WritableCharChunk result = WritableCharChunk.makeWritableChunk(fetchSize);

        charArraySource.fillChunk(arrayContext, result, rowSequence);

        bh.consume(result);
    }

    @Override
    public void fillFromSparse(Blackhole bh, int fetchSize, RowSequence rowSequence) {
        final WritableCharChunk result = WritableCharChunk.makeWritableChunk(fetchSize);

        charSparseArraySource.fillChunk(sparseContext, result, rowSequence);

        bh.consume(result);
    }

    private char makeValue(Random random) {
        // region makeValue
        return (char)(random.nextInt('Z' - 'A') + 'A');
        // region makeValue
    }
}
