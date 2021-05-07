/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharToLongCastWithOffset and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.hashing;

import io.deephaven.db.v2.sources.chunk.Attributes.Any;
import io.deephaven.db.v2.sources.chunk.ByteChunk;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.LongChunk;
import io.deephaven.db.v2.sources.chunk.WritableLongChunk;

/**
 * Cast the values in the input chunk to an long and add the specified offset.
 *
 * @param <T> the chunk's attribute
 */
public class ByteToLongCastWithOffset<T extends Any> implements ToLongFunctor<T> {
    private final WritableLongChunk<T> result;
    private final long offset;

    ByteToLongCastWithOffset(int size, long offset) {
        result = WritableLongChunk.makeWritableChunk(size);
        this.offset = offset;
    }

    @Override
    public LongChunk<T> apply(Chunk<T> input) {
        return castWithOffset(input.asByteChunk());
    }

    private LongChunk<T> castWithOffset(ByteChunk<T> input) {
        castInto(input, result, offset);
        return result;
    }

    public static <T2 extends Any>  void castInto(ByteChunk<T2> input, WritableLongChunk<T2> result, long offset) {
        for (int ii = 0; ii < input.size(); ++ii) {
            result.set(ii, (long)input.get(ii) + offset);
        }
        result.setSize(input.size());
    }

    @Override
    public void close() {
        result.close();
    }
}
