package io.deephaven.chunk;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.util.pools.PoolableChunk;

/**
 * {@link WritableChunk} that may have its backing storage reset to a slice of that belonging to another
 * {@link WritableChunk} or a native array.
 */
public interface ResettableWritableChunk<ATTR_BASE extends Any>
        extends ResettableChunk<ATTR_BASE>, WritableChunk, PoolableChunk {

    @Override
    <ATTR extends ATTR_BASE> WritableChunk<ATTR> resetFromChunk(WritableChunk<ATTR> other, int offset, int capacity);

    @Override
    <ATTR extends ATTR_BASE> WritableChunk<ATTR> resetFromArray(Object array, int offset, int capacity);

    @Override
    <ATTR extends ATTR_BASE> WritableChunk<ATTR> resetFromArray(Object array);

    @Override
    <ATTR extends ATTR_BASE> WritableChunk<ATTR> clear();

    default ResettableWritableByteChunk<ATTR_BASE> asResettableWritableByteChunk() {
        return (ResettableWritableByteChunk<ATTR_BASE>) this;
    }

    default ResettableWritableBooleanChunk<ATTR_BASE> asResettableWritableBooleanChunk() {
        return (ResettableWritableBooleanChunk<ATTR_BASE>) this;
    }

    default ResettableWritableCharChunk<ATTR_BASE> asResettableWritableCharChunk() {
        return (ResettableWritableCharChunk<ATTR_BASE>) this;
    }

    default ResettableWritableShortChunk<ATTR_BASE> asResettableWritableShortChunk() {
        return (ResettableWritableShortChunk<ATTR_BASE>) this;
    }

    default ResettableWritableIntChunk<ATTR_BASE> asResettableWritableIntChunk() {
        return (ResettableWritableIntChunk<ATTR_BASE>) this;
    }

    default ResettableWritableLongChunk<ATTR_BASE> asResettableWritableLongChunk() {
        return (ResettableWritableLongChunk<ATTR_BASE>) this;
    }

    default ResettableWritableFloatChunk<ATTR_BASE> asResettableWritableFloatChunk() {
        return (ResettableWritableFloatChunk<ATTR_BASE>) this;
    }

    default ResettableWritableDoubleChunk<ATTR_BASE> asResettableWritableDoubleChunk() {
        return (ResettableWritableDoubleChunk<ATTR_BASE>) this;
    }

    default <T> ResettableWritableObjectChunk<T, ATTR_BASE> asResettableWritableObjectChunk() {
        return (ResettableWritableObjectChunk<T, ATTR_BASE>) this;
    }
}
