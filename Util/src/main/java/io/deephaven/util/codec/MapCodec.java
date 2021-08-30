package io.deephaven.util.codec;

import io.deephaven.datastructures.util.CollectionUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.ref.SoftReference;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * ObjectCodec implementation for Maps of type K to V.
 *
 * Each map is encoded as an integer length, followed by encoded pairs of key values.
 *
 * A null map is represented as an array of zero bytes.
 */
@SuppressWarnings("unused")
public abstract class MapCodec<K, V> implements ObjectCodec<Map<K, V>> {
    private static final byte[] nullBytes = CollectionUtil.ZERO_LENGTH_BYTE_ARRAY;
    private static final byte[] zeroBytes = new byte[4];

    private static final int MINIMUM_SCRATCH_CAPACITY = 4096;
    private static final ThreadLocal<SoftReference<ByteBuffer>> scratchBufferThreadLocal =
        ThreadLocal
            .withInitial(() -> new SoftReference<>(ByteBuffer.allocate(MINIMUM_SCRATCH_CAPACITY)));

    MapCodec(@Nullable final String arguments) {}

    @Override
    public boolean isNullable() {
        return true;
    }

    @Override
    public int getPrecision() {
        return 0;
    }

    @Override
    public int getScale() {
        return 0;
    }

    @NotNull
    @Override
    public byte[] encode(@Nullable final Map<K, V> input) {
        if (input == null) {
            return nullBytes;
        }
        if (input.size() == 0) {
            return zeroBytes;
        }

        // the scratch buffer is a soft reference, if we are the operation that allocated
        // it, we want to ensure hard reachability for the duration of this function.
        ByteBuffer holdScratch = scratchBufferThreadLocal.get().get();
        if (holdScratch == null) {
            holdScratch = allocateScratch(Math.max(estimateSize(input), 4096));
        }

        // the resulting successfully encoded scratch buffer
        ByteBuffer scratch = null;

        int estimatedCapacity = -1;
        // on the first try, we'll use whatever our scratch buffer was
        // on the second try, we'll have an estimate which is 10% bigger than if every character was
        // 1 byte
        // on the third try, we'll allow for every character to be two bytes
        // on the fourth try, we'll allow for every character to be four bytes
        // if there is a fifth try, it means that we could not encode this properly, given that
        // there is a limit of
        // 4 bytes in a UTF-8 character.
        for (int tryCount = 0; tryCount < 4; ++tryCount) {
            try {
                scratch = encodeIntoBuffer(holdScratch, input);
            } catch (BufferUnderflowException bue) {
                if (estimatedCapacity < 0) {
                    estimatedCapacity = estimateSize(input);
                } else {
                    estimatedCapacity *= 2;
                }
                holdScratch = allocateScratch(estimatedCapacity);
            }
        }
        if (scratch == null) {
            throw new BufferUnderflowException();
        }

        final byte[] bytes = new byte[scratch.position()];
        scratch.flip();
        scratch.get(bytes);
        return bytes;
    }

    private ByteBuffer allocateScratch(int estimatedCapacity) {
        final ByteBuffer holdScratch;
        scratchBufferThreadLocal
            .set(new SoftReference<>(holdScratch = ByteBuffer.allocate(estimatedCapacity)));
        return holdScratch;
    }


    private ByteBuffer encodeIntoBuffer(final ByteBuffer scratch, @NotNull Map<K, V> input) {
        scratch.clear();
        scratch.putInt(input.size());
        for (final Map.Entry<K, V> entry : input.entrySet()) {
            encodeKey(scratch, entry.getKey());
            encodeValue(scratch, entry.getValue());
        }
        return scratch;
    }

    @Nullable
    @Override
    public Map<K, V> decode(@NotNull final byte[] input, final int offset, final int length) {
        if (input.length == 0) {
            return null;
        }
        final ByteBuffer byteBuffer = ByteBuffer.wrap(input);
        final int size = byteBuffer.getInt();
        if (size == 0) {
            return Collections.emptyMap();
        }
        if (size == 1) {
            final K key = decodeKey(byteBuffer);
            final V value = decodeValue(byteBuffer);
            return Collections.singletonMap(key, value);
        }
        final LinkedHashMap<K, V> result = new LinkedHashMap<>(size);
        for (int ii = 0; ii < size; ++ii) {
            result.put(decodeKey(byteBuffer), decodeValue(byteBuffer));
        }
        return Collections.unmodifiableMap(result);
    }

    /**
     * Estimate the size of the encoded map.
     *
     * The estimated size is used to encode the map; and is doubled twice if there is a buffer
     * underflow exception. Thus if you are wrong by more than a factor of 4x, the map can not be
     * encoded and a BufferUnderflow exception is returned to the caller.
     *
     * @param input the input map
     * @return the estimated size of the map
     */
    abstract int estimateSize(Map<K, V> input);

    abstract K decodeKey(ByteBuffer byteBuffer);

    abstract V decodeValue(ByteBuffer byteBuffer);

    abstract void encodeKey(ByteBuffer scratch, K entry);

    abstract void encodeValue(ByteBuffer scratch, V entry);

    @Override
    public int expectedObjectWidth() {
        return VARIABLE_WIDTH_SENTINEL;
    }
}
