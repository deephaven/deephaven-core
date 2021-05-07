package io.deephaven.db.v2.sources.regioned;

import io.deephaven.base.MathUtil;
import io.deephaven.db.tables.utils.QueryPerformanceRecorder;

import java.lang.ref.SoftReference;

/**
 * Minor utility methods used by column region and {@link RegionedColumnSource} implementations.
 */
final class RegionUtilities {

    private RegionUtilities() {
    }

    /**
     * Initial size for new decoder buffers. A cache line seems like a nice size to work with.
     */
    final static int INITIAL_DECODER_BUFFER_SIZE = 64;

    /**
     * Thread-local decoder buffer.
     */
    private final static ThreadLocal<SoftReference<byte[]>> DECODER_BUFFER = ThreadLocal.withInitial(() ->
            new SoftReference<>(QueryPerformanceRecorder.recordPoolAllocation(
                    () -> new byte[INITIAL_DECODER_BUFFER_SIZE])));

    /**
     * Get this thread's decoder buffer ({@code byte[]}), ensuring that it has sufficient space.
     * The result may only be used on the current thread, and only for one operation at a time.
     *
     * @param requiredLength The required length
     * @return A byte array of at least the required length
     */
    static byte[] getDecoderBuffer(final int requiredLength) {
        // TODO-POOLING: Pool decoder buffers, instead of using ThreadLocals.
        byte[] currentDecoderBuffer = DECODER_BUFFER.get().get();
        if (currentDecoderBuffer == null || currentDecoderBuffer.length < requiredLength) {
            // NB: If we re-introduce maximum cached buffer sizes, make sure we determine cacheability based on the
            //     (power-of-two) size of the buffer that would be allocated.
            DECODER_BUFFER.set(new SoftReference<>(currentDecoderBuffer = QueryPerformanceRecorder.recordPoolAllocation(
                    () -> new byte[1 << MathUtil.ceilLog2(requiredLength)])));
        }
        return currentDecoderBuffer;
    }
}
