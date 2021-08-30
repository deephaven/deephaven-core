package io.deephaven.db.v2.utils.copy;

import io.deephaven.db.v2.sources.chunk.*;

import static io.deephaven.db.v2.sources.chunk.Attributes.*;

public interface CopyKernel {
    static CopyKernel makeCopyKernel(ChunkType chunkType) {
        switch (chunkType) {
            case Char:
                return CharCopyKernel.INSTANCE;
            case Byte:
                return ByteCopyKernel.INSTANCE;
            case Short:
                return ShortCopyKernel.INSTANCE;
            case Int:
                return IntCopyKernel.INSTANCE;
            case Long:
                return LongCopyKernel.INSTANCE;
            case Float:
                return FloatCopyKernel.INSTANCE;
            case Double:
                return DoubleCopyKernel.INSTANCE;
            case Boolean:
                return BooleanCopyKernel.INSTANCE;
            default:
                return ObjectCopyKernel.INSTANCE;
        }
    }

    /**
     * Copy values from baseInput / overInput into output.
     * <p>
     * Pseudo-implementation: output[i] = useOverInput.forBit(i) ? overInput[i] : baseInput[i];
     * <p>
     * Note that useOverInput should cover the same data range as baseInput such that
     * {@code useOverInput.length == (overInput.length + 63) / 64} is true.
     *
     * @param output the output chunk
     * @param baseInput the input array to use when bit in useOverInput is zero (array)
     * @param overInput the input array to use when bit in useOverInput is one (array)
     * @param useOverInput the bitset array to indicate whether to use baseInput or overInput for
     *        each element
     * @param srcOffset the offset in baseInput/overInput
     * @param dstOffset the offset in output
     * @param length the number of elements to copy
     */
    <T extends Any> void conditionalCopy(WritableChunk<T> output, Object baseInput,
        Object overInput, long[] useOverInput,
        int srcOffset, int dstOffset, int length);

    class Utils {
        /**
         * Returns the index of the first bit that is set to {@code true} that occurs on or after
         * the specified starting index and up to but not including the specified word index.
         * <p>
         * If no such bit exists then {@code endIndex} is returned.
         *
         * @param words the bitset data array
         * @param fromIndex the index to start checking from (inclusive)
         * @param endIndex the index to stop checking from (exclusive)
         * @param flipWords if true return first false bit set instead of the first true bit set
         * @return the index of the next set bit, any value {@code >= endIndex} is returned if no
         *         such bit exists
         */
        static int nextSetBit(long[] words, int fromIndex, int endIndex, boolean flipWords) {
            if (fromIndex >= endIndex) {
                return endIndex;
            }
            int u = fromIndex >> 6;

            long word = (flipWords ? ~words[u] : words[u]) & (-1L << (fromIndex & 63));

            while (true) {
                if (word != 0) {
                    return (u << 6) + Long.numberOfTrailingZeros(word);
                }
                if ((++u) << 6 >= endIndex) {
                    return endIndex;
                }
                word = flipWords ? ~words[u] : words[u];
            }
        }
    }
}
