package io.deephaven.db.v2.sort.megamerge;

import io.deephaven.db.v2.sort.timsort.*;
import io.deephaven.db.v2.sources.chunk.*;

import java.util.Random;

public class MegaMergeTestUtils {
    static char getRandomChar(Random random) {
        return (char) ('A' + random.nextInt(10));
    }

    static long getRandomLong(Random random) {
        return random.nextLong();
    }

    static double getRandomDouble(Random random) {
        return random.nextDouble();
    }

    static float getRandomFloat(Random random) {
        return random.nextFloat();
    }

    static byte getRandomByte(Random random) {
        return (byte) (random.nextInt(Byte.MAX_VALUE - Byte.MIN_VALUE) - Byte.MIN_VALUE);
    }

    static short getRandomShort(Random random) {
        return (short) (random.nextInt(Short.MAX_VALUE - Short.MIN_VALUE) - Short.MIN_VALUE);
    }

    static int getRandomInt(Random random) {
        return random.nextInt();
    }

    static String getRandomObject(Random random) {
        return random.nextDouble() < 0.1 ? null : Long.toString(random.nextInt());
    }

    static void doSort(boolean ascending, int chunkSize,
        WritableCharChunk<Attributes.Values> values,
        WritableLongChunk<Attributes.KeyIndices> keys) {
        if (ascending) {
            try (
                final NullAwareCharLongTimsortKernel.CharLongSortKernelContext<Attributes.Values, Attributes.KeyIndices> sortKernel =
                    NullAwareCharLongTimsortKernel.createContext(chunkSize)) {
                NullAwareCharLongTimsortKernel.sort(sortKernel, keys, values);
            }
        } else {
            try (
                final NullAwareCharLongTimsortDescendingKernel.CharLongSortKernelContext<Attributes.Values, Attributes.KeyIndices> sortKernel =
                    NullAwareCharLongTimsortDescendingKernel.createContext(chunkSize)) {
                NullAwareCharLongTimsortDescendingKernel.sort(sortKernel, keys, values);
            }
        }
    }

    static void doSort(boolean ascending, int chunkSize,
        WritableByteChunk<Attributes.Values> values,
        WritableLongChunk<Attributes.KeyIndices> keys) {
        if (ascending) {
            try (
                final ByteLongTimsortKernel.ByteLongSortKernelContext<Attributes.Values, Attributes.KeyIndices> sortKernel =
                    ByteLongTimsortKernel.createContext(chunkSize)) {
                ByteLongTimsortKernel.sort(sortKernel, keys, values);
            }
        } else {
            try (
                final ByteLongTimsortDescendingKernel.ByteLongSortKernelContext<Attributes.Values, Attributes.KeyIndices> sortKernel =
                    ByteLongTimsortDescendingKernel.createContext(chunkSize)) {
                ByteLongTimsortDescendingKernel.sort(sortKernel, keys, values);
            }
        }
    }

    static void doSort(boolean ascending, int chunkSize,
        WritableShortChunk<Attributes.Values> values,
        WritableLongChunk<Attributes.KeyIndices> keys) {
        if (ascending) {
            try (
                final ShortLongTimsortKernel.ShortLongSortKernelContext<Attributes.Values, Attributes.KeyIndices> sortKernel =
                    ShortLongTimsortKernel.createContext(chunkSize)) {
                ShortLongTimsortKernel.sort(sortKernel, keys, values);
            }
        } else {
            try (
                final ShortLongTimsortDescendingKernel.ShortLongSortKernelContext<Attributes.Values, Attributes.KeyIndices> sortKernel =
                    ShortLongTimsortDescendingKernel.createContext(chunkSize)) {
                ShortLongTimsortDescendingKernel.sort(sortKernel, keys, values);
            }
        }
    }

    static void doSort(boolean ascending, int chunkSize, WritableIntChunk<Attributes.Values> values,
        WritableLongChunk<Attributes.KeyIndices> keys) {
        if (ascending) {
            try (
                final IntLongTimsortKernel.IntLongSortKernelContext<Attributes.Values, Attributes.KeyIndices> sortKernel =
                    IntLongTimsortKernel.createContext(chunkSize)) {
                IntLongTimsortKernel.sort(sortKernel, keys, values);
            }
        } else {
            try (
                final IntLongTimsortDescendingKernel.IntLongSortKernelContext<Attributes.Values, Attributes.KeyIndices> sortKernel =
                    IntLongTimsortDescendingKernel.createContext(chunkSize)) {
                IntLongTimsortDescendingKernel.sort(sortKernel, keys, values);
            }
        }
    }

    static void doSort(boolean ascending, int chunkSize,
        WritableLongChunk<Attributes.Values> values,
        WritableLongChunk<Attributes.KeyIndices> keys) {
        if (ascending) {
            try (
                final LongLongTimsortKernel.LongLongSortKernelContext<Attributes.Values, Attributes.KeyIndices> sortKernel =
                    LongLongTimsortKernel.createContext(chunkSize)) {
                LongLongTimsortKernel.sort(sortKernel, keys, values);
            }
        } else {
            try (
                final LongLongTimsortDescendingKernel.LongLongSortKernelContext<Attributes.Values, Attributes.KeyIndices> sortKernel =
                    LongLongTimsortDescendingKernel.createContext(chunkSize)) {
                LongLongTimsortDescendingKernel.sort(sortKernel, keys, values);
            }
        }
    }

    static void doSort(boolean ascending, int chunkSize,
        WritableFloatChunk<Attributes.Values> values,
        WritableLongChunk<Attributes.KeyIndices> keys) {
        if (ascending) {
            try (
                final FloatLongTimsortKernel.FloatLongSortKernelContext<Attributes.Values, Attributes.KeyIndices> sortKernel =
                    FloatLongTimsortKernel.createContext(chunkSize)) {
                FloatLongTimsortKernel.sort(sortKernel, keys, values);
            }
        } else {
            try (
                final FloatLongTimsortDescendingKernel.FloatLongSortKernelContext<Attributes.Values, Attributes.KeyIndices> sortKernel =
                    FloatLongTimsortDescendingKernel.createContext(chunkSize)) {
                FloatLongTimsortDescendingKernel.sort(sortKernel, keys, values);
            }
        }
    }


    static void doSort(boolean ascending, int chunkSize,
        WritableDoubleChunk<Attributes.Values> values,
        WritableLongChunk<Attributes.KeyIndices> keys) {
        if (ascending) {
            try (
                final DoubleLongTimsortKernel.DoubleLongSortKernelContext<Attributes.Values, Attributes.KeyIndices> sortKernel =
                    DoubleLongTimsortKernel.createContext(chunkSize)) {
                DoubleLongTimsortKernel.sort(sortKernel, keys, values);
            }
        } else {
            try (
                final DoubleLongTimsortDescendingKernel.DoubleLongSortKernelContext<Attributes.Values, Attributes.KeyIndices> sortKernel =
                    DoubleLongTimsortDescendingKernel.createContext(chunkSize)) {
                DoubleLongTimsortDescendingKernel.sort(sortKernel, keys, values);
            }
        }
    }

    static void doSort(boolean ascending, int chunkSize,
        WritableObjectChunk<Object, Attributes.Values> values,
        WritableLongChunk<Attributes.KeyIndices> keys) {
        if (ascending) {
            try (
                final ObjectLongTimsortKernel.ObjectLongSortKernelContext<Attributes.Values, Attributes.KeyIndices> sortKernel =
                    ObjectLongTimsortKernel.createContext(chunkSize)) {
                ObjectLongTimsortKernel.sort(sortKernel, keys, values);
            }
        } else {
            try (
                final ObjectLongTimsortDescendingKernel.ObjectLongSortKernelContext<Attributes.Values, Attributes.KeyIndices> sortKernel =
                    ObjectLongTimsortDescendingKernel.createContext(chunkSize)) {
                ObjectLongTimsortDescendingKernel.sort(sortKernel, keys, values);
            }
        }
    }

}
