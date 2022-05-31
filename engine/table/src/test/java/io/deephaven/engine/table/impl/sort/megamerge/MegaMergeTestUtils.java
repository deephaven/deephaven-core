package io.deephaven.engine.table.impl.sort.megamerge;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.sort.timsort.*;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;

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

    static void doSort(boolean ascending, int chunkSize, WritableCharChunk<Values> values,
            WritableLongChunk<RowKeys> keys) {
        if (ascending) {
            try (final NullAwareCharLongTimsortKernel.CharLongSortKernelContext<Values, RowKeys> sortKernel =
                    NullAwareCharLongTimsortKernel.createContext(chunkSize)) {
                NullAwareCharLongTimsortKernel.sort(sortKernel, keys, values);
            }
        } else {
            try (final NullAwareCharLongTimsortDescendingKernel.CharLongSortKernelContext<Values, RowKeys> sortKernel =
                    NullAwareCharLongTimsortDescendingKernel.createContext(chunkSize)) {
                NullAwareCharLongTimsortDescendingKernel.sort(sortKernel, keys, values);
            }
        }
    }

    static void doSort(boolean ascending, int chunkSize, WritableByteChunk<Values> values,
            WritableLongChunk<RowKeys> keys) {
        if (ascending) {
            try (final ByteLongTimsortKernel.ByteLongSortKernelContext<Values, RowKeys> sortKernel =
                    ByteLongTimsortKernel.createContext(chunkSize)) {
                ByteLongTimsortKernel.sort(sortKernel, keys, values);
            }
        } else {
            try (final ByteLongTimsortDescendingKernel.ByteLongSortKernelContext<Values, RowKeys> sortKernel =
                    ByteLongTimsortDescendingKernel.createContext(chunkSize)) {
                ByteLongTimsortDescendingKernel.sort(sortKernel, keys, values);
            }
        }
    }

    static void doSort(boolean ascending, int chunkSize, WritableShortChunk<Values> values,
            WritableLongChunk<RowKeys> keys) {
        if (ascending) {
            try (final ShortLongTimsortKernel.ShortLongSortKernelContext<Values, RowKeys> sortKernel =
                    ShortLongTimsortKernel.createContext(chunkSize)) {
                ShortLongTimsortKernel.sort(sortKernel, keys, values);
            }
        } else {
            try (final ShortLongTimsortDescendingKernel.ShortLongSortKernelContext<Values, RowKeys> sortKernel =
                    ShortLongTimsortDescendingKernel.createContext(chunkSize)) {
                ShortLongTimsortDescendingKernel.sort(sortKernel, keys, values);
            }
        }
    }

    static void doSort(boolean ascending, int chunkSize, WritableIntChunk<Values> values,
            WritableLongChunk<RowKeys> keys) {
        if (ascending) {
            try (final IntLongTimsortKernel.IntLongSortKernelContext<Values, RowKeys> sortKernel =
                    IntLongTimsortKernel.createContext(chunkSize)) {
                IntLongTimsortKernel.sort(sortKernel, keys, values);
            }
        } else {
            try (final IntLongTimsortDescendingKernel.IntLongSortKernelContext<Values, RowKeys> sortKernel =
                    IntLongTimsortDescendingKernel.createContext(chunkSize)) {
                IntLongTimsortDescendingKernel.sort(sortKernel, keys, values);
            }
        }
    }

    static void doSort(boolean ascending, int chunkSize, WritableLongChunk<Values> values,
            WritableLongChunk<RowKeys> keys) {
        if (ascending) {
            try (final LongLongTimsortKernel.LongLongSortKernelContext<Values, RowKeys> sortKernel =
                    LongLongTimsortKernel.createContext(chunkSize)) {
                LongLongTimsortKernel.sort(sortKernel, keys, values);
            }
        } else {
            try (final LongLongTimsortDescendingKernel.LongLongSortKernelContext<Values, RowKeys> sortKernel =
                    LongLongTimsortDescendingKernel.createContext(chunkSize)) {
                LongLongTimsortDescendingKernel.sort(sortKernel, keys, values);
            }
        }
    }

    static void doSort(boolean ascending, int chunkSize, WritableFloatChunk<Values> values,
            WritableLongChunk<RowKeys> keys) {
        if (ascending) {
            try (final FloatLongTimsortKernel.FloatLongSortKernelContext<Values, RowKeys> sortKernel =
                    FloatLongTimsortKernel.createContext(chunkSize)) {
                FloatLongTimsortKernel.sort(sortKernel, keys, values);
            }
        } else {
            try (final FloatLongTimsortDescendingKernel.FloatLongSortKernelContext<Values, RowKeys> sortKernel =
                    FloatLongTimsortDescendingKernel.createContext(chunkSize)) {
                FloatLongTimsortDescendingKernel.sort(sortKernel, keys, values);
            }
        }
    }


    static void doSort(boolean ascending, int chunkSize, WritableDoubleChunk<Values> values,
            WritableLongChunk<RowKeys> keys) {
        if (ascending) {
            try (final DoubleLongTimsortKernel.DoubleLongSortKernelContext<Values, RowKeys> sortKernel =
                    DoubleLongTimsortKernel.createContext(chunkSize)) {
                DoubleLongTimsortKernel.sort(sortKernel, keys, values);
            }
        } else {
            try (final DoubleLongTimsortDescendingKernel.DoubleLongSortKernelContext<Values, RowKeys> sortKernel =
                    DoubleLongTimsortDescendingKernel.createContext(chunkSize)) {
                DoubleLongTimsortDescendingKernel.sort(sortKernel, keys, values);
            }
        }
    }

    static void doSort(boolean ascending, int chunkSize, WritableObjectChunk<Object, Values> values,
            WritableLongChunk<RowKeys> keys) {
        if (ascending) {
            try (final ObjectLongTimsortKernel.ObjectLongSortKernelContext<Values, RowKeys> sortKernel =
                    ObjectLongTimsortKernel.createContext(chunkSize)) {
                ObjectLongTimsortKernel.sort(sortKernel, keys, values);
            }
        } else {
            try (final ObjectLongTimsortDescendingKernel.ObjectLongSortKernelContext<Values, RowKeys> sortKernel =
                    ObjectLongTimsortDescendingKernel.createContext(chunkSize)) {
                ObjectLongTimsortDescendingKernel.sort(sortKernel, keys, values);
            }
        }
    }

}
