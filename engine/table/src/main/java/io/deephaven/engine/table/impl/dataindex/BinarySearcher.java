package io.deephaven.engine.table.impl.dataindex;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.vector.*;
import io.deephaven.function.BinSearch;
import io.deephaven.function.BinSearchAlgo;

public abstract class BinarySearcher {
    abstract int searchFirst(final Object key, int fromIndex, int toIndex);

    abstract int searchLast(final Object key, int fromIndex, int toIndex);

    private static class ObjectBinarySearcher<T extends Comparable<? super T>> extends BinarySearcher {
        private final ObjectVectorColumnWrapper<T> vector;

        ObjectBinarySearcher(final ColumnSource<T> columnSource, final RowSet rowSet) {
            this.vector = new ObjectVectorColumnWrapper<T>(columnSource, rowSet);
        }

        @Override
        public int searchFirst(final Object key, int fromIndex, int toIndex) {
            return BinSearch.rawBinSearchIndex(vector, fromIndex, toIndex, (T)key, BinSearchAlgo.BS_LOWEST);
        }
        @Override
        public int searchLast(final Object key, int fromIndex, int toIndex) {
            return BinSearch.rawBinSearchIndex(vector, fromIndex, toIndex, (T)key, BinSearchAlgo.BS_HIGHEST);
        }
    }

    private static class ByteBinarySearcher extends BinarySearcher {
        private final ByteVectorColumnWrapper vector;

        ByteBinarySearcher(final ColumnSource<Byte> columnSource, final RowSet rowSet) {
            this.vector = new ByteVectorColumnWrapper(columnSource, rowSet);
        }

        @Override
        public int searchFirst(final Object key, int fromIndex, int toIndex) {
            return BinSearch.rawBinSearchIndex(vector, fromIndex, toIndex, (byte) key, BinSearchAlgo.BS_LOWEST);
        }
        @Override
        public int searchLast(final Object key, int fromIndex, int toIndex) {
            return BinSearch.rawBinSearchIndex(vector, fromIndex, toIndex, (byte) key, BinSearchAlgo.BS_HIGHEST);
        }
    }

    private static class CharBinarySearcher extends BinarySearcher {
        private final CharVectorColumnWrapper vector;

        CharBinarySearcher(final ColumnSource<Character> columnSource, final RowSet rowSet) {
            this.vector = new CharVectorColumnWrapper(columnSource, rowSet);
        }

        @Override
        public int searchFirst(final Object key, int fromIndex, int toIndex) {
            return BinSearch.rawBinSearchIndex(vector, fromIndex, toIndex, (char) key, BinSearchAlgo.BS_LOWEST);
        }
        @Override
        public int searchLast(final Object key, int fromIndex, int toIndex) {
            return BinSearch.rawBinSearchIndex(vector, fromIndex, toIndex, (char) key, BinSearchAlgo.BS_HIGHEST);
        }
    }

    private static class DoubleBinarySearcher extends BinarySearcher {
        private final DoubleVectorColumnWrapper vector;

        DoubleBinarySearcher(final ColumnSource<Double> columnSource, final RowSet rowSet) {
            this.vector = new DoubleVectorColumnWrapper(columnSource, rowSet);
        }

        @Override
        public int searchFirst(final Object key, int fromIndex, int toIndex) {
            return BinSearch.rawBinSearchIndex(vector, fromIndex, toIndex, (double) key, BinSearchAlgo.BS_LOWEST);
        }
        @Override
        public int searchLast(final Object key, int fromIndex, int toIndex) {
            return BinSearch.rawBinSearchIndex(vector, fromIndex, toIndex, (double) key, BinSearchAlgo.BS_HIGHEST);
        }
    }

    private static class FloatBinarySearcher extends BinarySearcher {
        private final FloatVectorColumnWrapper vector;

        FloatBinarySearcher(final ColumnSource<Float> columnSource, final RowSet rowSet) {
            this.vector = new FloatVectorColumnWrapper(columnSource, rowSet);
        }

        @Override
        public int searchFirst(final Object key, int fromIndex, int toIndex) {
            return BinSearch.rawBinSearchIndex(vector, fromIndex, toIndex, (float) key, BinSearchAlgo.BS_LOWEST);
        }
        @Override
        public int searchLast(final Object key, int fromIndex, int toIndex) {
            return BinSearch.rawBinSearchIndex(vector, fromIndex, toIndex, (float) key, BinSearchAlgo.BS_HIGHEST);
        }
    }

    private static class IntBinarySearcher extends BinarySearcher {
        private final IntVectorColumnWrapper vector;

        IntBinarySearcher(final ColumnSource<Integer> columnSource, final RowSet rowSet) {
            this.vector = new IntVectorColumnWrapper(columnSource, rowSet);
        }

        @Override
        public int searchFirst(final Object key, int fromIndex, int toIndex) {
            return BinSearch.rawBinSearchIndex(vector, fromIndex, toIndex, (int) key, BinSearchAlgo.BS_LOWEST);
        }
        @Override
        public int searchLast(final Object key, int fromIndex, int toIndex) {
            return BinSearch.rawBinSearchIndex(vector, fromIndex, toIndex, (int) key, BinSearchAlgo.BS_HIGHEST);
        }
    }

    private static class LongBinarySearcher extends BinarySearcher {
        private final LongVectorColumnWrapper vector;

        LongBinarySearcher(final ColumnSource<Long> columnSource, final RowSet rowSet) {
            this.vector = new LongVectorColumnWrapper(columnSource, rowSet);
        }

        @Override
        public int searchFirst(final Object key, int fromIndex, int toIndex) {
            return BinSearch.rawBinSearchIndex(vector, fromIndex, toIndex, (long) key, BinSearchAlgo.BS_LOWEST);
        }
        @Override
        public int searchLast(final Object key, int fromIndex, int toIndex) {
            return BinSearch.rawBinSearchIndex(vector, fromIndex, toIndex, (long) key, BinSearchAlgo.BS_HIGHEST);
        }
    }

    private static class ShortBinarySearcher extends BinarySearcher {
        private final ShortVectorColumnWrapper vector;

        ShortBinarySearcher(final ColumnSource<Short> columnSource, final RowSet rowSet) {
            this.vector = new ShortVectorColumnWrapper(columnSource, rowSet);
        }

        @Override
        public int searchFirst(final Object key, int fromIndex, int toIndex) {
            return BinSearch.rawBinSearchIndex(vector, fromIndex, toIndex, (short) key, BinSearchAlgo.BS_LOWEST);
        }
        @Override
        public int searchLast(final Object key, int fromIndex, int toIndex) {
            return BinSearch.rawBinSearchIndex(vector, fromIndex, toIndex, (short) key, BinSearchAlgo.BS_HIGHEST);
        }
    }

    @SuppressWarnings("unchecked")
    public static BinarySearcher from(ColumnSource<?> cs, RowSet rowSet) {
        final Class<?> type = cs.getType();
        if (type == Boolean.class) {
            return new ObjectBinarySearcher<>((ColumnSource<Boolean>) cs, rowSet);
        }
        if (type == byte.class) {
            return new ByteBinarySearcher((ColumnSource<Byte>) cs, rowSet);
        }
        if (type == char.class) {
            return new CharBinarySearcher((ColumnSource<Character>) cs, rowSet);
        }
        if (type == double.class) {
            return new DoubleBinarySearcher((ColumnSource<Double>) cs, rowSet);
        }
        if (type == float.class) {
            return new FloatBinarySearcher((ColumnSource<Float>) cs, rowSet);
        }
        if (type == int.class) {
            return new IntBinarySearcher((ColumnSource<Integer>) cs, rowSet);
        }
        if (type == long.class) {
            return new LongBinarySearcher((ColumnSource<Long>) cs, rowSet);
        }
        if (type == short.class) {
            return new ShortBinarySearcher((ColumnSource<Short>) cs, rowSet);
        }
        return new ObjectBinarySearcher<>((ColumnSource<? extends Comparable>) cs, rowSet);
    }    
}
