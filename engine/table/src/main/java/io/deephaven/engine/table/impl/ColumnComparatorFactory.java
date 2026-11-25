//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.ColumnSource;

import java.util.Comparator;

import static io.deephaven.util.QueryConstants.*;

public class ColumnComparatorFactory {
    /**
     * Produce a {@link HeldTargetComparator} that compares the current value of one row of {@code source}, the target,
     * with the previous values of other rows. {@link HeldTargetComparator#setTarget} reads the target's value once and
     * holds it (as a primitive for a primitive source) for every later comparison.
     * <p>
     * For a primitive source, values equal by {@code ==} compare equal, otherwise the null value sorts first, and other
     * values are ordered by the boxed type's {@code compare}; for an Object source, {@code comparator} decides when it
     * is non-null, and otherwise {@code null} sorts first and other values are ordered by {@link Comparable#compareTo}.
     *
     * @param source the ColumnSource whose current target value is compared with its previous values
     * @param comparator the comparator for an Object source, or null to use the values' natural order
     * @return a HeldTargetComparator over {@code source}
     */
    public static HeldTargetComparator createHeldTargetComparatorCurrToPrev(final ColumnSource<?> source,
            final Comparator<?> comparator) {
        final Class<?> type = source.getType();
        if (type == byte.class) {
            return new ByteHeldTargetComparator(source);
        }
        if (type == char.class) {
            return new CharHeldTargetComparator(source);
        }
        if (type == short.class) {
            return new ShortHeldTargetComparator(source);
        }
        if (type == int.class) {
            return new IntHeldTargetComparator(source);
        }
        if (type == long.class) {
            return new LongHeldTargetComparator(source);
        }
        if (type == float.class) {
            return new FloatHeldTargetComparator(source);
        }
        if (type == double.class) {
            return new DoubleHeldTargetComparator(source);
        }
        if (comparator != null) {
            // noinspection unchecked
            return new ObjectHeldTargetComparator(source, (Comparator<Object>) comparator);
        }
        return new ComparableHeldTargetComparator(source);
    }

    public interface IComparator {
        int compare(long pos1, long pos2);
    }

    /**
     * Compares one held value, the current value of a target row, with the previous values of other rows of the same
     * ColumnSource.
     */
    public interface HeldTargetComparator {
        /**
         * Read and hold the current value at {@code targetKey}.
         *
         * @param targetKey the row key of the target
         */
        void setTarget(long targetKey);

        /**
         * Compare the held target value with the previous value at {@code rowKey}.
         *
         * @param rowKey the row key whose previous value is compared
         * @return a negative number, zero, or a positive number as the target value is less than, equal to, or greater
         *         than the previous value at {@code rowKey}
         */
        int compareTargetToPrev(long rowKey);
    }

    private static final class ByteHeldTargetComparator implements HeldTargetComparator {
        private final ColumnSource<?> source;
        private byte target;

        private ByteHeldTargetComparator(final ColumnSource<?> source) {
            this.source = source;
        }

        @Override
        public void setTarget(final long targetKey) {
            target = source.getByte(targetKey);
        }

        @Override
        public int compareTargetToPrev(final long rowKey) {
            final byte prev = source.getPrevByte(rowKey);
            return target == prev ? 0
                    : (target == NULL_BYTE ? -1 : (prev == NULL_BYTE ? 1 : Byte.compare(target, prev)));
        }
    }

    private static final class CharHeldTargetComparator implements HeldTargetComparator {
        private final ColumnSource<?> source;
        private char target;

        private CharHeldTargetComparator(final ColumnSource<?> source) {
            this.source = source;
        }

        @Override
        public void setTarget(final long targetKey) {
            target = source.getChar(targetKey);
        }

        @Override
        public int compareTargetToPrev(final long rowKey) {
            final char prev = source.getPrevChar(rowKey);
            return target == prev ? 0
                    : (target == NULL_CHAR ? -1 : (prev == NULL_CHAR ? 1 : Character.compare(target, prev)));
        }
    }

    private static final class ShortHeldTargetComparator implements HeldTargetComparator {
        private final ColumnSource<?> source;
        private short target;

        private ShortHeldTargetComparator(final ColumnSource<?> source) {
            this.source = source;
        }

        @Override
        public void setTarget(final long targetKey) {
            target = source.getShort(targetKey);
        }

        @Override
        public int compareTargetToPrev(final long rowKey) {
            final short prev = source.getPrevShort(rowKey);
            return target == prev ? 0
                    : (target == NULL_SHORT ? -1 : (prev == NULL_SHORT ? 1 : Short.compare(target, prev)));
        }
    }

    private static final class IntHeldTargetComparator implements HeldTargetComparator {
        private final ColumnSource<?> source;
        private int target;

        private IntHeldTargetComparator(final ColumnSource<?> source) {
            this.source = source;
        }

        @Override
        public void setTarget(final long targetKey) {
            target = source.getInt(targetKey);
        }

        @Override
        public int compareTargetToPrev(final long rowKey) {
            final int prev = source.getPrevInt(rowKey);
            return target == prev ? 0
                    : (target == NULL_INT ? -1 : (prev == NULL_INT ? 1 : Integer.compare(target, prev)));
        }
    }

    private static final class LongHeldTargetComparator implements HeldTargetComparator {
        private final ColumnSource<?> source;
        private long target;

        private LongHeldTargetComparator(final ColumnSource<?> source) {
            this.source = source;
        }

        @Override
        public void setTarget(final long targetKey) {
            target = source.getLong(targetKey);
        }

        @Override
        public int compareTargetToPrev(final long rowKey) {
            final long prev = source.getPrevLong(rowKey);
            return target == prev ? 0
                    : (target == NULL_LONG ? -1 : (prev == NULL_LONG ? 1 : Long.compare(target, prev)));
        }
    }

    private static final class FloatHeldTargetComparator implements HeldTargetComparator {
        private final ColumnSource<?> source;
        private float target;

        private FloatHeldTargetComparator(final ColumnSource<?> source) {
            this.source = source;
        }

        @Override
        public void setTarget(final long targetKey) {
            target = source.getFloat(targetKey);
        }

        @Override
        public int compareTargetToPrev(final long rowKey) {
            final float prev = source.getPrevFloat(rowKey);
            return target == prev ? 0
                    : (target == NULL_FLOAT ? -1 : (prev == NULL_FLOAT ? 1 : Float.compare(target, prev)));
        }
    }

    private static final class DoubleHeldTargetComparator implements HeldTargetComparator {
        private final ColumnSource<?> source;
        private double target;

        private DoubleHeldTargetComparator(final ColumnSource<?> source) {
            this.source = source;
        }

        @Override
        public void setTarget(final long targetKey) {
            target = source.getDouble(targetKey);
        }

        @Override
        public int compareTargetToPrev(final long rowKey) {
            final double prev = source.getPrevDouble(rowKey);
            return target == prev ? 0
                    : (target == NULL_DOUBLE ? -1 : (prev == NULL_DOUBLE ? 1 : Double.compare(target, prev)));
        }
    }

    private static final class ObjectHeldTargetComparator implements HeldTargetComparator {
        private final ColumnSource<?> source;
        private final Comparator<Object> comparator;
        private Object target;

        private ObjectHeldTargetComparator(final ColumnSource<?> source, final Comparator<Object> comparator) {
            this.source = source;
            this.comparator = comparator;
        }

        @Override
        public void setTarget(final long targetKey) {
            target = source.get(targetKey);
        }

        @Override
        public int compareTargetToPrev(final long rowKey) {
            return comparator.compare(target, source.getPrev(rowKey));
        }
    }

    private static final class ComparableHeldTargetComparator implements HeldTargetComparator {
        private final ColumnSource<?> source;
        private Comparable<Object> target;

        private ComparableHeldTargetComparator(final ColumnSource<?> source) {
            this.source = source;
        }

        @Override
        public void setTarget(final long targetKey) {
            // noinspection unchecked
            target = (Comparable<Object>) source.get(targetKey);
        }

        @Override
        public int compareTargetToPrev(final long rowKey) {
            final Object prev = source.getPrev(rowKey);
            return target == prev ? 0 : (target == null ? -1 : (prev == null ? 1 : target.compareTo(prev)));
        }
    }
}
