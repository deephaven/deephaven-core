//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.pmt;

import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.util.QueryConstants;

import java.time.Instant;

/**
 * Null out a range from an WritableColumnSource.
 * <p>
 * This is done as an interface rather than using the set call with null to avoid per-cell virtual calls. The
 * WritableColumnSource implementations marks their set methods final so that they are avoided.
 */
public interface ColumnSourceNuller {
    void nullColumnSource(WritableColumnSource<?> source, long start, long count);

    static ColumnSourceNuller makeNuller(Class<?> type) {
        if (type == char.class) {
            return CharNuller.INSTANCE;
        }
        if (type == byte.class) {
            return ByteNuller.INSTANCE;
        }
        if (type == short.class) {
            return ShortNuller.INSTANCE;
        }
        if (type == int.class) {
            return IntNuller.INSTANCE;
        }
        if (type == long.class) {
            return LongNuller.INSTANCE;
        }
        if (type == float.class) {
            return FloatNuller.INSTANCE;
        }
        if (type == double.class) {
            return DoubleNuller.INSTANCE;
        }
        if (type == Instant.class) {
            return NanosBasedTimeNuller.INSTANCE;
        }
        return ObjectNuller.INSTANCE;
    }

    class CharNuller implements ColumnSourceNuller {
        private static final CharNuller INSTANCE = new CharNuller();

        @Override
        public void nullColumnSource(final WritableColumnSource<?> source, final long start, final long count) {
            final CharacterArraySource cs = (CharacterArraySource) source;
            for (long ii = start; ii < start + count; ++ii) {
                cs.set(ii, QueryConstants.NULL_CHAR);
            }
        }
    }

    class ByteNuller implements ColumnSourceNuller {
        private static final ByteNuller INSTANCE = new ByteNuller();

        @Override
        public void nullColumnSource(final WritableColumnSource<?> source, final long start, final long count) {
            final ByteArraySource cs = (ByteArraySource) source;
            for (long ii = start; ii < start + count; ++ii) {
                cs.set(ii, QueryConstants.NULL_BYTE);
            }
        }
    }

    class ShortNuller implements ColumnSourceNuller {
        private static final ShortNuller INSTANCE = new ShortNuller();

        @Override
        public void nullColumnSource(final WritableColumnSource<?> source, final long start, final long count) {
            final ShortArraySource cs = (ShortArraySource) source;
            for (long ii = start; ii < start + count; ++ii) {
                cs.set(ii, QueryConstants.NULL_SHORT);
            }
        }
    }

    class IntNuller implements ColumnSourceNuller {
        private static final IntNuller INSTANCE = new IntNuller();

        @Override
        public void nullColumnSource(final WritableColumnSource<?> source, final long start, final long count) {
            final IntegerArraySource cs = (IntegerArraySource) source;
            for (long ii = start; ii < start + count; ++ii) {
                cs.set(ii, QueryConstants.NULL_INT);
            }
        }
    }

    class LongNuller implements ColumnSourceNuller {
        private static final LongNuller INSTANCE = new LongNuller();

        @Override
        public void nullColumnSource(final WritableColumnSource<?> source, final long start, final long count) {
            final LongArraySource cs = (LongArraySource) source;
            for (long ii = start; ii < start + count; ++ii) {
                cs.set(ii, QueryConstants.NULL_LONG);
            }
        }
    }

    class FloatNuller implements ColumnSourceNuller {
        private static final FloatNuller INSTANCE = new FloatNuller();

        @Override
        public void nullColumnSource(final WritableColumnSource<?> source, final long start, final long count) {
            final FloatArraySource cs = (FloatArraySource) source;
            for (long ii = start; ii < start + count; ++ii) {
                cs.set(ii, QueryConstants.NULL_FLOAT);
            }
        }
    }

    class DoubleNuller implements ColumnSourceNuller {
        private static final DoubleNuller INSTANCE = new DoubleNuller();

        @Override
        public void nullColumnSource(final WritableColumnSource<?> source, final long start, final long count) {
            final DoubleArraySource cs = (DoubleArraySource) source;
            for (long ii = start; ii < start + count; ++ii) {
                cs.set(ii, QueryConstants.NULL_DOUBLE);
            }
        }
    }

    class NanosBasedTimeNuller implements ColumnSourceNuller {
        private static final NanosBasedTimeNuller INSTANCE = new NanosBasedTimeNuller();

        @Override
        public void nullColumnSource(final WritableColumnSource<?> source, final long start, final long count) {
            final NanosBasedTimeArraySource<?> cs = (NanosBasedTimeArraySource<?>) source;
            for (long ii = start; ii < start + count; ++ii) {
                cs.set(ii, QueryConstants.NULL_LONG);
            }
        }
    }

    class ObjectNuller implements ColumnSourceNuller {
        private static final ObjectNuller INSTANCE = new ObjectNuller();

        @Override
        public void nullColumnSource(final WritableColumnSource<?> source, final long start, final long count) {
            final ObjectArraySource<?> cs = (ObjectArraySource<?>) source;
            for (long ii = start; ii < start + count; ++ii) {
                cs.set(ii, null);
            }
        }
    }
}
