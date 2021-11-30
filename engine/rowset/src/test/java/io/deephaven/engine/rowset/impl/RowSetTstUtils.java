package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.singlerange.SingleRange;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
import io.deephaven.util.datastructures.LongRangeAbortableConsumer;
import org.apache.commons.lang3.mutable.MutableObject;

import java.util.Random;

/**
 * Testing-related RowSet utilities
 */
public class RowSetTstUtils {

    public static WritableRowSet getRandomRowSet(long minValue, int size, Random random) {
        final RowSetBuilderRandom builder = RowSetFactory.builderRandom();
        long previous = minValue;
        for (int i = 0; i < size; i++) {
            previous += (1 + random.nextInt(100));
            builder.addKey(previous);
        }
        return builder.build();
    }

    public static boolean stringToRanges(final String str, final LongRangeAbortableConsumer lrac) {
        final String[] ranges = str.split(",");
        for (String range : ranges) {
            if (range.contains("-")) {
                final String[] parts = range.split("-");
                final long start = Long.parseLong(parts[0]);
                final long end = Long.parseLong(parts[1]);
                if (!lrac.accept(start, end)) {
                    return false;
                }
            } else {
                final long key = Long.parseLong(range);
                if (!lrac.accept(key, key)) {
                    return false;
                }
            }
        }
        return true;
    }

    public static WritableRowSet makeEmptyRsp() {
        return new WritableRowSetImpl(RspBitmap.makeEmpty());
    }

    public static WritableRowSet makeEmptySr() {
        return new WritableRowSetImpl(SortedRanges.makeEmpty());
    }

    public static WritableRowSet makeSingleRange(final long start, final long end) {
        return new WritableRowSetImpl(SingleRange.make(start, end));
    }

    public static class BuilderToRangeConsumer implements LongRangeAbortableConsumer {
        private RowSetBuilderRandom builder;

        private BuilderToRangeConsumer(final RowSetBuilderRandom builder) {
            this.builder = builder;
        }

        public static BuilderToRangeConsumer adapt(final RowSetBuilderRandom builder) {
            return new BuilderToRangeConsumer(builder);
        }

        @Override
        public boolean accept(final long start, final long end) {
            builder.addRange(start, end);
            return true;
        }
    }

    public static WritableRowSet rowSetFromString(final String str, final RowSetBuilderRandom builder) {
        final BuilderToRangeConsumer adaptor = BuilderToRangeConsumer.adapt(builder);
        stringToRanges(str, adaptor);
        return builder.build();
    }

    public static WritableRowSet rowSetFromString(String string) {
        final RowSetBuilderRandom builder = RowSetFactory.builderRandom();
        return rowSetFromString(string, builder);
    }

    public static final class RowSetToBuilderRandomAdaptor implements RowSetBuilderRandom {

        private final WritableRowSet rs;

        public RowSetToBuilderRandomAdaptor(final WritableRowSet rs) {
            this.rs = rs;
        }

        @Override
        public WritableRowSet build() {
            return rs;
        }

        @Override
        public void addKey(final long rowKey) {
            rs.insert(rowKey);
        }

        @Override
        public void addRange(final long firstRowKey, final long lastRowKey) {
            rs.insertRange(firstRowKey, lastRowKey);
        }

        public static RowSetToBuilderRandomAdaptor adapt(final WritableRowSet rs) {
            return new RowSetToBuilderRandomAdaptor(rs);
        }
    }

    public static WritableRowSet rowSetFromString(String string, final WritableRowSet ix) {
        return rowSetFromString(string, RowSetToBuilderRandomAdaptor.adapt(ix));
    }

    public static SortedRanges sortedRangesFromString(final String str) {
        final MutableObject<SortedRanges> msr = new MutableObject(SortedRanges.makeEmpty());
        final LongRangeAbortableConsumer c = (final long start, final long end) -> {
            SortedRanges ans = msr.getValue();
            ans = ans.addRange(start, end);
            if (ans == null) {
                return false;
            }
            msr.setValue(ans);
            return true;
        };
        stringToRanges(str, c);
        SortedRanges sr = msr.getValue();
        if (sr != null) {
            sr = sr.tryCompactUnsafe(0);
        }
        return sr;
    }

    public static RspBitmap rspFromString(final String str) {
        final RspBitmap rsp = RspBitmap.makeEmpty();
        final LongRangeAbortableConsumer c = (final long start, final long end) -> {
            rsp.addRangeUnsafeNoWriteCheck(0, start, end);
            return true;
        };
        stringToRanges(str, c);
        return rsp;
    }
}
