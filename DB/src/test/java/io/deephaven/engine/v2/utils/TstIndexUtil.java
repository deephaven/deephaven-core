package io.deephaven.engine.v2.utils;

import io.deephaven.engine.v2.utils.sortedranges.SortedRanges;
import io.deephaven.engine.v2.utils.rsp.RspBitmap;
import org.apache.commons.lang3.mutable.MutableObject;

public class TstIndexUtil {
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

    public static class BuilderToRangeConsumer implements LongRangeAbortableConsumer {
        private RowSetBuilder builder;

        private BuilderToRangeConsumer(final RowSetBuilder builder) {
            this.builder = builder;
        }

        public static BuilderToRangeConsumer adapt(final RowSetBuilder builder) {
            return new BuilderToRangeConsumer(builder);
        }

        @Override
        public boolean accept(final long start, final long end) {
            builder.addRange(start, end);
            return true;
        }
    }

    public static TrackingMutableRowSet indexFromString(final String str, final RowSetBuilder builder) {
        final BuilderToRangeConsumer adaptor = BuilderToRangeConsumer.adapt(builder);
        stringToRanges(str, adaptor);
        return builder.build();
    }

    public static TrackingMutableRowSet indexFromString(String string) {
        final RowSetBuilder builder = TrackingMutableRowSet.FACTORY.getBuilder();
        return indexFromString(string, builder);
    }

    public static final class RowSetToBuilderAdaptor implements RowSetBuilder {
        private final TrackingMutableRowSet ix;

        public RowSetToBuilderAdaptor(final TrackingMutableRowSet ix) {
            this.ix = ix;
        }

        @Override
        public MutableRowSet build() {
            return ix;
        }

        @Override
        public void addKey(final long key) {
            ix.insert(key);
        }

        @Override
        public void addRange(final long firstKey, final long lastKey) {
            ix.insertRange(firstKey, lastKey);
        }

        public static RowSetToBuilderAdaptor adapt(final TrackingMutableRowSet ix) {
            return new RowSetToBuilderAdaptor(ix);
        }
    }

    public static TrackingMutableRowSet indexFromString(String string, final TrackingMutableRowSet ix) {
        return indexFromString(string, RowSetToBuilderAdaptor.adapt(ix));
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
