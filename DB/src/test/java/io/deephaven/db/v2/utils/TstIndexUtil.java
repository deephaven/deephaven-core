package io.deephaven.db.v2.utils;

import io.deephaven.db.v2.utils.sortedranges.SortedRanges;
import io.deephaven.db.v2.utils.rsp.RspBitmap;
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
        private IndexBuilder builder;

        private BuilderToRangeConsumer(final IndexBuilder builder) {
            this.builder = builder;
        }

        public static BuilderToRangeConsumer adapt(final IndexBuilder builder) {
            return new BuilderToRangeConsumer(builder);
        }

        @Override
        public boolean accept(final long start, final long end) {
            builder.addRange(start, end);
            return true;
        }
    }

    public static Index indexFromString(final String str, final IndexBuilder builder) {
        final BuilderToRangeConsumer adaptor = BuilderToRangeConsumer.adapt(builder);
        stringToRanges(str, adaptor);
        return builder.getIndex();
    }

    public static Index indexFromString(String string) {
        final IndexBuilder builder = Index.FACTORY.getBuilder();
        return indexFromString(string, builder);
    }

    public static final class IndexToBuilderAdaptor implements IndexBuilder {
        private final Index ix;

        public IndexToBuilderAdaptor(final Index ix) {
            this.ix = ix;
        }

        @Override
        public Index getIndex() {
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

        public static IndexToBuilderAdaptor adapt(final Index ix) {
            return new IndexToBuilderAdaptor(ix);
        }
    }

    public static Index indexFromString(String string, final Index ix) {
        return indexFromString(string, IndexToBuilderAdaptor.adapt(ix));
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
