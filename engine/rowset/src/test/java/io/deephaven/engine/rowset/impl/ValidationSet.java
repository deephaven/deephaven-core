//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.util.Arrays;

public class ValidationSet {

    public static LongOpenHashSet make(final int sz) {
        return new LongOpenHashSet(sz, 0.5f);
    }

    public static String set2str(final String pfx, final LongOpenHashSet set) {
        final long[] es = set.toLongArray();
        Arrays.sort(es);
        StringBuilder sb = new StringBuilder(pfx);
        sb.append("{ ");
        boolean comma = false;
        for (long e : es) {
            if (comma) {
                sb.append(", ");
            }
            sb.append(e);
            comma = true;
        }
        sb.append(" }");
        return sb.toString();
    }

    public interface Op {
        LongOpenHashSet apply(LongOpenHashSet h1, LongOpenHashSet h2);
    }

    public static final Op unionOp = new Op() {
        @Override
        public LongOpenHashSet apply(final LongOpenHashSet h1, final LongOpenHashSet h2) {
            final LongOpenHashSet r = make(h1.size() + h2.size());
            h1.forEach((long v) -> r.add(v));
            h2.forEach((long v) -> r.add(v));
            return r;
        }
    };

    public static final Op subtractOp = new Op() {
        @Override
        public LongOpenHashSet apply(final LongOpenHashSet h1, final LongOpenHashSet h2) {
            final LongOpenHashSet r = make(h1.size());
            h1.forEach((long v) -> {
                if (!h2.contains(v))
                    r.add(v);
            });
            return r;
        }
    };

    @SuppressWarnings("unused")
    public static void dump(final String label, final LongOpenHashSet set, final int sizeDumpThreshold) {
        System.out.print("*** " + label + " ==> sz = " + set.size() + "|");
        if (set.size() > sizeDumpThreshold) {
            System.out.println("...");
            return;
        }
        final StringBuilder sb = new StringBuilder();
        final long[] a = set.toLongArray();
        Arrays.sort(a);
        appendLongArray(sb, a);
        System.out.println(sb.toString());
    }

    public static void appendLongArray(final StringBuilder sb, final long[] a) {
        boolean first = true;
        long pendingRangeStart = -1;
        long pendingRangeEnd = -1;
        for (int i = 0; i < a.length; ++i) {
            if (pendingRangeEnd != -1) {
                if (pendingRangeEnd == a[i] - 1) {
                    pendingRangeEnd = a[i];
                } else {
                    if (!first)
                        sb.append(", ");
                    first = false;
                    sb.append(pendingRangeStart);
                    if (pendingRangeStart != pendingRangeEnd) {
                        sb.append("-");
                        sb.append(pendingRangeEnd);
                    }
                    pendingRangeStart = pendingRangeEnd = a[i];
                }
            } else {
                pendingRangeStart = pendingRangeEnd = a[i];
            }
        }
        if (pendingRangeEnd != -1) {
            if (!first)
                sb.append(", ");
            sb.append(pendingRangeStart);
            if (pendingRangeStart != pendingRangeEnd) {
                sb.append("-");
                sb.append(pendingRangeEnd);
            }
        }
    }
}
