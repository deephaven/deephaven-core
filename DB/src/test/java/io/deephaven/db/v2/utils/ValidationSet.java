
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.utils;

import gnu.trove.procedure.TLongProcedure;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;

import java.util.Arrays;
import java.util.function.BiFunction;

public class ValidationSet {

    public static TLongSet make(final int sz) {
        return new TLongHashSet(sz, 0.5f, -1L);
    }

    public static String set2str(final String pfx, final TLongSet set) {
        final long[] es = set.toArray();
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
        TLongSet apply(TLongSet h1, TLongSet h2);
    }

    public static final Op unionOp = new Op() {
        @Override
        public TLongSet apply(final TLongSet h1, final TLongSet h2) {
            final TLongSet r = make(h1.size() + h2.size());
            h1.forEach(new TLongProcedure() {
                @Override
                public boolean execute(final long v) {
                    r.add(v);
                    return true;
                }
            });
            h2.forEach(new TLongProcedure() {
                @Override
                public boolean execute(final long v) {
                    r.add(v);
                    return true;
                }
            });
            return r;
        }
    };

    public static final Op intersectOp = new Op() {
        @Override
        public TLongSet apply(final TLongSet h1, final TLongSet h2) {
            final TLongSet r = make(Math.min(h1.size(), h2.size()));
            final TLongSet driver;
            final TLongSet other;
            if (h1.size() <= h2.size()) {
                driver = h1;
                other = h2;
            } else {
                driver = h2;
                other = h1;
            }
            driver.forEach(new TLongProcedure() {
                @Override
                public boolean execute(final long v) {
                    if (other.contains(v))
                        r.add(v);
                    return true;
                }
            });
            return r;
        }
    };

    public static final BiFunction<TLongSet, TLongSet, Boolean> overlapOp =
        (h1, h2) -> !h1.forEach(v -> !h2.contains(v));
    public static final BiFunction<TLongSet, TLongSet, Boolean> subsetOfOp =
        (h1, h2) -> h1.forEach(h2::contains);

    public static final Op subtractOp = new Op() {
        @Override
        public TLongSet apply(final TLongSet h1, final TLongSet h2) {
            final TLongSet r = make(h1.size());
            h1.forEach(new TLongProcedure() {
                @Override
                public boolean execute(final long v) {
                    if (!h2.contains(v))
                        r.add(v);
                    return true;
                }
            });
            return r;
        }
    };

    public static final Op inverseSubtractOp = new Op() {
        @Override
        public TLongSet apply(final TLongSet h1, final TLongSet h2) {
            final TLongSet r = make(h2.size());
            h2.forEach(new TLongProcedure() {
                @Override
                public boolean execute(final long v) {
                    if (!h1.contains(v))
                        r.add(v);
                    return true;
                }
            });
            return r;
        }
    };

    public interface TriOp {
        TLongSet apply(TLongSet h1, TLongSet h2, TLongSet h3);
    }

    public static final TriOp updateOp = new TriOp() {
        @Override
        public TLongSet apply(TLongSet h1, TLongSet h2, TLongSet h3) {
            return unionOp.apply(subtractOp.apply(h1, h3), h2);
        }
    };

    public static void dump(final String label, final TLongSet set, final int sizeDumpThreshold) {
        System.out.print("*** " + label + " ==> sz = " + set.size() + "|");
        if (set.size() > sizeDumpThreshold) {
            System.out.println("...");
            return;
        }
        final StringBuilder sb = new StringBuilder();
        final long[] a = set.toArray();
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
