/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base;

public class Predicate {

    public interface Nullary {
        public boolean call();
    }

    public interface Int {
        public boolean call(int arg);

        public static final Int ALWAYS_TRUE = new Int() {
            public boolean call(final int arg) {
                return true;
            }
        };

        public static final Int ALWAYS_FALSE = new Int() {
            public boolean call(final int arg) {
                return false;
            }
        };
    }

    public interface Long {
        public boolean call(long arg);
    }

    public interface Unary<A> {
        public boolean call(A arg);
    }

    public interface Binary<A1, A2> {
        public boolean call(A1 arg, A2 arg2);
    }

    public interface Ternary<A1, A2, A3> {
        public boolean call(A1 arg, A2 arg2, A3 arg3);
    }

    public interface DoubleDouble {
        public boolean call(double arg1, double arg2);
    }

    public interface LongDouble {
        public boolean call(long arg1, double arg2);
    }
}
