/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base;

@SuppressWarnings("unused") // There's no reason to force anyone to figure out that 5-ary is quinary
                            // (etc) ever again, so don't complain about unused interfaces.
public class Procedure {

    @FunctionalInterface
    public interface Nullary {
        void call();
    }

    @FunctionalInterface
    public interface ThrowingNullary<T extends Exception> {
        void call() throws T;
    }

    @FunctionalInterface
    public interface Unary<A> {
        void call(A arg);

        class Null<A> implements Unary<A> {
            public void call(A arg) {}
        }
    }

    @FunctionalInterface
    public interface UnaryByte {
        void call(byte arg);
    }

    @FunctionalInterface
    public interface UnaryChar {
        void call(char arg);
    }

    @FunctionalInterface
    public interface UnaryFloat {
        void call(float arg);
    }

    @FunctionalInterface
    public interface UnaryDouble {
        void call(double arg);
    }

    @FunctionalInterface
    public interface UnaryInt {
        void call(int arg);
    }

    @FunctionalInterface
    public interface UnaryLong {
        void call(long arg);
    }

    @FunctionalInterface
    public interface UnaryShort {
        void call(short arg);
    }

    @FunctionalInterface
    public interface BinaryInt<A> {
        void call(int arg1, A arg2);
    }

    @FunctionalInterface
    public interface Binary<A1, A2> {
        void call(A1 arg1, A2 arg2);
    }

    @FunctionalInterface
    public interface Ternary<A1, A2, A3> {
        void call(A1 arg1, A2 arg2, A3 arg3);
    }

    @FunctionalInterface
    public interface Quaternary<A1, A2, A3, A4> {
        void call(A1 arg1, A2 arg2, A3 arg3, A4 arg4);
    }

    @FunctionalInterface
    public interface Quinary<A1, A2, A3, A4, A5> {
        void call(A1 arg1, A2 arg2, A3 arg3, A4 arg4, A5 arg5);
    }
}
