/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base;

@SuppressWarnings("unused") // There's no reason to force anyone to figure out that 5-ary is quinary (etc) ever again,
                            // so don't complain about unused interfaces.
public class Function {

    @FunctionalInterface
    public interface Nullary<R> {
        R call();
    }

    @FunctionalInterface
    public interface ThrowingNullary<R, ExceptionType extends Exception> {
        R call() throws ExceptionType;
    }

    @FunctionalInterface
    public interface Long_Nullary {
        long call();
    }

    @FunctionalInterface
    public interface Char_Int {
        char call(int arg);
    }

    @FunctionalInterface
    public interface Unary_Int<R> {
        R call(int arg);
    }

    @FunctionalInterface
    public interface Unary_Int_Int<R> {
        R call(int arg, int arg2);
    }

    @FunctionalInterface
    public interface Unary_Int_Boolean<R> {
        R call(int arg, boolean arg2);
    }

    @FunctionalInterface
    public interface Unary_Long<R> {
        R call(long arg);
    }

    @FunctionalInterface
    public interface Unary<R, A> {
        R call(A arg);
    }

    @FunctionalInterface
    public interface Binary<R, A1, A2> {
        R call(A1 arg, A2 arg2);
    }

    @FunctionalInterface
    public interface Ternary_Int<A1, A2, A3> {
        int call(A1 arg, A2 arg2, A3 arg3);
    }

    @FunctionalInterface
    public interface Ternary<R, A1, A2, A3> {
        R call(A1 arg, A2 arg2, A3 arg3);
    }

    @FunctionalInterface
    public interface Quaternary<R, A1, A2, A3, A4> {
        R call(A1 arg, A2 arg2, A3 arg3, A4 arg4);
    }

    @FunctionalInterface
    public interface Quinary<R, A1, A2, A3, A4, A5> {
        R call(A1 arg, A2 arg2, A3 arg3, A4 arg4, A5 arg5);
    }

    @FunctionalInterface
    public interface Senary<R, A1, A2, A3, A4, A5, A6> {
        R call(A1 arg, A2 arg2, A3 arg3, A4 arg4, A5 arg5, A6 arg6);
    }

    @FunctionalInterface
    public interface Septenary<R, A1, A2, A3, A4, A5, A6, A7> {
        R call(A1 arg, A2 arg2, A3 arg3, A4 arg4, A5 arg5, A6 arg6, A7 arg7);
    }

    @FunctionalInterface
    public interface Octonary<R, A1, A2, A3, A4, A5, A6, A7, A8> {
        R call(A1 arg, A2 arg2, A3 arg3, A4 arg4, A5 arg5, A6 arg6, A7 arg7, A8 arg8);
    }

    @FunctionalInterface
    public interface Int {
        int call();
    }

    @FunctionalInterface
    public interface Long {
        long call();
    }

    @FunctionalInterface
    public interface Double {
        double call();
    }

    @FunctionalInterface
    public interface Double_ObjectInt<A> {
        double call(A arg, int i);
    }

    @FunctionalInterface
    public interface Double_ObjectObjectInt<A> {
        double call(A arg1, A arg2, int i);
    }

    @FunctionalInterface
    public interface Object_Int<R> {
        R call(int arg);
    }

    @FunctionalInterface
    public interface Object_Long<R> {
        R call(long arg);
    }

    @FunctionalInterface
    public interface Int_IntIntIntInt {
        int call(int arg, int arg2, int arg3, int arg4);
    }

    @FunctionalInterface
    public interface Int_Object<A> {
        int call(A arg);
    }

    @FunctionalInterface
    public interface Long_Long<R> {
        long call(long arg);
    }

    @FunctionalInterface
    public interface Double_Int {
        double call(int arg);
    }

    @FunctionalInterface
    public interface Double_DoubleDouble {
        double call(double arg1, double arg2);
    }

    @FunctionalInterface
    public interface Boolean_Long {
        boolean call(long arg);
    }
}
