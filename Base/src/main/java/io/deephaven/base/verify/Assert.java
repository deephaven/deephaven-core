/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.verify;

import java.awt.EventQueue;
import java.util.function.Consumer;

// --------------------------------------------------------------------
/**
 * Assertion methods for simple runtime program verification. Failed assertions throw {@link AssertionFailure}.
 * <p>
 * Methods:
 * <ul>
 * <li>void assertion(boolean condition, String conditionText[, String detailMessage])
 * <li>void assertion(boolean condition, String conditionText, value0, String name0, value1, String name0, ... )
 * </ul>
 * <ul>
 * <li>void statementNeverExecuted()
 * <li>void statementNeverExecuted(String statementDescription)
 * <li>void exceptionNeverCaught(Exception caughtException)
 * <li>void exceptionNeverCaught(String tryStatementDescription, Exception caughtException)
 * <li>void valueNeverOccurs(value, String name)
 * <li>void valuesNeverOccur(value0, name0, value1, name1, ... )
 * </ul>
 * <ul>
 * <li>void holdsLock/notHoldsLock(Object, String name)
 * </ul>
 * <ul>
 * <li>void instanceOf/notInstanceOf(Object, String name, Class type[, int numCallsBelowRequirer])
 * </ul>
 * <ul>
 * <li>void eq/neq(boolean/char/byte/short/int/long/float/double, String name0,
 * boolean/char/byte/short/int/long/float/double[, String name1])
 * <li>void lt/leq/gt/geq(char/byte/short/int/long/float/double, String name0, char/byte/short/int/long/float/double[,
 * String name1])
 * </ul>
 * <ul>
 * <li>void eqFalse/neqFalse/eqTrue/neqTrue(boolean, String name)
 * <li>void eqZero/neqZero(char/byte/short/int/long/float/double, String name)
 * <li>void ltZero/leqZero/gtZero/geqZero(byte/short/int/long/float/double, String name)
 * </ul>
 * <ul>
 * <li>void eq/neq(Object, name0, Object[, name1])
 * <li>void eqNull/neqNull(Object, String name)
 * </ul>
 * <ul>
 * <li>void equals(Object, String name0, Object, String name1)
 * <li>void nonempty(String, String name)
 * </ul>
 * <p>
 * Naming Rationale:
 * <ul>
 * <li>eq, neq, lt, leq, gt, get correspond to ==, !=, <, <=, >, >=, e.g.,
 * <ul>
 * <li>For Object a and b, Assert.eq(a, "a", b, "b") corresponds to assert (a == b)
 * <li>For Object o, Assert.neqNull(o, "o") corresponds to assert (o != null)
 * <li>for int x, Assert.eqZero(x, "x") corresponds to assert (x == 0)
 * </ul>
 * <li>equals corresponds to Object.equals (preceded by necessary null checks), e.g.,
 * <ul>
 * <li>For Object a and b, Assert.equals(a, "a", b, "b") corresponds to assert (a!= null && b != null && a.equals(b))
 * <li>for String s, Assert.nonempty(s, "s") corresponds to assert (s != null && s.length() != 0)
 * </ul>
 * </ul>
 */
public final class Assert {
    // ################################################################
    static private volatile Consumer<AssertionFailure> onAssertionCallback;

    public static boolean setOnAssertionCallback(Consumer<AssertionFailure> newCallback) {
        final boolean wasSet = onAssertionCallback != null;
        onAssertionCallback = newCallback;
        return wasSet;
    }

    // we should only have static methods
    private Assert() {}

    // ################################################################
    // Handle failed assertions

    // ----------------------------------------------------------------
    private static void fail(String conditionText) {
        final AssertionFailure assertionFailure =
                new AssertionFailure(ExceptionMessageUtil.failureMessage("Assertion", "asserted", conditionText, null));
        if (onAssertionCallback != null) {
            try {
                onAssertionCallback.accept(assertionFailure);
            } catch (Exception ignored) {
            }
        }
        throw assertionFailure;
    }

    // ----------------------------------------------------------------
    private static void fail(String conditionText, String detailMessage) {
        final AssertionFailure assertionFailure = new AssertionFailure(
                ExceptionMessageUtil.failureMessage("Assertion", "asserted", conditionText, detailMessage));
        if (onAssertionCallback != null) {
            try {
                onAssertionCallback.accept(assertionFailure);
            } catch (Exception ignored) {
            }
        }
        throw assertionFailure;
    }

    // ################################################################
    // assertion

    // ----------------------------------------------------------------
    /** assert (condition, conditionText) */
    public static void assertion(boolean condition, String conditionText) {
        if (!(condition)) {
            fail(conditionText);
        }
    }

    // ----------------------------------------------------------------
    /** assert (condition, conditionText, detailMessage) */
    public static void assertion(boolean condition, String conditionText, String detailMessage) {
        if (!(condition)) {
            fail(conditionText, detailMessage);
        }
    }

    // ----------------------------------------------------------------
    /** assert (condition, conditionText, Object o0, String name0, ... ) */
    public static void assertion(boolean condition, String conditionText, Object o0, String name0) {
        if (!(condition)) {
            fail(conditionText, ExceptionMessageUtil.valueAndName(o0, name0));
        }
    }

    public static void assertion(boolean condition, String conditionText, Object o0, String name0, Object o1,
            String name1) {
        if (!(condition)) {
            fail(conditionText, ExceptionMessageUtil.valueAndName(o0, name0, o1, name1));
        }
    }

    public static void assertion(boolean condition, String conditionText, Object o0, String name0, Object o1,
            String name1, Object o2, String name2) {
        if (!(condition)) {
            fail(conditionText, ExceptionMessageUtil.valueAndName(o0, name0, o1, name1, o2, name2));
        }
    }

    public static void assertion(boolean condition, String conditionText, Object o0, String name0, Object o1,
            String name1, int i2, String name2) {
        if (!(condition)) {
            fail(conditionText, ExceptionMessageUtil.concat(ExceptionMessageUtil.valueAndName(o0, name0, o1, name1),
                    ExceptionMessageUtil.valueAndName(i2, name2)));
        }
    }

    public static void assertion(boolean condition, String conditionText, Object o0, String name0, Object o1,
            String name1, Object o2, String name2, Object o3, String name3) {
        if (!(condition)) {
            fail(conditionText, ExceptionMessageUtil.valueAndName(o0, name0, o1, name1, o2, name2, o3, name3));
        }
    }

    // ----------------------------------------------------------------
    /** assert (condition, conditionText, boolean b0, String name0, ... ) */
    public static void assertion(boolean condition, String conditionText, boolean b0, String name0) {
        if (!(condition)) {
            fail(conditionText, ExceptionMessageUtil.valueAndName(b0, name0));
        }
    }

    public static void assertion(boolean condition, String conditionText, boolean b0, String name0, boolean b1,
            String name1) {
        if (!(condition)) {
            fail(conditionText, ExceptionMessageUtil.valueAndName(b0, name0, b1, name1));
        }
    }

    public static void assertion(boolean condition, String conditionText, boolean b0, String name0, boolean b1,
            String name1, boolean b2, String name2) {
        if (!(condition)) {
            fail(conditionText, ExceptionMessageUtil.valueAndName(b0, name0, b1, name1, b2, name2));
        }
    }

    public static void assertion(boolean condition, String conditionText, boolean b0, String name0, boolean b1,
            String name1, boolean b2, String name2, boolean b3, String name3) {
        if (!(condition)) {
            fail(conditionText, ExceptionMessageUtil.valueAndName(b0, name0, b1, name1, b2, name2, b3, name3));
        }
    }

    // ----------------------------------------------------------------
    /** assert (condition, conditionText, int i0, String name0, ... ) */
    public static void assertion(boolean condition, String conditionText, int i0, String name0) {
        if (!(condition)) {
            fail(conditionText, ExceptionMessageUtil.valueAndName(i0, name0));
        }
    }

    public static void assertion(boolean condition, String conditionText, int i0, String name0, int i1, String name1) {
        if (!(condition)) {
            fail(conditionText, ExceptionMessageUtil.valueAndName(i0, name0, i1, name1));
        }
    }

    // ################################################################
    // statementNeverExceuted

    // ----------------------------------------------------------------
    /** assert (this statement is never executed) */
    public static AssertionFailure statementNeverExecuted() {
        fail("statement is never executed");
        return null;
    }

    // ----------------------------------------------------------------
    /** assert (statementDescription is never executed) */
    public static AssertionFailure statementNeverExecuted(String statementDescription) {
        fail(statementDescription + " is never executed");
        return null;
    }

    // ################################################################
    // exceptionNeverCaught

    // ----------------------------------------------------------------
    /** assert (this exception is never caught, Exception e) */
    public static AssertionFailure exceptionNeverCaught(Exception e) {
        try {
            fail(e.getClass().getName() + " is never caught",
                    e.getClass().getName() + "(" + e.getMessage() + ") caught");
        } catch (AssertionFailure assertionFailure) {
            assertionFailure.initCause(e);
            throw assertionFailure;
        }
        return null;
    }

    // ----------------------------------------------------------------
    /** assert (tryStatementDescription succeeds, Exception e) */
    public static AssertionFailure exceptionNeverCaught(String tryStatementDescription, Exception e) {
        try {
            fail(tryStatementDescription + " succeeds", e.getClass().getName() + "(" + e.getMessage() + ") caught");
        } catch (AssertionFailure assertionFailure) {
            assertionFailure.initCause(e);
            throw assertionFailure;
        }
        return null;
    }

    // ################################################################
    // valueNeverOccurs

    // ----------------------------------------------------------------
    /** assert (this value never occurs, Object o, name) */
    public static AssertionFailure valueNeverOccurs(Object o, String name) {
        fail(ExceptionMessageUtil.valueAndName(o, name) + " never occurs");
        return null;
    }

    // ----------------------------------------------------------------
    /** assert (this value never occurs, boolean b, name) */
    public static AssertionFailure valueNeverOccurs(boolean b, String name) {
        fail(ExceptionMessageUtil.valueAndName(b, name) + " never occurs");
        return null;
    }

    // ----------------------------------------------------------------
    /** assert (this value never occurs, char c, name) */
    public static AssertionFailure valueNeverOccurs(char c, String name) {
        fail(ExceptionMessageUtil.valueAndName(c, name) + " never occurs");
        return null;
    }

    // ----------------------------------------------------------------
    /** assert (this value never occurs, byte b, name) */
    public static AssertionFailure valueNeverOccurs(byte b, String name) {
        fail(ExceptionMessageUtil.valueAndName(b, name) + " never occurs");
        return null;
    }

    // ----------------------------------------------------------------
    /** assert (this value never occurs, short s, name) */
    public static AssertionFailure valueNeverOccurs(short s, String name) {
        fail(ExceptionMessageUtil.valueAndName(s, name) + " never occurs");
        return null;
    }

    // ----------------------------------------------------------------
    /** assert (this value never occurs, int i, name) */
    public static AssertionFailure valueNeverOccurs(int i, String name) {
        fail(ExceptionMessageUtil.valueAndName(i, name) + " never occurs");
        return null;
    }

    // ----------------------------------------------------------------
    /** assert (this value never occurs, long l, name) */
    public static AssertionFailure valueNeverOccurs(long l, String name) {
        fail(ExceptionMessageUtil.valueAndName(l, name) + " never occurs");
        return null;
    }

    // ----------------------------------------------------------------
    /** assert (this value never occurs, float f, name) */
    public static AssertionFailure valueNeverOccurs(float f, String name) {
        fail(ExceptionMessageUtil.valueAndName(f, name) + " never occurs");
        return null;
    }

    // ----------------------------------------------------------------
    /** assert (this value never occurs, double d, name) */
    public static AssertionFailure valueNeverOccurs(double d, String name) {
        fail(ExceptionMessageUtil.valueAndName(d, name) + " never occurs");
        return null;
    }

    // ################################################################
    // holdsLock, notHoldsLock

    // ----------------------------------------------------------------
    /** assert (o != null && (current thread holds o's lock)) */
    public static void holdsLock(Object o, String name) {
        neqNull(o, "o");
        if (!Thread.holdsLock(o)) {
            fail("\"" + Thread.currentThread().getName() + "\".holdsLock(" + name + ")");
        }
    }

    // ----------------------------------------------------------------
    /** assert (o != null && !(current thread holds o's lock)) */
    public static void notHoldsLock(Object o, String name) {
        neqNull(o, "o");
        if (Thread.holdsLock(o)) {
            fail("!\"" + Thread.currentThread().getName() + "\".holdsLock(" + name + ")");
        }
    }

    // ################################################################
    // instanceOf, notInstanceOf

    // ----------------------------------------------------------------
    /** assert (o instanceof type) */
    public static void instanceOf(Object o, String name, Class<?> type) {
        if (!type.isInstance(o)) {
            fail(name + " instanceof " + type, null == o ? ExceptionMessageUtil.valueAndName(o, name)
                    : name + " instanceof " + o.getClass() + " (" + ExceptionMessageUtil.valueAndName(o, name) + ")");
        }
    }

    // ----------------------------------------------------------------
    /** assert !(o instanceof type) */
    public static void notInstanceOf(Object o, String name, Class<?> type) {
        if (type.isInstance(o)) {
            fail("!(" + name + " instanceof " + type + ")",
                    name + " instanceof " + o.getClass() + " (" + ExceptionMessageUtil.valueAndName(o, name) + ")");
        }
    }

    // ################################################################
    // isAWTThread, isNotAWTThread

    // ----------------------------------------------------------------
    /** assert (current thread is AWT Event Dispatch Thread) */
    public static void isAWTThread() {
        if (!EventQueue.isDispatchThread()) {
            fail("\"" + Thread.currentThread().getName() + "\".isAWTThread()");
        }
    }

    // ----------------------------------------------------------------
    /** assert (current thread is AWT Event Dispatch Thread) */
    public static void isNotAWTThread() {
        if (EventQueue.isDispatchThread()) {
            fail("!\"" + Thread.currentThread().getName() + "\".isAWTThread()");
        }
    }

    // ################################################################
    // eq (primitiveValue == primitiveValue)

    // ----------------------------------------------------------------
    /** assert (b0 == b1) */
    public static void eq(boolean b0, String name0, boolean b1, String name1) {
        if (!(b0 == b1)) {
            fail(name0 + " == " + name1, ExceptionMessageUtil.valueAndName(b0, name0, b1, name1));
        }
    }

    public static void eq(boolean b0, String name0, boolean b1) {
        if (!(b0 == b1)) {
            fail(name0 + " == " + b1, ExceptionMessageUtil.valueAndName(b0, name0));
        }
    }

    // ----------------------------------------------------------------
    /** assert (c0 == c1) */
    public static void eq(char c0, String name0, char c1, String name1) {
        if (!(c0 == c1)) {
            fail(name0 + " == " + name1, ExceptionMessageUtil.valueAndName(c0, name0, c1, name1));
        }
    }

    public static void eq(char c0, String name0, char c1) {
        if (!(c0 == c1)) {
            fail(name0 + " == " + ExceptionMessageUtil.valueString(c1), ExceptionMessageUtil.valueAndName(c0, name0));
        }
    }

    // ----------------------------------------------------------------
    /** assert (b0 == b1) */
    public static void eq(byte b0, String name0, byte b1, String name1) {
        if (!(b0 == b1)) {
            fail(name0 + " == " + name1, ExceptionMessageUtil.valueAndName(b0, name0, b1, name1));
        }
    }

    public static void eq(byte b0, String name0, byte b1) {
        if (!(b0 == b1)) {
            fail(name0 + " == " + b1, ExceptionMessageUtil.valueAndName(b0, name0));
        }
    }

    // ----------------------------------------------------------------
    /** assert (s0 == s1) */
    public static void eq(short s0, String name0, short s1, String name1) {
        if (!(s0 == s1)) {
            fail(name0 + " == " + name1, ExceptionMessageUtil.valueAndName(s0, name0, s1, name1));
        }
    }

    public static void eq(short s0, String name0, short s1) {
        if (!(s0 == s1)) {
            fail(name0 + " == " + s1, ExceptionMessageUtil.valueAndName(s0, name0));
        }
    }

    // ----------------------------------------------------------------
    /** assert (i0 == i1) */
    public static void eq(int i0, String name0, int i1, String name1) {
        if (!(i0 == i1)) {
            fail(name0 + " == " + name1, ExceptionMessageUtil.valueAndName(i0, name0, i1, name1));
        }
    }

    public static void eq(int i0, String name0, int i1) {
        if (!(i0 == i1)) {
            fail(name0 + " == " + i1, ExceptionMessageUtil.valueAndName(i0, name0));
        }
    }

    // ----------------------------------------------------------------
    /** assert (l0 == l1) */
    public static void eq(long l0, String name0, long l1, String name1) {
        if (!(l0 == l1)) {
            fail(name0 + " == " + name1, ExceptionMessageUtil.valueAndName(l0, name0, l1, name1));
        }
    }

    public static void eq(long l0, String name0, long l1) {
        if (!(l0 == l1)) {
            fail(name0 + " == " + l1, ExceptionMessageUtil.valueAndName(l0, name0));
        }
    }

    // ----------------------------------------------------------------
    /** assert (f0 == f1) */
    public static void eq(float f0, String name0, float f1, String name1) {
        if (!(f0 == f1)) {
            fail(name0 + " == " + name1, ExceptionMessageUtil.valueAndName(f0, name0, f1, name1));
        }
    }

    public static void eq(float f0, String name0, float f1) {
        if (!(f0 == f1)) {
            fail(name0 + " == " + f1, ExceptionMessageUtil.valueAndName(f0, name0));
        }
    }

    // ----------------------------------------------------------------
    /** assert (d0 == d1) */
    public static void eq(double d0, String name0, double d1, String name1) {
        if (!(d0 == d1)) {
            fail(name0 + " == " + name1, ExceptionMessageUtil.valueAndName(d0, name0, d1, name1));
        }
    }

    public static void eq(double d0, String name0, double d1) {
        if (!(d0 == d1)) {
            fail(name0 + " == " + d1, ExceptionMessageUtil.valueAndName(d0, name0));
        }
    }

    // ################################################################
    // neq (primitiveValue != primitiveValue)

    // ----------------------------------------------------------------
    /** assert (b0 != b1) */
    public static void neq(boolean b0, String name0, boolean b1, String name1) {
        if (!(b0 != b1)) {
            fail(name0 + " != " + name1, ExceptionMessageUtil.valueAndName(b0, name0, b1, name1));
        }
    }

    public static void neq(boolean b0, String name0, boolean b1) {
        if (!(b0 != b1)) {
            fail(name0 + " != " + b1, ExceptionMessageUtil.valueAndName(b0, name0));
        }
    }

    // ----------------------------------------------------------------
    /** assert (c0 != c1) */
    public static void neq(char c0, String name0, char c1, String name1) {
        if (!(c0 != c1)) {
            fail(name0 + " != " + name1, ExceptionMessageUtil.valueAndName(c0, name0, c1, name1));
        }
    }

    public static void neq(char c0, String name0, char c1) {
        if (!(c0 != c1)) {
            fail(name0 + " != " + ExceptionMessageUtil.valueString(c1), ExceptionMessageUtil.valueAndName(c0, name0));
        }
    }

    // ----------------------------------------------------------------
    /** assert (b0 != b1) */
    public static void neq(byte b0, String name0, byte b1, String name1) {
        if (!(b0 != b1)) {
            fail(name0 + " != " + name1, ExceptionMessageUtil.valueAndName(b0, name0, b1, name1));
        }
    }

    public static void neq(byte b0, String name0, byte b1) {
        if (!(b0 != b1)) {
            fail(name0 + " != " + b1, ExceptionMessageUtil.valueAndName(b0, name0));
        }
    }

    // ----------------------------------------------------------------
    /** assert (s0 != s1) */
    public static void neq(short s0, String name0, short s1, String name1) {
        if (!(s0 != s1)) {
            fail(name0 + " != " + name1, ExceptionMessageUtil.valueAndName(s0, name0, s1, name1));
        }
    }

    public static void neq(short s0, String name0, short s1) {
        if (!(s0 != s1)) {
            fail(name0 + " != " + s1, ExceptionMessageUtil.valueAndName(s0, name0));
        }
    }

    // ----------------------------------------------------------------
    /** assert (i0 != i1) */
    public static void neq(int i0, String name0, int i1, String name1) {
        if (!(i0 != i1)) {
            fail(name0 + " != " + name1, ExceptionMessageUtil.valueAndName(i0, name0, i1, name1));
        }
    }

    public static void neq(int i0, String name0, int i1) {
        if (!(i0 != i1)) {
            fail(name0 + " != " + i1, ExceptionMessageUtil.valueAndName(i0, name0));
        }
    }

    // ----------------------------------------------------------------
    /** assert (l0 != l1) */
    public static void neq(long l0, String name0, long l1, String name1) {
        if (!(l0 != l1)) {
            fail(name0 + " != " + name1, ExceptionMessageUtil.valueAndName(l0, name0, l1, name1));
        }
    }

    public static void neq(long l0, String name0, long l1) {
        if (!(l0 != l1)) {
            fail(name0 + " != " + l1, ExceptionMessageUtil.valueAndName(l0, name0));
        }
    }

    // ----------------------------------------------------------------
    /** assert (f0 != f1) */
    public static void neq(float f0, String name0, float f1, String name1) {
        if (!(f0 != f1)) {
            fail(name0 + " != " + name1, ExceptionMessageUtil.valueAndName(f0, name0, f1, name1));
        }
    }

    public static void neq(float f0, String name0, float f1) {
        if (!(f0 != f1)) {
            fail(name0 + " != " + f1, ExceptionMessageUtil.valueAndName(f0, name0));
        }
    }

    // ----------------------------------------------------------------
    /** assert (d0 != d1) */
    public static void neq(double d0, String name0, double d1, String name1) {
        if (!(d0 != d1)) {
            fail(name0 + " != " + name1, ExceptionMessageUtil.valueAndName(d0, name0, d1, name1));
        }
    }

    public static void neq(double d0, String name0, double d1) {
        if (!(d0 != d1)) {
            fail(name0 + " != " + d1, ExceptionMessageUtil.valueAndName(d0, name0));
        }
    }

    // ################################################################
    // lt (primitiveValue < primitiveValue)

    // ----------------------------------------------------------------
    /** assert (c0 < c1) */
    public static void lt(char c0, String name0, char c1, String name1) {
        if (!(c0 < c1)) {
            fail(name0 + " < " + name1, ExceptionMessageUtil.valueAndName(c0, name0, c1, name1));
        }
    }

    public static void lt(char c0, String name0, char c1) {
        if (!(c0 < c1)) {
            fail(name0 + " < " + ExceptionMessageUtil.valueString(c1), ExceptionMessageUtil.valueAndName(c0, name0));
        }
    }

    // ----------------------------------------------------------------
    /** assert (b0 < b1) */
    public static void lt(byte b0, String name0, byte b1, String name1) {
        if (!(b0 < b1)) {
            fail(name0 + " < " + name1, ExceptionMessageUtil.valueAndName(b0, name0, b1, name1));
        }
    }

    public static void lt(byte b0, String name0, byte b1) {
        if (!(b0 < b1)) {
            fail(name0 + " < " + b1, ExceptionMessageUtil.valueAndName(b0, name0));
        }
    }

    // ----------------------------------------------------------------
    /** assert (s0 < s1) */
    public static void lt(short s0, String name0, short s1, String name1) {
        if (!(s0 < s1)) {
            fail(name0 + " < " + name1, ExceptionMessageUtil.valueAndName(s0, name0, s1, name1));
        }
    }

    public static void lt(short s0, String name0, short s1) {
        if (!(s0 < s1)) {
            fail(name0 + " < " + s1, ExceptionMessageUtil.valueAndName(s0, name0));
        }
    }

    // ----------------------------------------------------------------
    /** assert (i0 < i1) */
    public static void lt(int i0, String name0, int i1, String name1) {
        if (!(i0 < i1)) {
            fail(name0 + " < " + name1, ExceptionMessageUtil.valueAndName(i0, name0, i1, name1));
        }
    }

    public static void lt(int i0, String name0, int i1) {
        if (!(i0 < i1)) {
            fail(name0 + " < " + i1, ExceptionMessageUtil.valueAndName(i0, name0));
        }
    }

    // ----------------------------------------------------------------
    /** assert (l0 < l1) */
    public static void lt(long l0, String name0, long l1, String name1) {
        if (!(l0 < l1)) {
            fail(name0 + " < " + name1, ExceptionMessageUtil.valueAndName(l0, name0, l1, name1));
        }
    }

    public static void lt(long l0, String name0, long l1) {
        if (!(l0 < l1)) {
            fail(name0 + " < " + l1, ExceptionMessageUtil.valueAndName(l0, name0));
        }
    }

    // ----------------------------------------------------------------
    /** assert (f0 < f1) */
    public static void lt(float f0, String name0, float f1, String name1) {
        if (!(f0 < f1)) {
            fail(name0 + " < " + name1, ExceptionMessageUtil.valueAndName(f0, name0, f1, name1));
        }
    }

    public static void lt(float f0, String name0, float f1) {
        if (!(f0 < f1)) {
            fail(name0 + " < " + f1, ExceptionMessageUtil.valueAndName(f0, name0));
        }
    }

    // ----------------------------------------------------------------
    /** assert (d0 < d1) */
    public static void lt(double d0, String name0, double d1, String name1) {
        if (!(d0 < d1)) {
            fail(name0 + " < " + name1, ExceptionMessageUtil.valueAndName(d0, name0, d1, name1));
        }
    }

    public static void lt(double d0, String name0, double d1) {
        if (!(d0 < d1)) {
            fail(name0 + " < " + d1, ExceptionMessageUtil.valueAndName(d0, name0));
        }
    }

    // ################################################################
    // leq (primitiveValue <= primitiveValue)

    // ----------------------------------------------------------------
    /** assert (c0 <= c1) */
    public static void leq(char c0, String name0, char c1, String name1) {
        if (!(c0 <= c1)) {
            fail(name0 + " <= " + name1, ExceptionMessageUtil.valueAndName(c0, name0, c1, name1));
        }
    }

    public static void leq(char c0, String name0, char c1) {
        if (!(c0 <= c1)) {
            fail(name0 + " <= " + ExceptionMessageUtil.valueString(c1), ExceptionMessageUtil.valueAndName(c0, name0));
        }
    }

    // ----------------------------------------------------------------
    /** assert (b0 <= b1) */
    public static void leq(byte b0, String name0, byte b1, String name1) {
        if (!(b0 <= b1)) {
            fail(name0 + " <= " + name1, ExceptionMessageUtil.valueAndName(b0, name0, b1, name1));
        }
    }

    public static void leq(byte b0, String name0, byte b1) {
        if (!(b0 <= b1)) {
            fail(name0 + " <= " + b1, ExceptionMessageUtil.valueAndName(b0, name0));
        }
    }

    // ----------------------------------------------------------------
    /** assert (s0 <= s1) */
    public static void leq(short s0, String name0, short s1, String name1) {
        if (!(s0 <= s1)) {
            fail(name0 + " <= " + name1, ExceptionMessageUtil.valueAndName(s0, name0, s1, name1));
        }
    }

    public static void leq(short s0, String name0, short s1) {
        if (!(s0 <= s1)) {
            fail(name0 + " <= " + s1, ExceptionMessageUtil.valueAndName(s0, name0));
        }
    }

    // ----------------------------------------------------------------
    /** assert (i0 <= i1) */
    public static void leq(int i0, String name0, int i1, String name1) {
        if (!(i0 <= i1)) {
            fail(name0 + " <= " + name1, ExceptionMessageUtil.valueAndName(i0, name0, i1, name1));
        }
    }

    public static void leq(int i0, String name0, int i1) {
        if (!(i0 <= i1)) {
            fail(name0 + " <= " + i1, ExceptionMessageUtil.valueAndName(i0, name0));
        }
    }

    // ----------------------------------------------------------------
    /** assert (l0 <= l1) */
    public static void leq(long l0, String name0, long l1, String name1) {
        if (!(l0 <= l1)) {
            fail(name0 + " <= " + name1, ExceptionMessageUtil.valueAndName(l0, name0, l1, name1));
        }
    }

    public static void leq(long l0, String name0, long l1) {
        if (!(l0 <= l1)) {
            fail(name0 + " <= " + l1, ExceptionMessageUtil.valueAndName(l0, name0));
        }
    }

    // ----------------------------------------------------------------
    /** assert (f0 <= f1) */
    public static void leq(float f0, String name0, float f1, String name1) {
        if (!(f0 <= f1)) {
            fail(name0 + " <= " + name1, ExceptionMessageUtil.valueAndName(f0, name0, f1, name1));
        }
    }

    public static void leq(float f0, String name0, float f1) {
        if (!(f0 <= f1)) {
            fail(name0 + " <= " + f1, ExceptionMessageUtil.valueAndName(f0, name0));
        }
    }

    // ----------------------------------------------------------------
    /** assert (d0 <= d1) */
    public static void leq(double d0, String name0, double d1, String name1) {
        if (!(d0 <= d1)) {
            fail(name0 + " <= " + name1, ExceptionMessageUtil.valueAndName(d0, name0, d1, name1));
        }
    }

    public static void leq(double d0, String name0, double d1) {
        if (!(d0 <= d1)) {
            fail(name0 + " <= " + d1, ExceptionMessageUtil.valueAndName(d0, name0));
        }
    }

    // ################################################################
    // gt (primitiveValue > primitiveValue)

    // ----------------------------------------------------------------
    /** assert (c0 > c1) */
    public static void gt(char c0, String name0, char c1, String name1) {
        if (!(c0 > c1)) {
            fail(name0 + " > " + name1, ExceptionMessageUtil.valueAndName(c0, name0, c1, name1));
        }
    }

    public static void gt(char c0, String name0, char c1) {
        if (!(c0 > c1)) {
            fail(name0 + " > " + ExceptionMessageUtil.valueString(c1), ExceptionMessageUtil.valueAndName(c0, name0));
        }
    }

    // ----------------------------------------------------------------
    /** assert (b0 > b1) */
    public static void gt(byte b0, String name0, byte b1, String name1) {
        if (!(b0 > b1)) {
            fail(name0 + " > " + name1, ExceptionMessageUtil.valueAndName(b0, name0, b1, name1));
        }
    }

    public static void gt(byte b0, String name0, byte b1) {
        if (!(b0 > b1)) {
            fail(name0 + " > " + b1, ExceptionMessageUtil.valueAndName(b0, name0));
        }
    }

    // ----------------------------------------------------------------
    /** assert (s0 > s1) */
    public static void gt(short s0, String name0, short s1, String name1) {
        if (!(s0 > s1)) {
            fail(name0 + " > " + name1, ExceptionMessageUtil.valueAndName(s0, name0, s1, name1));
        }
    }

    public static void gt(short s0, String name0, short s1) {
        if (!(s0 > s1)) {
            fail(name0 + " > " + s1, ExceptionMessageUtil.valueAndName(s0, name0));
        }
    }

    // ----------------------------------------------------------------
    /** assert (i0 > i1) */
    public static void gt(int i0, String name0, int i1, String name1) {
        if (!(i0 > i1)) {
            fail(name0 + " > " + name1, ExceptionMessageUtil.valueAndName(i0, name0, i1, name1));
        }
    }

    public static void gt(int i0, String name0, int i1) {
        if (!(i0 > i1)) {
            fail(name0 + " > " + i1, ExceptionMessageUtil.valueAndName(i0, name0));
        }
    }

    // ----------------------------------------------------------------
    /** assert (l0 > l1) */
    public static void gt(long l0, String name0, long l1, String name1) {
        if (!(l0 > l1)) {
            fail(name0 + " > " + name1, ExceptionMessageUtil.valueAndName(l0, name0, l1, name1));
        }
    }

    public static void gt(long l0, String name0, long l1) {
        if (!(l0 > l1)) {
            fail(name0 + " > " + l1, ExceptionMessageUtil.valueAndName(l0, name0));
        }
    }

    // ----------------------------------------------------------------
    /** assert (f0 > f1) */
    public static void gt(float f0, String name0, float f1, String name1) {
        if (!(f0 > f1)) {
            fail(name0 + " > " + name1, ExceptionMessageUtil.valueAndName(f0, name0, f1, name1));
        }
    }

    public static void gt(float f0, String name0, float f1) {
        if (!(f0 > f1)) {
            fail(name0 + " > " + f1, ExceptionMessageUtil.valueAndName(f0, name0));
        }
    }

    // ----------------------------------------------------------------
    /** assert (d0 > d1) */
    public static void gt(double d0, String name0, double d1, String name1) {
        if (!(d0 > d1)) {
            fail(name0 + " > " + name1, ExceptionMessageUtil.valueAndName(d0, name0, d1, name1));
        }
    }

    public static void gt(double d0, String name0, double d1) {
        if (!(d0 > d1)) {
            fail(name0 + " > " + d1, ExceptionMessageUtil.valueAndName(d0, name0));
        }
    }

    // ################################################################
    // geq (primitiveValue >= primitiveValue)

    // ----------------------------------------------------------------
    /** assert (c0 >= c1) */
    public static void geq(char c0, String name0, char c1, String name1) {
        if (!(c0 >= c1)) {
            fail(name0 + " >= " + name1, ExceptionMessageUtil.valueAndName(c0, name0, c1, name1));
        }
    }

    public static void geq(char c0, String name0, char c1) {
        if (!(c0 >= c1)) {
            fail(name0 + " >= " + ExceptionMessageUtil.valueString(c1), ExceptionMessageUtil.valueAndName(c0, name0));
        }
    }

    // ----------------------------------------------------------------
    /** assert (b0 >= b1) */
    public static void geq(byte b0, String name0, byte b1, String name1) {
        if (!(b0 >= b1)) {
            fail(name0 + " >= " + name1, ExceptionMessageUtil.valueAndName(b0, name0, b1, name1));
        }
    }

    public static void geq(byte b0, String name0, byte b1) {
        if (!(b0 >= b1)) {
            fail(name0 + " >= " + b1, ExceptionMessageUtil.valueAndName(b0, name0));
        }
    }

    // ----------------------------------------------------------------
    /** assert (s0 >= s1) */
    public static void geq(short s0, String name0, short s1, String name1) {
        if (!(s0 >= s1)) {
            fail(name0 + " >= " + name1, ExceptionMessageUtil.valueAndName(s0, name0, s1, name1));
        }
    }

    public static void geq(short s0, String name0, short s1) {
        if (!(s0 >= s1)) {
            fail(name0 + " >= " + s1, ExceptionMessageUtil.valueAndName(s0, name0));
        }
    }

    // ----------------------------------------------------------------
    /** assert (i0 >= i1) */
    public static void geq(int i0, String name0, int i1, String name1) {
        if (!(i0 >= i1)) {
            fail(name0 + " >= " + name1, ExceptionMessageUtil.valueAndName(i0, name0, i1, name1));
        }
    }

    public static void geq(int i0, String name0, int i1) {
        if (!(i0 >= i1)) {
            fail(name0 + " >= " + i1, ExceptionMessageUtil.valueAndName(i0, name0));
        }
    }

    // ----------------------------------------------------------------
    /** assert (l0 >= l1) */
    public static void geq(long l0, String name0, long l1, String name1) {
        if (!(l0 >= l1)) {
            fail(name0 + " >= " + name1, ExceptionMessageUtil.valueAndName(l0, name0, l1, name1));
        }
    }

    public static void geq(long l0, String name0, long l1) {
        if (!(l0 >= l1)) {
            fail(name0 + " >= " + l1, ExceptionMessageUtil.valueAndName(l0, name0));
        }
    }

    // ----------------------------------------------------------------
    /** assert (f0 >= f1) */
    public static void geq(float f0, String name0, float f1, String name1) {
        if (!(f0 >= f1)) {
            fail(name0 + " >= " + name1, ExceptionMessageUtil.valueAndName(f0, name0, f1, name1));
        }
    }

    public static void geq(float f0, String name0, float f1) {
        if (!(f0 >= f1)) {
            fail(name0 + " >= " + f1, ExceptionMessageUtil.valueAndName(f0, name0));
        }
    }

    // ----------------------------------------------------------------
    /** assert (d0 >= d1) */
    public static void geq(double d0, String name0, double d1, String name1) {
        if (!(d0 >= d1)) {
            fail(name0 + " >= " + name1, ExceptionMessageUtil.valueAndName(d0, name0, d1, name1));
        }
    }

    public static void geq(double d0, String name0, double d1) {
        if (!(d0 >= d1)) {
            fail(name0 + " >= " + d1, ExceptionMessageUtil.valueAndName(d0, name0));
        }
    }

    // ################################################################
    // eqFalse, neqFalse, eqTrue, neqTrue (boolean ==/!= false/true)

    // ----------------------------------------------------------------
    /** assert (b == false) */
    public static void eqFalse(boolean b, String name) {
        if (!(false == b)) {
            fail(name + " == false", ExceptionMessageUtil.valueAndName(b, name));
        }
    }

    // ----------------------------------------------------------------
    /** assert (b != false) */
    public static void neqFalse(boolean b, String name) {
        if (!(false != b)) {
            fail(name + " != false", ExceptionMessageUtil.valueAndName(b, name));
        }
    }

    // ----------------------------------------------------------------
    /** assert (b == true) */
    public static void eqTrue(boolean b, String name) {
        if (!(true == b)) {
            fail(name + " == true", ExceptionMessageUtil.valueAndName(b, name));
        }
    }

    // ----------------------------------------------------------------
    /** assert (b != true) */
    public static void neqTrue(boolean b, String name) {
        if (!(true != b)) {
            fail(name + " != true", ExceptionMessageUtil.valueAndName(b, name));
        }
    }

    // ################################################################
    // eqZero (primitiveValue == 0)

    // ----------------------------------------------------------------
    /** assert (c == 0) */
    public static void eqZero(char c, String name) {
        if (!(0 == c)) {
            fail(name + " == 0", ExceptionMessageUtil.valueAndName(c, name));
        }
    }

    // ----------------------------------------------------------------
    /** assert (b == 0) */
    public static void eqZero(byte b, String name) {
        if (!(0 == b)) {
            fail(name + " == 0", ExceptionMessageUtil.valueAndName(b, name));
        }
    }

    // ----------------------------------------------------------------
    /** assert (s == 0) */
    public static void eqZero(short s, String name) {
        if (!(0 == s)) {
            fail(name + " == 0", ExceptionMessageUtil.valueAndName(s, name));
        }
    }

    // ----------------------------------------------------------------
    /** assert (i == 0) */
    public static void eqZero(int i, String name) {
        if (!(0 == i)) {
            fail(name + " == 0", ExceptionMessageUtil.valueAndName(i, name));
        }
    }

    // ----------------------------------------------------------------
    /** assert (l == 0) */
    public static void eqZero(long l, String name) {
        if (!(0 == l)) {
            fail(name + " == 0", ExceptionMessageUtil.valueAndName(l, name));
        }
    }

    // ----------------------------------------------------------------
    /** assert (f == 0) */
    public static void eqZero(float f, String name) {
        if (!(0 == f)) {
            fail(name + " == 0", ExceptionMessageUtil.valueAndName(f, name));
        }
    }

    // ----------------------------------------------------------------
    /** assert (d == 0) */
    public static void eqZero(double d, String name) {
        if (!(0 == d)) {
            fail(name + " == 0", ExceptionMessageUtil.valueAndName(d, name));
        }
    }

    // ################################################################
    // neqZero (primitiveValue != 0)

    // ----------------------------------------------------------------
    /** assert (c != 0) */
    public static void neqZero(char c, String name) {
        if (!(0 != c)) {
            fail(name + " != 0", ExceptionMessageUtil.valueAndName(c, name));
        }
    }

    // ----------------------------------------------------------------
    /** assert (b != 0) */
    public static void neqZero(byte b, String name) {
        if (!(0 != b)) {
            fail(name + " != 0", ExceptionMessageUtil.valueAndName(b, name));
        }
    }

    // ----------------------------------------------------------------
    /** assert (s != 0) */
    public static void neqZero(short s, String name) {
        if (!(0 != s)) {
            fail(name + " != 0", ExceptionMessageUtil.valueAndName(s, name));
        }
    }

    // ----------------------------------------------------------------
    /** assert (i != 0) */
    public static void neqZero(int i, String name) {
        if (!(0 != i)) {
            fail(name + " != 0", ExceptionMessageUtil.valueAndName(i, name));
        }
    }

    // ----------------------------------------------------------------
    /** assert (l != 0) */
    public static void neqZero(long l, String name) {
        if (!(0 != l)) {
            fail(name + " != 0", ExceptionMessageUtil.valueAndName(l, name));
        }
    }

    // ----------------------------------------------------------------
    /** assert (f != 0) */
    public static void neqZero(float f, String name) {
        if (!(0 != f)) {
            fail(name + " != 0", ExceptionMessageUtil.valueAndName(f, name));
        }
    }

    // ----------------------------------------------------------------
    /** assert (d != 0) */
    public static void neqZero(double d, String name) {
        if (!(0 != d)) {
            fail(name + " != 0", ExceptionMessageUtil.valueAndName(d, name));
        }
    }

    // ################################################################
    // ltZero (primitiveValue < 0)

    // ----------------------------------------------------------------
    /** assert (b < 0) */
    public static void ltZero(byte b, String name) {
        if (!(b < 0)) {
            fail(name + " < 0", ExceptionMessageUtil.valueAndName(b, name));
        }
    }

    // ----------------------------------------------------------------
    /** assert (s < 0) */
    public static void ltZero(short s, String name) {
        if (!(s < 0)) {
            fail(name + " < 0", ExceptionMessageUtil.valueAndName(s, name));
        }
    }

    // ----------------------------------------------------------------
    /** assert (i < 0) */
    public static void ltZero(int i, String name) {
        if (!(i < 0)) {
            fail(name + " < 0", ExceptionMessageUtil.valueAndName(i, name));
        }
    }

    // ----------------------------------------------------------------
    /** assert (l < 0) */
    public static void ltZero(long l, String name) {
        if (!(l < 0)) {
            fail(name + " < 0", ExceptionMessageUtil.valueAndName(l, name));
        }
    }

    // ----------------------------------------------------------------
    /** assert (f < 0) */
    public static void ltZero(float f, String name) {
        if (!(f < 0)) {
            fail(name + " < 0", ExceptionMessageUtil.valueAndName(f, name));
        }
    }

    // ----------------------------------------------------------------
    /** assert (d < 0) */
    public static void ltZero(double d, String name) {
        if (!(d < 0)) {
            fail(name + " < 0", ExceptionMessageUtil.valueAndName(d, name));
        }
    }

    // ################################################################
    // leqZero (primitiveValue <= 0)

    // ----------------------------------------------------------------
    /** assert (b <= 0) */
    public static void leqZero(byte b, String name) {
        if (!(b <= 0)) {
            fail(name + " <= 0", ExceptionMessageUtil.valueAndName(b, name));
        }
    }

    // ----------------------------------------------------------------
    /** assert (s <= 0) */
    public static void leqZero(short s, String name) {
        if (!(s <= 0)) {
            fail(name + " <= 0", ExceptionMessageUtil.valueAndName(s, name));
        }
    }

    // ----------------------------------------------------------------
    /** assert (i <= 0) */
    public static void leqZero(int i, String name) {
        if (!(i <= 0)) {
            fail(name + " <= 0", ExceptionMessageUtil.valueAndName(i, name));
        }
    }

    // ----------------------------------------------------------------
    /** assert (l <= 0) */
    public static void leqZero(long l, String name) {
        if (!(l <= 0)) {
            fail(name + " <= 0", ExceptionMessageUtil.valueAndName(l, name));
        }
    }

    // ----------------------------------------------------------------
    /** assert (f <= 0) */
    public static void leqZero(float f, String name) {
        if (!(f <= 0)) {
            fail(name + " <= 0", ExceptionMessageUtil.valueAndName(f, name));
        }
    }

    // ----------------------------------------------------------------
    /** assert (d <= 0) */
    public static void leqZero(double d, String name) {
        if (!(d <= 0)) {
            fail(name + " <= 0", ExceptionMessageUtil.valueAndName(d, name));
        }
    }

    // ################################################################
    // gtZero (primitiveValue > 0)

    // ----------------------------------------------------------------
    /** assert (b > 0) */
    public static void gtZero(byte b, String name) {
        if (!(b > 0)) {
            fail(name + " > 0", ExceptionMessageUtil.valueAndName(b, name));
        }
    }

    // ----------------------------------------------------------------
    /** assert (s > 0) */
    public static void gtZero(short s, String name) {
        if (!(s > 0)) {
            fail(name + " > 0", ExceptionMessageUtil.valueAndName(s, name));
        }
    }

    // ----------------------------------------------------------------
    /** assert (i > 0) */
    public static void gtZero(int i, String name) {
        if (!(i > 0)) {
            fail(name + " > 0", ExceptionMessageUtil.valueAndName(i, name));
        }
    }

    // ----------------------------------------------------------------
    /** assert (l > 0) */
    public static void gtZero(long l, String name) {
        if (!(l > 0)) {
            fail(name + " > 0", ExceptionMessageUtil.valueAndName(l, name));
        }
    }

    // ----------------------------------------------------------------
    /** assert (f > 0) */
    public static void gtZero(float f, String name) {
        if (!(f > 0)) {
            fail(name + " > 0", ExceptionMessageUtil.valueAndName(f, name));
        }
    }

    // ----------------------------------------------------------------
    /** assert (d > 0) */
    public static void gtZero(double d, String name) {
        if (!(d > 0)) {
            fail(name + " > 0", ExceptionMessageUtil.valueAndName(d, name));
        }
    }

    // ################################################################
    // geqZero (primitiveValue >= 0)

    // ----------------------------------------------------------------
    /** assert (b >= 0) */
    public static void geqZero(byte b, String name) {
        if (!(b >= 0)) {
            fail(name + " >= 0", ExceptionMessageUtil.valueAndName(b, name));
        }
    }

    // ----------------------------------------------------------------
    /** assert (s >= 0) */
    public static void geqZero(short s, String name) {
        if (!(s >= 0)) {
            fail(name + " >= 0", ExceptionMessageUtil.valueAndName(s, name));
        }
    }

    // ----------------------------------------------------------------
    /** assert (i >= 0) */
    public static void geqZero(int i, String name) {
        if (!(i >= 0)) {
            fail(name + " >= 0", ExceptionMessageUtil.valueAndName(i, name));
        }
    }

    // ----------------------------------------------------------------
    /** assert (l >= 0) */
    public static void geqZero(long l, String name) {
        if (!(l >= 0)) {
            fail(name + " >= 0", ExceptionMessageUtil.valueAndName(l, name));
        }
    }

    // ----------------------------------------------------------------
    /** assert (f >= 0) */
    public static void geqZero(float f, String name) {
        if (!(f >= 0)) {
            fail(name + " >= 0", ExceptionMessageUtil.valueAndName(f, name));
        }
    }

    // ----------------------------------------------------------------
    /** assert (d >= 0) */
    public static void geqZero(double d, String name) {
        if (!(d >= 0)) {
            fail(name + " >= 0", ExceptionMessageUtil.valueAndName(d, name));
        }
    }

    // ################################################################
    // eq, neq (Object ==/!= Object)

    // ----------------------------------------------------------------
    /** assert (o0 == o1) */
    public static void eq(Object o0, String name0, Object o1, String name1) {
        // noinspection ObjectEquality
        if (!(o0 == o1)) {
            fail(name0 + " == " + name1, ExceptionMessageUtil.valueAndName(o0, name0, o1, name1));
        }
    }

    public static void eq(Object o0, String name0, Object o1) {
        // noinspection ObjectEquality
        if (!(o0 == o1)) {
            fail(name0 + " == " + ExceptionMessageUtil.valueString(o1), ExceptionMessageUtil.valueAndName(o0, name0));
        }
    }

    // ----------------------------------------------------------------
    /** assert (o0 != o1) */
    public static void neq(Object o0, String name0, Object o1, String name1) {
        // noinspection ObjectEquality
        if (!(o0 != o1)) {
            fail(name0 + " != " + name1, ExceptionMessageUtil.valueAndName(o0, name0, o1, name1));
        }
    }

    public static void neq(Object o0, String name0, Object o1) {
        // noinspection ObjectEquality
        if (!(o0 != o1)) {
            fail(name0 + " != " + ExceptionMessageUtil.valueString(o1), ExceptionMessageUtil.valueAndName(o0, name0));
        }
    }

    // ################################################################
    // eqNull, neqNull (Object ==/!= null)

    // ----------------------------------------------------------------
    /** assert (o == null) */
    public static void eqNull(Object o, String name) {
        if (!(null == o)) {
            fail(name + " == null", ExceptionMessageUtil.valueAndName(o, name));
        }
    }

    // ----------------------------------------------------------------
    /** assert (o != null) */
    public static void neqNull(Object o, String name) {
        if (!(null != o)) {
            fail(name + " != null", ExceptionMessageUtil.valueAndName(o, name));
        }
    }

    // ################################################################
    // eqNaN, neqNaN

    // ----------------------------------------------------------------
    /** assert (Double.isNaN(d)) */
    public static void eqNaN(double d, String name) {
        if (!Double.isNaN(d)) {
            fail(name + " == NaN", ExceptionMessageUtil.valueAndName(d, name));
        }
    }

    // ----------------------------------------------------------------
    /** assert (!Double.isNaN(d) */
    public static void neqNaN(double d, String name) {
        if (Double.isNaN(d)) {
            fail(name + " != NaN", ExceptionMessageUtil.valueAndName(d, name));
        }
    }

    // ################################################################
    // equals (Object.equals(Object))

    // ----------------------------------------------------------------
    /** assert (o0 != null && o1 != null && o0.equals(o1)) */
    public static void equals(Object o0, String name0, Object o1, String name1) {
        neqNull(o0, name0);
        neqNull(o1, name1);
        if (!(o0.equals(o1))) {
            fail(name0 + ".equals(" + name1 + ")", ExceptionMessageUtil.valueAndName(o0, name0, o1, name1));
        }
    }

    // ----------------------------------------------------------------
    /** assert (o0 != null && o1 != null && o0.equals(o1)) */
    public static void equals(Object o0, String name0, Object o1) {
        neqNull(o0, name0);
        neqNull(o1, "o1");
        if (!(o0.equals(o1))) {
            fail(name0 + ".equals(" + ExceptionMessageUtil.valueString(o1) + ")",
                    ExceptionMessageUtil.valueAndName(o0, name0));
        }
    }

    // ----------------------------------------------------------------
    /** assert ((o0 == null && o1 == null) || (o0 != null && o0.equals(o1))) */
    public static void nullSafeEquals(Object o0, String name0, Object o1, String name1) {
        if ((null == o0 && null != o1) || (null != o1 && !o0.equals(o1))) {
            fail(name0 + ".equals(" + name1 + ")", ExceptionMessageUtil.valueAndName(o0, name0, o1, name1));
        }
    }


    // ----------------------------------------------------------------
    /** assert (o0 != null && o1 != null && !o0.equals(o1)) */
    public static void notEquals(Object o0, String name0, Object o1, String name1) {
        neqNull(o0, name0);
        neqNull(o1, name1);
        if (o0.equals(o1)) {
            fail("!" + name0 + ".equals(" + name1 + ")", ExceptionMessageUtil.valueAndName(o0, name0, o1, name1));
        }
    }

    // ----------------------------------------------------------------
    /** assert (o0 != null && o1 != null && !o0.equals(o1)) */
    public static void notEquals(Object o0, String name0, Object o1) {
        neqNull(o0, name0);
        neqNull(o1, "o1");
        if (o0.equals(o1)) {
            fail("!" + name0 + ".equals(" + ExceptionMessageUtil.valueString(o1) + ")",
                    ExceptionMessageUtil.valueAndName(o0, name0));
        }
    }

    // ################################################################
    // nonempty (String.equals(nonempty))

    // ----------------------------------------------------------------
    /** assert (s != null && s.length() > 0) */
    public static void nonempty(String s, String name) {
        neqNull(s, name);
        if (!(s.length() > 0)) {
            fail(name + ".length() > 0", ExceptionMessageUtil.valueAndName(s, name));
        }
    }

    @SuppressWarnings("UnusedParameters")
    public static <TYPE> TYPE neverInvoked(TYPE t1, TYPE t2) {
        throw statementNeverExecuted();
    }
}
