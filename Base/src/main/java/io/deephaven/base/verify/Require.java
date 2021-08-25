/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.verify;

import org.jetbrains.annotations.NotNull;

import java.awt.EventQueue;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

// --------------------------------------------------------------------
/**
 * Requirement methods for simple runtime program verification. Failed requirements throw {@link RequirementFailure}.
 * <p>
 * Methods:
 * <ul>
 * <li>void requirement(boolean condition, String conditionText[, String detailMessage][, int numCallsBelowRequirer])
 * <li>void requirement(boolean condition, String conditionText, value0, String name0, value1, String name1, ... [, int
 * numCallsBelowRequirer])
 * </ul>
 * <ul>
 * <li>void statementNeverExecuted([int numCallsBelowRequirer])
 * <li>void statementNeverExecuted(String statementDescription[, int numCallsBelowRequirer])
 * <li>void exceptionNeverCaught(Exception caughtException[, int numCallsBelowRequirer])
 * <li>void exceptionNeverCaught(String tryStatementDescription, Exception caughtException[, int numCallsBelowRequirer])
 * <li>void valueNeverOccurs(value, String name[, int numCallsBelowRequirer])
 * <li>void valuesNeverOccur(value0, name0, value1, name1, ... [, int numCallsBelowRequirer])
 * </ul>
 * <ul>
 * <li>void holdsLock/notHoldsLock(Object, String name[, int numCallsBelowRequirer])
 * </ul>
 * <ul>
 * <li>void instanceOf/notInstanceOf(Object, String name, Class type[, int numCallsBelowRequirer])
 * </ul>
 * <ul>
 * <li>void eq/neq(boolean/char/byte/short/int/long/float/double, String name0,
 * boolean/char/byte/short/int/long/float/double[, String name1][, int numCallsBelowRequirer])
 * <li>void lt/leq/gt/geq(char/byte/short/int/long/float/double, String name0, char/byte/short/int/long/float/double[,
 * String name1][, int numCallsBelowRequirer])
 * </ul>
 * <ul>
 * <li>void eqFalse/neqFalse/eqTrue/neqTrue(boolean, String name[, int numCallsBelowRequirer])
 * <li>void eqZero/neqZero(char/byte/short/int/long/float/double, String name[, int numCallsBelowRequirer])
 * <li>void ltZero/leqZero/gtZero/geqZero(byte/short/int/long/float/double, String name[, int numCallsBelowRequirer])
 * </ul>
 * <ul>
 * <li>void eq/neq(Object, name0, Object[, name1][, int numCallsBelowRequirer])
 * <li>void eqNull/neqNull(Object, String name[, int numCallsBelowRequirer])
 * </ul>
 * <ul>
 * <li>void equals(Object, String name0, Object, String name1[, int numCallsBelowRequirer])
 * <li>void nonempty(String, String name[, int numCallsBelowRequirer])
 * </ul>
 * <p>
 * Naming Rationale:
 * <ul>
 * <li>eq, neq, lt, leq, gt, get correspond to ==, !=, <, <=, >, >=, e.g.,
 * <ul>
 * <li>For Object a and b, Require.eq(a, "a", b, "b") corresponds to require (a == b)
 * <li>For Object o, Require.neqNull(o, "o") corresponds to require (o != null)
 * <li>for int x, Require.eqZero(x, "x") corresponds to require (x == 0)
 * </ul>
 * <li>equals corresponds to Object.equals (preceded by necessary null checks), e.g.,
 * <ul>
 * <li>For Object a and b, Require.equals(a, "a", b, "b") corresponds to require (a!= null && b != null && a.equals(b))
 * <li>for String s, Require.nonempty(s, "s") corresponds to require (s != null && s.length() != 0)
 * </ul>
 * </ul>
 */
public final class Require {
    // ################################################################
    static private volatile Consumer<RequirementFailure> onFailureCallback;

    public static boolean setOnFailureCallback(Consumer<RequirementFailure> newCallback) {
        final boolean wasSet = onFailureCallback != null;
        onFailureCallback = newCallback;
        return wasSet;
    }

    // we should only have static methods
    private Require() {}

    // ################################################################
    // Handle failed requirements

    // ----------------------------------------------------------------
    private static void fail(String conditionText, int numCallsBelowRequirer) {
        final RequirementFailure requirementFailure = new RequirementFailure(
                ExceptionMessageUtil.failureMessage("Requirement", "required", conditionText, null),
                numCallsBelowRequirer + 1);
        if (onFailureCallback != null) {
            try {
                onFailureCallback.accept(requirementFailure);
            } catch (Exception ignored) {
            }
        }
        throw requirementFailure;
    }

    // ----------------------------------------------------------------
    private static void fail(String conditionText, String detailMessage, int numCallsBelowRequirer) {
        final RequirementFailure requirementFailure = new RequirementFailure(
                ExceptionMessageUtil.failureMessage("Requirement", "required", conditionText, detailMessage),
                numCallsBelowRequirer + 1);
        if (onFailureCallback != null) {
            try {
                onFailureCallback.accept(requirementFailure);
            } catch (Exception ignored) {
            }
        }
        throw requirementFailure;
    }

    // ################################################################
    // requirement

    // ----------------------------------------------------------------
    /**
     * require (condition, conditionText)
     */
    public static void requirement(boolean condition, String conditionText, int numCallsBelowRequirer) {
        if (!(condition)) {
            fail(conditionText, numCallsBelowRequirer + 1);
        }
    }

    public static void requirement(boolean condition, String conditionText) {
        requirement(condition, conditionText, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (condition, conditionText, detailMessage)
     */
    public static void requirement(boolean condition, String conditionText, String detailMessage,
            int numCallsBelowRequirer) {
        if (!(condition)) {
            fail(conditionText, detailMessage, numCallsBelowRequirer + 1);
        }
    }

    public static void requirement(boolean condition, String conditionText, String detailMessage) {
        requirement(condition, conditionText, detailMessage, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (condition, Object o0, String name0, ... )
     */
    public static void requirement(boolean condition, String conditionText, Object o0, String name0,
            int numCallsBelowRequirer) {
        if (!(condition)) {
            fail(conditionText, ExceptionMessageUtil.valueAndName(o0, name0), numCallsBelowRequirer + 1);
        }
    }

    public static void requirement(boolean condition, String conditionText, Object o0, String name0) {
        requirement(condition, conditionText, o0, name0, 1);
    }

    public static void requirement(boolean condition, String conditionText, Object o0, String name0, Object o1,
            String name1, int numCallsBelowRequirer) {
        if (!(condition)) {
            fail(conditionText, ExceptionMessageUtil.valueAndName(o0, name0, o1, name1), numCallsBelowRequirer + 1);
        }
    }

    public static void requirement(boolean condition, String conditionText, Object o0, String name0, Object o1,
            String name1) {
        requirement(condition, conditionText, o0, name0, o1, name1, 1);
    }

    public static void requirement(boolean condition, String conditionText, Object o0, String name0, Object o1,
            String name1, Object o2, String name2, int numCallsBelowRequirer) {
        if (!(condition)) {
            fail(conditionText, ExceptionMessageUtil.valueAndName(o0, name0, o1, name1, o2, name2),
                    numCallsBelowRequirer + 1);
        }
    }

    public static void requirement(boolean condition, String conditionText, long o0, String name0, long o1,
            String name1, long o2, String name2, int numCallsBelowRequirer) {
        if (!(condition)) {
            fail(conditionText, ExceptionMessageUtil.valueAndName(o0, name0, o1, name1, o2, name2),
                    numCallsBelowRequirer + 1);
        }
    }

    public static void requirement(boolean condition, String conditionText, Object o0, String name0, Object o1,
            String name1, Object o2, String name2) {
        requirement(condition, conditionText, o0, name0, o1, name1, o2, name2, 1);
    }

    public static void requirement(boolean condition, String conditionText, long o0, String name0, long o1,
            String name1, long o2, String name2) {
        requirement(condition, conditionText, o0, name0, o1, name1, o2, name2, 1);
    }

    public static void requirement(boolean condition, String conditionText, Object o0, String name0, Object o1,
            String name1, Object o2, String name2, Object o3, String name3, int numCallsBelowRequirer) {
        if (!(condition)) {
            fail(conditionText, ExceptionMessageUtil.valueAndName(o0, name0, o1, name1, o2, name2, o3, name3),
                    numCallsBelowRequirer + 1);
        }
    }

    public static void requirement(boolean condition, String conditionText, Object o0, String name0, Object o1,
            String name1, Object o2, String name2, Object o3, String name3) {
        requirement(condition, conditionText, o0, name0, o1, name1, o2, name2, o3, name3, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (condition, boolean b0, String name0, ... )
     */
    public static void requirement(boolean condition, String conditionText, boolean b0, String name0,
            int numCallsBelowRequirer) {
        if (!(condition)) {
            fail(conditionText, ExceptionMessageUtil.valueAndName(b0, name0), numCallsBelowRequirer + 1);
        }
    }

    public static void requirement(boolean condition, String conditionText, boolean b0, String name0) {
        requirement(condition, conditionText, b0, name0, 1);
    }

    public static void requirement(boolean condition, String conditionText, boolean b0, String name0, boolean b1,
            String name1, int numCallsBelowRequirer) {
        if (!(condition)) {
            fail(conditionText, ExceptionMessageUtil.valueAndName(b0, name0, b1, name1), numCallsBelowRequirer + 1);
        }
    }

    public static void requirement(boolean condition, String conditionText, boolean b0, String name0, boolean b1,
            String name1) {
        requirement(condition, conditionText, b0, name0, b1, name1, 1);
    }

    public static void requirement(boolean condition, String conditionText, boolean b0, String name0, double d1,
            String name1, int numCallsBelowRequirer) {
        if (!(condition)) {
            fail(conditionText, ExceptionMessageUtil.concat(ExceptionMessageUtil.valueAndName(b0, name0),
                    ExceptionMessageUtil.valueAndName(d1, name1)), numCallsBelowRequirer + 1);
        }
    }

    public static void requirement(boolean condition, String conditionText, boolean b0, String name0, double d1,
            String name1) {
        requirement(condition, conditionText, b0, name0, d1, name1, 1);
    }

    public static void requirement(boolean condition, String conditionText, boolean b0, String name0, boolean b1,
            String name1, boolean b2, String name2, int numCallsBelowRequirer) {
        if (!(condition)) {
            fail(conditionText, ExceptionMessageUtil.valueAndName(b0, name0, b1, name1, b2, name2),
                    numCallsBelowRequirer + 1);
        }
    }

    public static void requirement(boolean condition, String conditionText, boolean b0, String name0, boolean b1,
            String name1, boolean b2, String name2) {
        requirement(condition, conditionText, b0, name0, b1, name1, b2, name2, 1);
    }

    public static void requirement(boolean condition, String conditionText, boolean b0, String name0, boolean b1,
            String name1, boolean b2, String name2, boolean b3, String name3, int numCallsBelowRequirer) {
        if (!(condition)) {
            fail(conditionText, ExceptionMessageUtil.valueAndName(b0, name0, b1, name1, b2, name2, b3, name3),
                    numCallsBelowRequirer + 1);
        }
    }

    public static void requirement(boolean condition, String conditionText, boolean b0, String name0, boolean b1,
            String name1, boolean b2, String name2, boolean b3, String name3) {
        requirement(condition, conditionText, b0, name0, b1, name1, b2, name2, b3, name3, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (condition, int i0, String name0, ... )
     */
    public static void requirement(boolean condition, String conditionText, int i0, String name0,
            int numCallsBelowRequirer) {
        if (!(condition)) {
            fail(conditionText, ExceptionMessageUtil.valueAndName(i0, name0), numCallsBelowRequirer + 1);
        }
    }

    public static void requirement(boolean condition, String conditionText, int i0, String name0) {
        requirement(condition, conditionText, i0, name0, 1);
    }

    public static void requirement(boolean condition, String conditionText, int i0, String name0, int i1, String name1,
            int numCallsBelowRequirer) {
        if (!(condition)) {
            fail(conditionText, ExceptionMessageUtil.valueAndName(i0, name0, i1, name1), numCallsBelowRequirer + 1);
        }
    }

    public static void requirement(boolean condition, String conditionText, int i0, String name0, int i1,
            String name1) {
        requirement(condition, conditionText, i0, name0, i1, name1, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (condition, long l0, String name0, ... )
     */
    public static void requirement(boolean condition, String conditionText, long l0, String name0,
            int numCallsBelowRequirer) {
        if (!(condition)) {
            fail(conditionText, ExceptionMessageUtil.valueAndName(l0, name0), numCallsBelowRequirer + 1);
        }
    }

    public static void requirement(boolean condition, String conditionText, long l0, String name0) {
        requirement(condition, conditionText, l0, name0, 1);
    }

    // ################################################################
    // statementNeverExecuted

    // ----------------------------------------------------------------
    /**
     * require (this statement is never executed)
     */
    public static RequirementFailure statementNeverExecuted(int numCallsBelowRequirer) {
        fail("statement is never executed", numCallsBelowRequirer + 1);
        return null;
    }

    public static RequirementFailure statementNeverExecuted() {
        return statementNeverExecuted(1);
    }

    // ----------------------------------------------------------------
    /**
     * require (statementDescription is never executed)
     */
    public static RequirementFailure statementNeverExecuted(String statementDescription, int numCallsBelowRequirer) {
        fail(statementDescription + " is never executed", numCallsBelowRequirer + 1);
        return null;
    }

    public static RequirementFailure statementNeverExecuted(String statementDescription) {
        return statementNeverExecuted(statementDescription, 1);
    }

    // ################################################################
    // exceptionNeverCaught

    // ----------------------------------------------------------------
    /**
     * require (this exception is never caught, Exception e)
     */
    public static RequirementFailure exceptionNeverCaught(Exception e, int numCallsBelowRequirer) {
        try {
            fail(e.getClass().getName() + " is never caught",
                    e.getClass().getName() + "(" + e.getMessage() + ") caught", numCallsBelowRequirer + 1);
        } catch (RequirementFailure requirementFailure) {
            requirementFailure.initCause(e);
            throw requirementFailure;
        }
        return null;
    }

    public static RequirementFailure exceptionNeverCaught(Exception e) {
        return exceptionNeverCaught(e, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (tryStatementDescription succeeds, Exception e)
     */
    public static RequirementFailure exceptionNeverCaught(String tryStatementDescription, Exception e,
            int numCallsBelowRequirer) {
        try {
            fail(tryStatementDescription + " succeeds", e.getClass().getName() + "(" + e.getMessage() + ") caught",
                    numCallsBelowRequirer + 1);
        } catch (RequirementFailure requirementFailure) {
            requirementFailure.initCause(e);
            throw requirementFailure;
        }
        return null;
    }

    public static RequirementFailure exceptionNeverCaught(String tryStatementDescription, Exception e) {
        return exceptionNeverCaught(tryStatementDescription, e, 1);
    }

    // ################################################################
    // valueNeverOccurs

    // ----------------------------------------------------------------
    /**
     * require (this value never occurs, Object o, name)
     */
    public static RequirementFailure valueNeverOccurs(Object o, String name, int numCallsBelowRequirer) {
        fail(ExceptionMessageUtil.valueAndName(o, name) + " never occurs", numCallsBelowRequirer + 1);
        return null;
    }

    public static RequirementFailure valueNeverOccurs(Object o, String name) {
        return valueNeverOccurs(o, name, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (this value never occurs, boolean b, name)
     */
    public static RequirementFailure valueNeverOccurs(boolean b, String name, int numCallsBelowRequirer) {
        fail(ExceptionMessageUtil.valueAndName(b, name) + " never occurs", numCallsBelowRequirer + 1);
        return null;
    }

    public static RequirementFailure valueNeverOccurs(boolean b, String name) {
        return valueNeverOccurs(b, name, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (this value never occurs, char c, name)
     */
    public static RequirementFailure valueNeverOccurs(char c, String name, int numCallsBelowRequirer) {
        fail(ExceptionMessageUtil.valueAndName(c, name) + " never occurs", numCallsBelowRequirer + 1);
        return null;
    }

    public static RequirementFailure valueNeverOccurs(char c, String name) {
        return valueNeverOccurs(c, name, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (this value never occurs, byte b, name)
     */
    public static RequirementFailure valueNeverOccurs(byte b, String name, int numCallsBelowRequirer) {
        fail(ExceptionMessageUtil.valueAndName(b, name) + " never occurs", numCallsBelowRequirer + 1);
        return null;
    }

    public static RequirementFailure valueNeverOccurs(byte b, String name) {
        return valueNeverOccurs(b, name, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (this value never occurs, short s, name)
     */
    public static RequirementFailure valueNeverOccurs(short s, String name, int numCallsBelowRequirer) {
        fail(ExceptionMessageUtil.valueAndName(s, name) + " never occurs", numCallsBelowRequirer + 1);
        return null;
    }

    public static RequirementFailure valueNeverOccurs(short s, String name) {
        return valueNeverOccurs(s, name, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (this value never occurs, int i, name)
     */
    public static RequirementFailure valueNeverOccurs(int i, String name, int numCallsBelowRequirer) {
        fail(ExceptionMessageUtil.valueAndName(i, name) + " never occurs", numCallsBelowRequirer + 1);
        return null;
    }

    public static RequirementFailure valueNeverOccurs(int i, String name) {
        return valueNeverOccurs(i, name, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (this value never occurs, long l, name)
     */
    public static RequirementFailure valueNeverOccurs(long l, String name, int numCallsBelowRequirer) {
        fail(ExceptionMessageUtil.valueAndName(l, name) + " never occurs", numCallsBelowRequirer + 1);
        return null;
    }

    public static RequirementFailure valueNeverOccurs(long l, String name) {
        return valueNeverOccurs(l, name, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (this value never occurs, float f, name)
     */
    public static RequirementFailure valueNeverOccurs(float f, String name, int numCallsBelowRequirer) {
        fail(ExceptionMessageUtil.valueAndName(f, name) + " never occurs", numCallsBelowRequirer + 1);
        return null;
    }

    public static RequirementFailure valueNeverOccurs(float f, String name) {
        return valueNeverOccurs(f, name, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (this value never occurs, double d, name)
     */
    public static RequirementFailure valueNeverOccurs(double d, String name, int numCallsBelowRequirer) {
        fail(ExceptionMessageUtil.valueAndName(d, name) + " never occurs", numCallsBelowRequirer + 1);
        return null;
    }

    public static RequirementFailure valueNeverOccurs(double d, String name) {
        return valueNeverOccurs(d, name, 1);
    }

    // ################################################################
    // holdsLock, notHoldsLock

    // ----------------------------------------------------------------
    /**
     * require (o != null && (current thread holds o's lock))
     */
    public static void holdsLock(Object o, String name, int numCallsBelowRequirer) {
        neqNull(o, "o");
        if (!Thread.holdsLock(o)) {
            fail("\"" + Thread.currentThread().getName() + "\".holdsLock(" + name + ")", numCallsBelowRequirer + 1);
        }
    }

    public static void holdsLock(Object o, String name) {
        holdsLock(o, name, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (o != null && !(current thread holds o's lock))
     */
    public static void notHoldsLock(Object o, String name, int numCallsBelowRequirer) {
        neqNull(o, "o");
        if (Thread.holdsLock(o)) {
            fail("!\"" + Thread.currentThread().getName() + "\".holdsLock(" + name + ")", numCallsBelowRequirer + 1);
        }
    }

    public static void notHoldsLock(Object o, String name) {
        notHoldsLock(o, name, 1);
    }


    // ################################################################
    // instanceOf, notInstanceOf

    // ----------------------------------------------------------------
    /**
     * require (o instanceof type)
     */
    public static <T> void instanceOf(Object o, String name, Class<T> type, int numCallsBelowRequirer) {
        if (!type.isInstance(o)) {
            fail(name + " instanceof " + type, null == o ? ExceptionMessageUtil.valueAndName(o, name)
                    : name + " instanceof " + o.getClass() + " (" + ExceptionMessageUtil.valueAndName(o, name) + ")",
                    numCallsBelowRequirer + 1);
        }
    }

    public static <T> void instanceOf(Object o, String name, Class<T> type) {
        instanceOf(o, name, type, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require !(o instanceof type)
     */
    public static <T> void notInstanceOf(Object o, String name, Class<T> type, int numCallsBelowRequirer) {
        if (type.isInstance(o)) {
            fail("!(" + name + " instanceof " + type + ")",
                    name + " instanceof " + o.getClass() + " (" + ExceptionMessageUtil.valueAndName(o, name) + ")",
                    numCallsBelowRequirer + 1);
        }
    }

    public static <T> void notInstanceOf(Object o, String name, Class<T> type) {
        notInstanceOf(o, name, type, 1);
    }

    // ################################################################
    // isAWTThread, isNotAWTThread

    // ----------------------------------------------------------------
    /**
     * require (current thread is AWT Event Dispatch Thread)
     */
    public static void isAWTThread() {
        isAWTThread(1);
    }

    public static void isAWTThread(int numCallsBelowRequirer) {
        if (!EventQueue.isDispatchThread()) {
            fail("\"" + Thread.currentThread().getName() + "\".isAWTThread()", numCallsBelowRequirer + 1);
        }
    }

    // ----------------------------------------------------------------
    /**
     * require (current thread is AWT Event Dispatch Thread)
     */
    public static void isNotAWTThread() {
        isNotAWTThread(1);
    }

    public static void isNotAWTThread(int numCallsBelowRequirer) {
        if (EventQueue.isDispatchThread()) {
            fail("!\"" + Thread.currentThread().getName() + "\".isAWTThread()", numCallsBelowRequirer + 1);
        }
    }

    // ################################################################
    // eq (primitiveValue == primitiveValue)

    // ----------------------------------------------------------------
    /**
     * require (b0 == b1)
     */
    public static void eq(boolean b0, String name0, boolean b1, String name1, int numCallsBelowRequirer) {
        if (!(b0 == b1)) {
            fail(name0 + " == " + name1, ExceptionMessageUtil.valueAndName(b0, name0, b1, name1),
                    numCallsBelowRequirer + 1);
        }
    }

    public static void eq(boolean b0, String name0, boolean b1, String name1) {
        eq(b0, name0, b1, name1, 1);
    }

    public static void eq(boolean b0, String name0, boolean b1, int numCallsBelowRequirer) {
        if (!(b0 == b1)) {
            fail(name0 + " == " + b1, ExceptionMessageUtil.valueAndName(b0, name0), numCallsBelowRequirer + 1);
        }
    }

    public static void eq(boolean b0, String name0, boolean b1) {
        eq(b0, name0, b1, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (c0 == c1)
     */
    public static void eq(char c0, String name0, char c1, String name1, int numCallsBelowRequirer) {
        if (!(c0 == c1)) {
            fail(name0 + " == " + name1, ExceptionMessageUtil.valueAndName(c0, name0, c1, name1),
                    numCallsBelowRequirer + 1);
        }
    }

    public static void eq(char c0, String name0, char c1, String name1) {
        eq(c0, name0, c1, name1, 1);
    }

    public static void eq(char c0, String name0, char c1, int numCallsBelowRequirer) {
        if (!(c0 == c1)) {
            fail(name0 + " == " + ExceptionMessageUtil.valueString(c1), ExceptionMessageUtil.valueAndName(c0, name0),
                    numCallsBelowRequirer + 1);
        }
    }

    public static void eq(char c0, String name0, char c1) {
        eq(c0, name0, c1, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (b0 == b1)
     */
    public static void eq(byte b0, String name0, byte b1, String name1, int numCallsBelowRequirer) {
        if (!(b0 == b1)) {
            fail(name0 + " == " + name1, ExceptionMessageUtil.valueAndName(b0, name0, b1, name1),
                    numCallsBelowRequirer + 1);
        }
    }

    public static void eq(byte b0, String name0, byte b1, String name1) {
        eq(b0, name0, b1, name1, 1);
    }

    public static void eq(byte b0, String name0, byte b1, int numCallsBelowRequirer) {
        if (!(b0 == b1)) {
            fail(name0 + " == " + b1, ExceptionMessageUtil.valueAndName(b0, name0), numCallsBelowRequirer + 1);
        }
    }

    public static void eq(byte b0, String name0, byte b1) {
        eq(b0, name0, b1, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (s0 == s1)
     */
    public static void eq(short s0, String name0, short s1, String name1, int numCallsBelowRequirer) {
        if (!(s0 == s1)) {
            fail(name0 + " == " + name1, ExceptionMessageUtil.valueAndName(s0, name0, s1, name1),
                    numCallsBelowRequirer + 1);
        }
    }

    public static void eq(short s0, String name0, short s1, String name1) {
        eq(s0, name0, s1, name1, 1);
    }

    public static void eq(short s0, String name0, short s1, int numCallsBelowRequirer) {
        if (!(s0 == s1)) {
            fail(name0 + " == " + s1, ExceptionMessageUtil.valueAndName(s0, name0), numCallsBelowRequirer + 1);
        }
    }

    public static void eq(short s0, String name0, short s1) {
        eq(s0, name0, s1, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (i0 == i1)
     */
    public static void eq(int i0, String name0, int i1, String name1, int numCallsBelowRequirer) {
        if (!(i0 == i1)) {
            fail(name0 + " == " + name1, ExceptionMessageUtil.valueAndName(i0, name0, i1, name1),
                    numCallsBelowRequirer + 1);
        }
    }

    public static void eq(int i0, String name0, int i1, String name1) {
        eq(i0, name0, i1, name1, 1);
    }

    public static void eq(int i0, String name0, int i1, int numCallsBelowRequirer) {
        if (!(i0 == i1)) {
            fail(name0 + " == " + i1, ExceptionMessageUtil.valueAndName(i0, name0), numCallsBelowRequirer + 1);
        }
    }

    public static void eq(int i0, String name0, int i1) {
        eq(i0, name0, i1, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (l0 == l1)
     */
    public static void eq(long l0, String name0, long l1, String name1, int numCallsBelowRequirer) {
        if (!(l0 == l1)) {
            fail(name0 + " == " + name1, ExceptionMessageUtil.valueAndName(l0, name0, l1, name1),
                    numCallsBelowRequirer + 1);
        }
    }

    public static void eq(long l0, String name0, long l1, String name1) {
        eq(l0, name0, l1, name1, 1);
    }

    public static void eq(long l0, String name0, long l1, int numCallsBelowRequirer) {
        if (!(l0 == l1)) {
            fail(name0 + " == " + l1, ExceptionMessageUtil.valueAndName(l0, name0), numCallsBelowRequirer + 1);
        }
    }

    public static void eq(long l0, String name0, long l1) {
        eq(l0, name0, l1, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (f0 == f1)
     */
    public static void eq(float f0, String name0, float f1, String name1, int numCallsBelowRequirer) {
        if (!(f0 == f1)) {
            fail(name0 + " == " + name1, ExceptionMessageUtil.valueAndName(f0, name0, f1, name1),
                    numCallsBelowRequirer + 1);
        }
    }

    public static void eq(float f0, String name0, float f1, String name1) {
        eq(f0, name0, f1, name1, 1);
    }

    public static void eq(float f0, String name0, float f1, int numCallsBelowRequirer) {
        if (!(f0 == f1)) {
            fail(name0 + " == " + f1, ExceptionMessageUtil.valueAndName(f0, name0), numCallsBelowRequirer + 1);
        }
    }

    public static void eq(float f0, String name0, float f1) {
        eq(f0, name0, f1, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (d0 == d1)
     */
    public static void eq(double d0, String name0, double d1, String name1, int numCallsBelowRequirer) {
        if (!(d0 == d1)) {
            fail(name0 + " == " + name1, ExceptionMessageUtil.valueAndName(d0, name0, d1, name1),
                    numCallsBelowRequirer + 1);
        }
    }

    public static void eq(double d0, String name0, double d1, String name1) {
        eq(d0, name0, d1, name1, 1);
    }

    public static void eq(double d0, String name0, double d1, int numCallsBelowRequirer) {
        if (!(d0 == d1)) {
            fail(name0 + " == " + d1, ExceptionMessageUtil.valueAndName(d0, name0), numCallsBelowRequirer + 1);
        }
    }

    public static void eq(double d0, String name0, double d1) {
        eq(d0, name0, d1, 1);
    }

    // ################################################################
    // neq (primitiveValue != primitiveValue)

    // ----------------------------------------------------------------
    /**
     * require (b0 != b1)
     */
    public static void neq(boolean b0, String name0, boolean b1, String name1, int numCallsBelowRequirer) {
        if (!(b0 != b1)) {
            fail(name0 + " != " + name1, ExceptionMessageUtil.valueAndName(b0, name0, b1, name1),
                    numCallsBelowRequirer + 1);
        }
    }

    public static void neq(boolean b0, String name0, boolean b1, String name1) {
        neq(b0, name0, b1, name1, 1);
    }

    public static void neq(boolean b0, String name0, boolean b1, int numCallsBelowRequirer) {
        if (!(b0 != b1)) {
            fail(name0 + " != " + b1, ExceptionMessageUtil.valueAndName(b0, name0), numCallsBelowRequirer + 1);
        }
    }

    public static void neq(boolean b0, String name0, boolean b1) {
        neq(b0, name0, b1, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (c0 != c1)
     */
    public static void neq(char c0, String name0, char c1, String name1, int numCallsBelowRequirer) {
        if (!(c0 != c1)) {
            fail(name0 + " != " + name1, ExceptionMessageUtil.valueAndName(c0, name0, c1, name1),
                    numCallsBelowRequirer + 1);
        }
    }

    public static void neq(char c0, String name0, char c1, String name1) {
        neq(c0, name0, c1, name1, 1);
    }

    public static void neq(char c0, String name0, char c1, int numCallsBelowRequirer) {
        if (!(c0 != c1)) {
            fail(name0 + " != " + ExceptionMessageUtil.valueString(c1), ExceptionMessageUtil.valueAndName(c0, name0),
                    numCallsBelowRequirer + 1);
        }
    }

    public static void neq(char c0, String name0, char c1) {
        neq(c0, name0, c1, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (b0 != b1)
     */
    public static void neq(byte b0, String name0, byte b1, String name1, int numCallsBelowRequirer) {
        if (!(b0 != b1)) {
            fail(name0 + " != " + name1, ExceptionMessageUtil.valueAndName(b0, name0, b1, name1),
                    numCallsBelowRequirer + 1);
        }
    }

    public static void neq(byte b0, String name0, byte b1, String name1) {
        neq(b0, name0, b1, name1, 1);
    }

    public static void neq(byte b0, String name0, byte b1, int numCallsBelowRequirer) {
        if (!(b0 != b1)) {
            fail(name0 + " != " + b1, ExceptionMessageUtil.valueAndName(b0, name0), numCallsBelowRequirer + 1);
        }
    }

    public static void neq(byte b0, String name0, byte b1) {
        neq(b0, name0, b1, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (s0 != s1)
     */
    public static void neq(short s0, String name0, short s1, String name1, int numCallsBelowRequirer) {
        if (!(s0 != s1)) {
            fail(name0 + " != " + name1, ExceptionMessageUtil.valueAndName(s0, name0, s1, name1),
                    numCallsBelowRequirer + 1);
        }
    }

    public static void neq(short s0, String name0, short s1, String name1) {
        neq(s0, name0, s1, name1, 1);
    }

    public static void neq(short s0, String name0, short s1, int numCallsBelowRequirer) {
        if (!(s0 != s1)) {
            fail(name0 + " != " + s1, ExceptionMessageUtil.valueAndName(s0, name0), numCallsBelowRequirer + 1);
        }
    }

    public static void neq(short s0, String name0, short s1) {
        neq(s0, name0, s1, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (i0 != i1)
     */
    public static int neq(int i0, String name0, int i1, String name1, int numCallsBelowRequirer) {
        if (!(i0 != i1)) {
            fail(name0 + " != " + name1, ExceptionMessageUtil.valueAndName(i0, name0, i1, name1),
                    numCallsBelowRequirer + 1);
        }
        return i0;
    }

    public static int neq(int i0, String name0, int i1, String name1) {
        return neq(i0, name0, i1, name1, 1);
    }

    public static void neq(int i0, String name0, int i1, int numCallsBelowRequirer) {
        if (!(i0 != i1)) {
            fail(name0 + " != " + i1, ExceptionMessageUtil.valueAndName(i0, name0), numCallsBelowRequirer + 1);
        }
    }

    public static void neq(int i0, String name0, int i1) {
        neq(i0, name0, i1, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (l0 != l1)
     */
    public static void neq(long l0, String name0, long l1, String name1, int numCallsBelowRequirer) {
        if (!(l0 != l1)) {
            fail(name0 + " != " + name1, ExceptionMessageUtil.valueAndName(l0, name0, l1, name1),
                    numCallsBelowRequirer + 1);
        }
    }

    public static void neq(long l0, String name0, long l1, String name1) {
        neq(l0, name0, l1, name1, 1);
    }

    public static void neq(long l0, String name0, long l1, int numCallsBelowRequirer) {
        if (!(l0 != l1)) {
            fail(name0 + " != " + l1, ExceptionMessageUtil.valueAndName(l0, name0), numCallsBelowRequirer + 1);
        }
    }

    public static void neq(long l0, String name0, long l1) {
        neq(l0, name0, l1, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (f0 != f1)
     */
    public static void neq(float f0, String name0, float f1, String name1, int numCallsBelowRequirer) {
        if (!(f0 != f1)) {
            fail(name0 + " != " + name1, ExceptionMessageUtil.valueAndName(f0, name0, f1, name1),
                    numCallsBelowRequirer + 1);
        }
    }

    public static void neq(float f0, String name0, float f1, String name1) {
        neq(f0, name0, f1, name1, 1);
    }

    public static void neq(float f0, String name0, float f1, int numCallsBelowRequirer) {
        if (!(f0 != f1)) {
            fail(name0 + " != " + f1, ExceptionMessageUtil.valueAndName(f0, name0), numCallsBelowRequirer + 1);
        }
    }

    public static void neq(float f0, String name0, float f1) {
        neq(f0, name0, f1, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (d0 != d1)
     */
    public static void neq(double d0, String name0, double d1, String name1, int numCallsBelowRequirer) {
        if (!(d0 != d1)) {
            fail(name0 + " != " + name1, ExceptionMessageUtil.valueAndName(d0, name0, d1, name1),
                    numCallsBelowRequirer + 1);
        }
    }

    public static void neq(double d0, String name0, double d1, String name1) {
        neq(d0, name0, d1, name1, 1);
    }

    public static void neq(double d0, String name0, double d1, int numCallsBelowRequirer) {
        if (!(d0 != d1)) {
            fail(name0 + " != " + d1, ExceptionMessageUtil.valueAndName(d0, name0), numCallsBelowRequirer + 1);
        }
    }

    public static void neq(double d0, String name0, double d1) {
        neq(d0, name0, d1, 1);
    }

    // ################################################################
    // lt (primitiveValue < primitiveValue)

    // ----------------------------------------------------------------
    /**
     * require (c0 < c1)
     */
    public static char lt(char c0, String name0, char c1, String name1, int numCallsBelowRequirer) {
        if (!(c0 < c1)) {
            fail(name0 + " < " + name1, ExceptionMessageUtil.valueAndName(c0, name0, c1, name1),
                    numCallsBelowRequirer + 1);
        }
        return c0;
    }

    public static char lt(char c0, String name0, char c1, String name1) {
        return lt(c0, name0, c1, name1, 1);
    }

    public static char lt(char c0, String name0, char c1, int numCallsBelowRequirer) {
        if (!(c0 < c1)) {
            fail(name0 + " < " + ExceptionMessageUtil.valueString(c1), ExceptionMessageUtil.valueAndName(c0, name0),
                    numCallsBelowRequirer + 1);
        }
        return c0;
    }

    public static char lt(char c0, String name0, char c1) {
        return lt(c0, name0, c1, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (b0 < b1)
     */
    public static byte lt(byte b0, String name0, byte b1, String name1, int numCallsBelowRequirer) {
        if (!(b0 < b1)) {
            fail(name0 + " < " + name1, ExceptionMessageUtil.valueAndName(b0, name0, b1, name1),
                    numCallsBelowRequirer + 1);
        }
        return b0;
    }

    public static byte lt(byte b0, String name0, byte b1, String name1) {
        return lt(b0, name0, b1, name1, 1);
    }

    public static byte lt(byte b0, String name0, byte b1, int numCallsBelowRequirer) {
        if (!(b0 < b1)) {
            fail(name0 + " < " + b1, ExceptionMessageUtil.valueAndName(b0, name0), numCallsBelowRequirer + 1);
        }
        return b0;
    }

    public static byte lt(byte b0, String name0, byte b1) {
        return lt(b0, name0, b1, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (s0 < s1)
     */
    public static short lt(short s0, String name0, short s1, String name1, int numCallsBelowRequirer) {
        if (!(s0 < s1)) {
            fail(name0 + " < " + name1, ExceptionMessageUtil.valueAndName(s0, name0, s1, name1),
                    numCallsBelowRequirer + 1);
        }
        return s0;
    }

    public static short lt(short s0, String name0, short s1, String name1) {
        return lt(s0, name0, s1, name1, 1);
    }

    public static short lt(short s0, String name0, short s1, int numCallsBelowRequirer) {
        if (!(s0 < s1)) {
            fail(name0 + " < " + s1, ExceptionMessageUtil.valueAndName(s0, name0), numCallsBelowRequirer + 1);
        }
        return s0;
    }

    public static short lt(short s0, String name0, short s1) {
        return lt(s0, name0, s1, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (i0 < i1)
     */
    public static int lt(int i0, String name0, int i1, String name1, int numCallsBelowRequirer) {
        if (!(i0 < i1)) {
            fail(name0 + " < " + name1, ExceptionMessageUtil.valueAndName(i0, name0, i1, name1),
                    numCallsBelowRequirer + 1);
        }
        return i0;
    }

    public static int lt(int i0, String name0, int i1, String name1) {
        return lt(i0, name0, i1, name1, 1);
    }

    public static int lt(int i0, String name0, int i1, int numCallsBelowRequirer) {
        if (!(i0 < i1)) {
            fail(name0 + " < " + i1, ExceptionMessageUtil.valueAndName(i0, name0), numCallsBelowRequirer + 1);
        }
        return i0;
    }

    public static int lt(int i0, String name0, int i1) {
        return lt(i0, name0, i1, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (l0 < l1)
     */
    public static long lt(long l0, String name0, long l1, String name1, int numCallsBelowRequirer) {
        if (!(l0 < l1)) {
            fail(name0 + " < " + name1, ExceptionMessageUtil.valueAndName(l0, name0, l1, name1),
                    numCallsBelowRequirer + 1);
        }
        return l0;
    }

    public static long lt(long l0, String name0, long l1, String name1) {
        return lt(l0, name0, l1, name1, 1);
    }

    public static long lt(long l0, String name0, long l1, int numCallsBelowRequirer) {
        if (!(l0 < l1)) {
            fail(name0 + " < " + l1, ExceptionMessageUtil.valueAndName(l0, name0), numCallsBelowRequirer + 1);
        }
        return l0;
    }

    public static long lt(long l0, String name0, long l1) {
        return lt(l0, name0, l1, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (f0 < f1)
     */
    public static float lt(float f0, String name0, float f1, String name1, int numCallsBelowRequirer) {
        if (!(f0 < f1)) {
            fail(name0 + " < " + name1, ExceptionMessageUtil.valueAndName(f0, name0, f1, name1),
                    numCallsBelowRequirer + 1);
        }
        return f0;
    }

    public static float lt(float f0, String name0, float f1, String name1) {
        return lt(f0, name0, f1, name1, 1);
    }

    public static float lt(float f0, String name0, float f1, int numCallsBelowRequirer) {
        if (!(f0 < f1)) {
            fail(name0 + " < " + f1, ExceptionMessageUtil.valueAndName(f0, name0), numCallsBelowRequirer + 1);
        }
        return f0;
    }

    public static float lt(float f0, String name0, float f1) {
        return lt(f0, name0, f1, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (d0 < d1)
     */
    public static double lt(double d0, String name0, double d1, String name1, int numCallsBelowRequirer) {
        if (!(d0 < d1)) {
            fail(name0 + " < " + name1, ExceptionMessageUtil.valueAndName(d0, name0, d1, name1),
                    numCallsBelowRequirer + 1);
        }
        return d0;
    }

    public static double lt(double d0, String name0, double d1, String name1) {
        return lt(d0, name0, d1, name1, 1);
    }

    public static double lt(double d0, String name0, double d1, int numCallsBelowRequirer) {
        if (!(d0 < d1)) {
            fail(name0 + " < " + d1, ExceptionMessageUtil.valueAndName(d0, name0), numCallsBelowRequirer + 1);
        }
        return d0;
    }

    public static double lt(double d0, String name0, double d1) {
        return lt(d0, name0, d1, 1);
    }

    // ################################################################
    // leq (primitiveValue <= primitiveValue)

    // ----------------------------------------------------------------
    /**
     * require (c0 <= c1)
     */
    public static char leq(char c0, String name0, char c1, String name1, int numCallsBelowRequirer) {
        if (!(c0 <= c1)) {
            fail(name0 + " <= " + name1, ExceptionMessageUtil.valueAndName(c0, name0, c1, name1),
                    numCallsBelowRequirer + 1);
        }
        return c0;
    }

    public static char leq(char c0, String name0, char c1, String name1) {
        return leq(c0, name0, c1, name1, 1);
    }

    public static char leq(char c0, String name0, char c1, int numCallsBelowRequirer) {
        if (!(c0 <= c1)) {
            fail(name0 + " <= " + ExceptionMessageUtil.valueString(c1), ExceptionMessageUtil.valueAndName(c0, name0),
                    numCallsBelowRequirer + 1);
        }
        return c0;
    }

    public static char leq(char c0, String name0, char c1) {
        return leq(c0, name0, c1, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (b0 <= b1)
     */
    public static byte leq(byte b0, String name0, byte b1, String name1, int numCallsBelowRequirer) {
        if (!(b0 <= b1)) {
            fail(name0 + " <= " + name1, ExceptionMessageUtil.valueAndName(b0, name0, b1, name1),
                    numCallsBelowRequirer + 1);
        }
        return b0;
    }

    public static byte leq(byte b0, String name0, byte b1, String name1) {
        return leq(b0, name0, b1, name1, 1);
    }

    public static byte leq(byte b0, String name0, byte b1, int numCallsBelowRequirer) {
        if (!(b0 <= b1)) {
            fail(name0 + " <= " + b1, ExceptionMessageUtil.valueAndName(b0, name0), numCallsBelowRequirer + 1);
        }
        return b0;
    }

    public static byte leq(byte b0, String name0, byte b1) {
        return leq(b0, name0, b1, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (s0 <= s1)
     */
    public static short leq(short s0, String name0, short s1, String name1, int numCallsBelowRequirer) {
        if (!(s0 <= s1)) {
            fail(name0 + " <= " + name1, ExceptionMessageUtil.valueAndName(s0, name0, s1, name1),
                    numCallsBelowRequirer + 1);
        }
        return s0;
    }

    public static short leq(short s0, String name0, short s1, String name1) {
        return leq(s0, name0, s1, name1, 1);
    }

    public static short leq(short s0, String name0, short s1, int numCallsBelowRequirer) {
        if (!(s0 <= s1)) {
            fail(name0 + " <= " + s1, ExceptionMessageUtil.valueAndName(s0, name0), numCallsBelowRequirer + 1);
        }
        return s0;
    }

    public static short leq(short s0, String name0, short s1) {
        return leq(s0, name0, s1, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (i0 <= i1)
     */
    public static int leq(int i0, String name0, int i1, String name1, int numCallsBelowRequirer) {
        if (!(i0 <= i1)) {
            fail(name0 + " <= " + name1, ExceptionMessageUtil.valueAndName(i0, name0, i1, name1),
                    numCallsBelowRequirer + 1);
        }
        return i0;
    }

    public static int leq(int i0, String name0, int i1, String name1) {
        return leq(i0, name0, i1, name1, 1);
    }

    public static int leq(int i0, String name0, int i1, int numCallsBelowRequirer) {
        if (!(i0 <= i1)) {
            fail(name0 + " <= " + i1, ExceptionMessageUtil.valueAndName(i0, name0), numCallsBelowRequirer + 1);
        }
        return i0;
    }

    public static int leq(int i0, String name0, int i1) {
        return leq(i0, name0, i1, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (l0 <= l1)
     */
    public static long leq(long l0, String name0, long l1, String name1, int numCallsBelowRequirer) {
        if (!(l0 <= l1)) {
            fail(name0 + " <= " + name1, ExceptionMessageUtil.valueAndName(l0, name0, l1, name1),
                    numCallsBelowRequirer + 1);
        }
        return l0;
    }

    public static long leq(long l0, String name0, long l1, String name1) {
        return leq(l0, name0, l1, name1, 1);
    }

    public static long leq(long l0, String name0, long l1, int numCallsBelowRequirer) {
        if (!(l0 <= l1)) {
            fail(name0 + " <= " + l1, ExceptionMessageUtil.valueAndName(l0, name0), numCallsBelowRequirer + 1);
        }
        return l0;
    }

    public static long leq(long l0, String name0, long l1) {
        return leq(l0, name0, l1, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (f0 <= f1)
     */
    public static float leq(float f0, String name0, float f1, String name1, int numCallsBelowRequirer) {
        if (!(f0 <= f1)) {
            fail(name0 + " <= " + name1, ExceptionMessageUtil.valueAndName(f0, name0, f1, name1),
                    numCallsBelowRequirer + 1);
        }
        return f0;
    }

    public static float leq(float f0, String name0, float f1, String name1) {
        return leq(f0, name0, f1, name1, 1);
    }

    public static float leq(float f0, String name0, float f1, int numCallsBelowRequirer) {
        if (!(f0 <= f1)) {
            fail(name0 + " <= " + f1, ExceptionMessageUtil.valueAndName(f0, name0), numCallsBelowRequirer + 1);
        }
        return f0;
    }

    public static float leq(float f0, String name0, float f1) {
        return leq(f0, name0, f1, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (d0 <= d1)
     */
    public static double leq(double d0, String name0, double d1, String name1, int numCallsBelowRequirer) {
        if (!(d0 <= d1)) {
            fail(name0 + " <= " + name1, ExceptionMessageUtil.valueAndName(d0, name0, d1, name1),
                    numCallsBelowRequirer + 1);
        }
        return d0;
    }

    public static double leq(double d0, String name0, double d1, String name1) {
        return leq(d0, name0, d1, name1, 1);
    }

    public static double leq(double d0, String name0, double d1, int numCallsBelowRequirer) {
        if (!(d0 <= d1)) {
            fail(name0 + " <= " + d1, ExceptionMessageUtil.valueAndName(d0, name0), numCallsBelowRequirer + 1);
        }
        return d0;
    }

    public static double leq(double d0, String name0, double d1) {
        return leq(d0, name0, d1, 1);
    }

    // ################################################################
    // gt (primitiveValue > primitiveValue)

    // ----------------------------------------------------------------
    /**
     * require (c0 > c1)
     */
    public static char gt(char c0, String name0, char c1, String name1, int numCallsBelowRequirer) {
        if (!(c0 > c1)) {
            fail(name0 + " > " + name1, ExceptionMessageUtil.valueAndName(c0, name0, c1, name1),
                    numCallsBelowRequirer + 1);
        }
        return c0;
    }

    public static char gt(char c0, String name0, char c1, String name1) {
        return gt(c0, name0, c1, name1, 1);
    }

    public static char gt(char c0, String name0, char c1, int numCallsBelowRequirer) {
        if (!(c0 > c1)) {
            fail(name0 + " > " + ExceptionMessageUtil.valueString(c1), ExceptionMessageUtil.valueAndName(c0, name0),
                    numCallsBelowRequirer + 1);
        }
        return c0;
    }

    public static char gt(char c0, String name0, char c1) {
        return gt(c0, name0, c1, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (b0 > b1)
     */
    public static byte gt(byte b0, String name0, byte b1, String name1, int numCallsBelowRequirer) {
        if (!(b0 > b1)) {
            fail(name0 + " > " + name1, ExceptionMessageUtil.valueAndName(b0, name0, b1, name1),
                    numCallsBelowRequirer + 1);
        }
        return b0;
    }

    public static byte gt(byte b0, String name0, byte b1, String name1) {
        return gt(b0, name0, b1, name1, 1);
    }

    public static byte gt(byte b0, String name0, byte b1, int numCallsBelowRequirer) {
        if (!(b0 > b1)) {
            fail(name0 + " > " + b1, ExceptionMessageUtil.valueAndName(b0, name0), numCallsBelowRequirer + 1);
        }
        return b0;
    }

    public static byte gt(byte b0, String name0, byte b1) {
        return gt(b0, name0, b1, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (s0 > s1)
     */
    public static short gt(short s0, String name0, short s1, String name1, int numCallsBelowRequirer) {
        if (!(s0 > s1)) {
            fail(name0 + " > " + name1, ExceptionMessageUtil.valueAndName(s0, name0, s1, name1),
                    numCallsBelowRequirer + 1);
        }
        return s0;
    }

    public static short gt(short s0, String name0, short s1, String name1) {
        return gt(s0, name0, s1, name1, 1);
    }

    public static short gt(short s0, String name0, short s1, int numCallsBelowRequirer) {
        if (!(s0 > s1)) {
            fail(name0 + " > " + s1, ExceptionMessageUtil.valueAndName(s0, name0), numCallsBelowRequirer + 1);
        }
        return s0;
    }

    public static short gt(short s0, String name0, short s1) {
        return gt(s0, name0, s1, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (i0 > i1)
     */
    public static int gt(int i0, String name0, int i1, String name1, int numCallsBelowRequirer) {
        if (!(i0 > i1)) {
            fail(name0 + " > " + name1, ExceptionMessageUtil.valueAndName(i0, name0, i1, name1),
                    numCallsBelowRequirer + 1);
        }
        return i0;
    }

    public static int gt(int i0, String name0, int i1, String name1) {
        return gt(i0, name0, i1, name1, 1);
    }

    public static int gt(int i0, String name0, int i1, int numCallsBelowRequirer) {
        if (!(i0 > i1)) {
            fail(name0 + " > " + i1, ExceptionMessageUtil.valueAndName(i0, name0), numCallsBelowRequirer + 1);
        }
        return i0;
    }

    public static int gt(int i0, String name0, int i1) {
        return gt(i0, name0, i1, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (l0 > l1)
     */
    public static long gt(long l0, String name0, long l1, String name1, int numCallsBelowRequirer) {
        if (!(l0 > l1)) {
            fail(name0 + " > " + name1, ExceptionMessageUtil.valueAndName(l0, name0, l1, name1),
                    numCallsBelowRequirer + 1);
        }
        return l0;
    }

    public static long gt(long l0, String name0, long l1, String name1) {
        return gt(l0, name0, l1, name1, 1);
    }

    public static long gt(long l0, String name0, long l1, int numCallsBelowRequirer) {
        if (!(l0 > l1)) {
            fail(name0 + " > " + l1, ExceptionMessageUtil.valueAndName(l0, name0), numCallsBelowRequirer + 1);
        }
        return l0;
    }

    public static long gt(long l0, String name0, long l1) {
        return gt(l0, name0, l1, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (f0 > f1)
     */
    public static float gt(float f0, String name0, float f1, String name1, int numCallsBelowRequirer) {
        if (!(f0 > f1)) {
            fail(name0 + " > " + name1, ExceptionMessageUtil.valueAndName(f0, name0, f1, name1),
                    numCallsBelowRequirer + 1);
        }
        return f0;
    }

    public static float gt(float f0, String name0, float f1, String name1) {
        return gt(f0, name0, f1, name1, 1);
    }

    public static float gt(float f0, String name0, float f1, int numCallsBelowRequirer) {
        if (!(f0 > f1)) {
            fail(name0 + " > " + f1, ExceptionMessageUtil.valueAndName(f0, name0), numCallsBelowRequirer + 1);
        }
        return f0;
    }

    public static float gt(float f0, String name0, float f1) {
        return gt(f0, name0, f1, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (d0 > d1)
     */
    public static double gt(double d0, String name0, double d1, String name1, int numCallsBelowRequirer) {
        if (!(d0 > d1)) {
            fail(name0 + " > " + name1, ExceptionMessageUtil.valueAndName(d0, name0, d1, name1),
                    numCallsBelowRequirer + 1);
        }
        return d0;
    }

    public static double gt(double d0, String name0, double d1, String name1) {
        return gt(d0, name0, d1, name1, 1);
    }

    public static double gt(double d0, String name0, double d1, int numCallsBelowRequirer) {
        if (!(d0 > d1)) {
            fail(name0 + " > " + d1, ExceptionMessageUtil.valueAndName(d0, name0), numCallsBelowRequirer + 1);
        }
        return d0;
    }

    public static double gt(double d0, String name0, double d1) {
        return gt(d0, name0, d1, 1);
    }

    // ################################################################
    // geq (primitiveValue >= primitiveValue)

    // ----------------------------------------------------------------
    /**
     * require (c0 >= c1)
     */
    public static char geq(char c0, String name0, char c1, String name1, int numCallsBelowRequirer) {
        if (!(c0 >= c1)) {
            fail(name0 + " >= " + name1, ExceptionMessageUtil.valueAndName(c0, name0, c1, name1),
                    numCallsBelowRequirer + 1);
        }
        return c0;
    }

    public static char geq(char c0, String name0, char c1, String name1) {
        return geq(c0, name0, c1, name1, 1);
    }

    public static char geq(char c0, String name0, char c1, int numCallsBelowRequirer) {
        if (!(c0 >= c1)) {
            fail(name0 + " >= " + ExceptionMessageUtil.valueString(c1), ExceptionMessageUtil.valueAndName(c0, name0),
                    numCallsBelowRequirer + 1);
        }
        return c0;
    }

    public static char geq(char c0, String name0, char c1) {
        return geq(c0, name0, c1, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (b0 >= b1)
     */
    public static byte geq(byte b0, String name0, byte b1, String name1, int numCallsBelowRequirer) {
        if (!(b0 >= b1)) {
            fail(name0 + " >= " + name1, ExceptionMessageUtil.valueAndName(b0, name0, b1, name1),
                    numCallsBelowRequirer + 1);
        }
        return b0;
    }

    public static byte geq(byte b0, String name0, byte b1, String name1) {
        return geq(b0, name0, b1, name1, 1);
    }

    public static byte geq(byte b0, String name0, byte b1, int numCallsBelowRequirer) {
        if (!(b0 >= b1)) {
            fail(name0 + " >= " + b1, ExceptionMessageUtil.valueAndName(b0, name0), numCallsBelowRequirer + 1);
        }
        return b0;
    }

    public static byte geq(byte b0, String name0, byte b1) {
        return geq(b0, name0, b1, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (s0 >= s1)
     */
    public static short geq(short s0, String name0, short s1, String name1, int numCallsBelowRequirer) {
        if (!(s0 >= s1)) {
            fail(name0 + " >= " + name1, ExceptionMessageUtil.valueAndName(s0, name0, s1, name1),
                    numCallsBelowRequirer + 1);
        }
        return s0;
    }

    public static short geq(short s0, String name0, short s1, String name1) {
        return geq(s0, name0, s1, name1, 1);
    }

    public static short geq(short s0, String name0, short s1, int numCallsBelowRequirer) {
        if (!(s0 >= s1)) {
            fail(name0 + " >= " + s1, ExceptionMessageUtil.valueAndName(s0, name0), numCallsBelowRequirer + 1);
        }
        return s0;
    }

    public static short geq(short s0, String name0, short s1) {
        return geq(s0, name0, s1, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (i0 >= i1)
     */
    public static int geq(int i0, String name0, int i1, String name1, int numCallsBelowRequirer) {
        if (!(i0 >= i1)) {
            fail(name0 + " >= " + name1, ExceptionMessageUtil.valueAndName(i0, name0, i1, name1),
                    numCallsBelowRequirer + 1);
        }
        return i0;
    }

    public static int geq(int i0, String name0, int i1, String name1) {
        return geq(i0, name0, i1, name1, 1);
    }

    public static int geq(int i0, String name0, int i1, int numCallsBelowRequirer) {
        if (!(i0 >= i1)) {
            fail(name0 + " >= " + i1, ExceptionMessageUtil.valueAndName(i0, name0), numCallsBelowRequirer + 1);
        }
        return i0;
    }

    public static int geq(int i0, String name0, int i1) {
        return geq(i0, name0, i1, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (l0 >= l1)
     */
    public static long geq(long l0, String name0, long l1, String name1, int numCallsBelowRequirer) {
        if (!(l0 >= l1)) {
            fail(name0 + " >= " + name1, ExceptionMessageUtil.valueAndName(l0, name0, l1, name1),
                    numCallsBelowRequirer + 1);
        }
        return l0;
    }

    public static long geq(long l0, String name0, long l1, String name1) {
        return geq(l0, name0, l1, name1, 1);
    }

    public static long geq(long l0, String name0, long l1, int numCallsBelowRequirer) {
        if (!(l0 >= l1)) {
            fail(name0 + " >= " + l1, ExceptionMessageUtil.valueAndName(l0, name0), numCallsBelowRequirer + 1);
        }
        return l0;
    }

    public static long geq(long l0, String name0, long l1) {
        return geq(l0, name0, l1, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (f0 >= f1)
     */
    public static float geq(float f0, String name0, float f1, String name1, int numCallsBelowRequirer) {
        if (!(f0 >= f1)) {
            fail(name0 + " >= " + name1, ExceptionMessageUtil.valueAndName(f0, name0, f1, name1),
                    numCallsBelowRequirer + 1);
        }
        return f0;
    }

    public static float geq(float f0, String name0, float f1, String name1) {
        return geq(f0, name0, f1, name1, 1);
    }

    public static float geq(float f0, String name0, float f1, int numCallsBelowRequirer) {
        if (!(f0 >= f1)) {
            fail(name0 + " >= " + f1, ExceptionMessageUtil.valueAndName(f0, name0), numCallsBelowRequirer + 1);
        }
        return f0;
    }

    public static float geq(float f0, String name0, float f1) {
        return geq(f0, name0, f1, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (d0 >= d1)
     */
    public static double geq(double d0, String name0, double d1, String name1, int numCallsBelowRequirer) {
        if (!(d0 >= d1)) {
            fail(name0 + " >= " + name1, ExceptionMessageUtil.valueAndName(d0, name0, d1, name1),
                    numCallsBelowRequirer + 1);
        }
        return d0;
    }

    public static double geq(double d0, String name0, double d1, String name1) {
        return geq(d0, name0, d1, name1, 1);
    }

    public static double geq(double d0, String name0, double d1, int numCallsBelowRequirer) {
        if (!(d0 >= d1)) {
            fail(name0 + " >= " + d1, ExceptionMessageUtil.valueAndName(d0, name0), numCallsBelowRequirer + 1);
        }
        return d0;
    }

    public static double geq(double d0, String name0, double d1) {
        return geq(d0, name0, d1, 1);
    }

    // ################################################################
    // eqFalse, neqFalse, eqTrue, neqTrue (boolean ==/!= false/true)

    // ----------------------------------------------------------------

    /**
     * require (b == false)
     */
    public static void eqFalse(boolean b, String name, int numCallsBelowRequirer) {
        if (!(false == b)) {
            fail(name + " == false", ExceptionMessageUtil.valueAndName(b, name), numCallsBelowRequirer + 1);
        }
    }

    public static void eqFalse(boolean b, String name) {
        eqFalse(b, name, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (b != false)
     */
    public static void neqFalse(boolean b, String name, int numCallsBelowRequirer) {
        if (!(false != b)) {
            fail(name + " != false", ExceptionMessageUtil.valueAndName(b, name), numCallsBelowRequirer + 1);
        }
    }

    public static void neqFalse(boolean b, String name) {
        neqFalse(b, name, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (b == true)
     */
    public static void eqTrue(boolean b, String name, int numCallsBelowRequirer) {
        if (!(true == b)) {
            fail(name + " == true", ExceptionMessageUtil.valueAndName(b, name), numCallsBelowRequirer + 1);
        }
    }

    public static void eqTrue(boolean b, String name) {
        eqTrue(b, name, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (b != true)
     */
    public static void neqTrue(boolean b, String name, int numCallsBelowRequirer) {
        if (!(true != b)) {
            fail(name + " != true", ExceptionMessageUtil.valueAndName(b, name), numCallsBelowRequirer + 1);
        }
    }

    public static void neqTrue(boolean b, String name) {
        neqTrue(b, name, 1);
    }

    // ################################################################
    // eqZero (primitiveValue == 0)

    // ----------------------------------------------------------------
    /**
     * require (c == 0)
     */
    public static void eqZero(char c, String name, int numCallsBelowRequirer) {
        if (!(0 == c)) {
            fail(name + " == 0", ExceptionMessageUtil.valueAndName(c, name), numCallsBelowRequirer + 1);
        }
    }

    public static void eqZero(char c, String name) {
        eqZero(c, name, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (b == 0)
     */
    public static void eqZero(byte b, String name, int numCallsBelowRequirer) {
        if (!(0 == b)) {
            fail(name + " == 0", ExceptionMessageUtil.valueAndName(b, name), numCallsBelowRequirer + 1);
        }
    }

    public static void eqZero(byte b, String name) {
        eqZero(b, name, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (s == 0)
     */
    public static void eqZero(short s, String name, int numCallsBelowRequirer) {
        if (!(0 == s)) {
            fail(name + " == 0", ExceptionMessageUtil.valueAndName(s, name), numCallsBelowRequirer + 1);
        }
    }

    public static void eqZero(short s, String name) {
        eqZero(s, name, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (i == 0)
     */
    public static void eqZero(int i, String name, int numCallsBelowRequirer) {
        if (!(0 == i)) {
            fail(name + " == 0", ExceptionMessageUtil.valueAndName(i, name), numCallsBelowRequirer + 1);
        }
    }

    public static void eqZero(int i, String name) {
        eqZero(i, name, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (l == 0)
     */
    public static void eqZero(long l, String name, int numCallsBelowRequirer) {
        if (!(0 == l)) {
            fail(name + " == 0", ExceptionMessageUtil.valueAndName(l, name), numCallsBelowRequirer + 1);
        }
    }

    public static void eqZero(long l, String name) {
        eqZero(l, name, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (f == 0)
     */
    public static void eqZero(float f, String name, int numCallsBelowRequirer) {
        if (!(0 == f)) {
            fail(name + " == 0", ExceptionMessageUtil.valueAndName(f, name), numCallsBelowRequirer + 1);
        }
    }

    public static void eqZero(float f, String name) {
        eqZero(f, name, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (d == 0)
     */
    public static void eqZero(double d, String name, int numCallsBelowRequirer) {
        if (!(0 == d)) {
            fail(name + " == 0", ExceptionMessageUtil.valueAndName(d, name), numCallsBelowRequirer + 1);
        }
    }

    public static void eqZero(double d, String name) {
        eqZero(d, name, 1);
    }

    // ################################################################
    // neqZero (primitiveValue != 0)

    // ----------------------------------------------------------------
    /**
     * require (c != 0)
     */
    public static char neqZero(char c, String name, int numCallsBelowRequirer) {
        if (!(0 != c)) {
            fail(name + " != 0", ExceptionMessageUtil.valueAndName(c, name), numCallsBelowRequirer + 1);
        }
        return c;
    }

    public static char neqZero(char c, String name) {
        return neqZero(c, name, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (b != 0)
     */
    public static byte neqZero(byte b, String name, int numCallsBelowRequirer) {
        if (!(0 != b)) {
            fail(name + " != 0", ExceptionMessageUtil.valueAndName(b, name), numCallsBelowRequirer + 1);
        }
        return b;
    }

    public static byte neqZero(byte b, String name) {
        return neqZero(b, name, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (s != 0)
     */
    public static short neqZero(short s, String name, int numCallsBelowRequirer) {
        if (!(0 != s)) {
            fail(name + " != 0", ExceptionMessageUtil.valueAndName(s, name), numCallsBelowRequirer + 1);
        }
        return s;
    }

    public static short neqZero(short s, String name) {
        return neqZero(s, name, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (i != 0)
     */
    public static int neqZero(int i, String name, int numCallsBelowRequirer) {
        if (!(0 != i)) {
            fail(name + " != 0", ExceptionMessageUtil.valueAndName(i, name), numCallsBelowRequirer + 1);
        }
        return i;
    }

    public static int neqZero(int i, String name) {
        return neqZero(i, name, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (l != 0)
     */
    public static long neqZero(long l, String name, int numCallsBelowRequirer) {
        if (!(0 != l)) {
            fail(name + " != 0", ExceptionMessageUtil.valueAndName(l, name), numCallsBelowRequirer + 1);
        }
        return l;
    }

    public static long neqZero(long l, String name) {
        return neqZero(l, name, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (f != 0)
     */
    public static float neqZero(float f, String name, int numCallsBelowRequirer) {
        if (!(0 != f)) {
            fail(name + " != 0", ExceptionMessageUtil.valueAndName(f, name), numCallsBelowRequirer + 1);
        }
        return f;
    }

    public static float neqZero(float f, String name) {
        return neqZero(f, name, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (d != 0)
     */
    public static double neqZero(double d, String name, int numCallsBelowRequirer) {
        if (!(0 != d)) {
            fail(name + " != 0", ExceptionMessageUtil.valueAndName(d, name), numCallsBelowRequirer + 1);
        }
        return d;
    }

    public static double neqZero(double d, String name) {
        return neqZero(d, name, 1);
    }

    // ################################################################
    // ltZero (primitiveValue < 0)

    // ----------------------------------------------------------------
    /**
     * require (b < 0)
     */
    public static byte ltZero(byte b, String name, int numCallsBelowRequirer) {
        if (!(b < 0)) {
            fail(name + " < 0", ExceptionMessageUtil.valueAndName(b, name), numCallsBelowRequirer + 1);
        }
        return b;
    }

    public static byte ltZero(byte b, String name) {
        return ltZero(b, name, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (s < 0)
     */
    public static short ltZero(short s, String name, int numCallsBelowRequirer) {
        if (!(s < 0)) {
            fail(name + " < 0", ExceptionMessageUtil.valueAndName(s, name), numCallsBelowRequirer + 1);
        }
        return s;
    }

    public static short ltZero(short s, String name) {
        return ltZero(s, name, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (i < 0)
     */
    public static int ltZero(int i, String name, int numCallsBelowRequirer) {
        if (!(i < 0)) {
            fail(name + " < 0", ExceptionMessageUtil.valueAndName(i, name), numCallsBelowRequirer + 1);
        }
        return i;
    }

    public static int ltZero(int i, String name) {
        return ltZero(i, name, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (l < 0)
     */
    public static long ltZero(long l, String name, int numCallsBelowRequirer) {
        if (!(l < 0)) {
            fail(name + " < 0", ExceptionMessageUtil.valueAndName(l, name), numCallsBelowRequirer + 1);
        }
        return l;
    }

    public static long ltZero(long l, String name) {
        return ltZero(l, name, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (f < 0)
     */
    public static float ltZero(float f, String name, int numCallsBelowRequirer) {
        if (!(f < 0)) {
            fail(name + " < 0", ExceptionMessageUtil.valueAndName(f, name), numCallsBelowRequirer + 1);
        }
        return f;
    }

    public static float ltZero(float f, String name) {
        return ltZero(f, name, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (d < 0)
     */
    public static double ltZero(double d, String name, int numCallsBelowRequirer) {
        if (!(d < 0)) {
            fail(name + " < 0", ExceptionMessageUtil.valueAndName(d, name), numCallsBelowRequirer + 1);
        }
        return d;
    }

    public static double ltZero(double d, String name) {
        return ltZero(d, name, 1);
    }

    // ################################################################
    // leqZero (primitiveValue <= 0)

    // ----------------------------------------------------------------
    /**
     * require (b <= 0)
     */
    public static byte leqZero(byte b, String name, int numCallsBelowRequirer) {
        if (!(b <= 0)) {
            fail(name + " <= 0", ExceptionMessageUtil.valueAndName(b, name), numCallsBelowRequirer + 1);
        }
        return b;
    }

    public static byte leqZero(byte b, String name) {
        return leqZero(b, name, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (s <= 0)
     */
    public static short leqZero(short s, String name, int numCallsBelowRequirer) {
        if (!(s <= 0)) {
            fail(name + " <= 0", ExceptionMessageUtil.valueAndName(s, name), numCallsBelowRequirer + 1);
        }
        return s;
    }

    public static short leqZero(short s, String name) {
        return leqZero(s, name, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (i <= 0)
     */
    public static int leqZero(int i, String name, int numCallsBelowRequirer) {
        if (!(i <= 0)) {
            fail(name + " <= 0", ExceptionMessageUtil.valueAndName(i, name), numCallsBelowRequirer + 1);
        }
        return i;
    }

    public static int leqZero(int i, String name) {
        return leqZero(i, name, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (l <= 0)
     */
    public static long leqZero(long l, String name, int numCallsBelowRequirer) {
        if (!(l <= 0)) {
            fail(name + " <= 0", ExceptionMessageUtil.valueAndName(l, name), numCallsBelowRequirer + 1);
        }
        return l;
    }

    public static long leqZero(long l, String name) {
        return leqZero(l, name, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (f <= 0)
     */
    public static float leqZero(float f, String name, int numCallsBelowRequirer) {
        if (!(f <= 0)) {
            fail(name + " <= 0", ExceptionMessageUtil.valueAndName(f, name), numCallsBelowRequirer + 1);
        }
        return f;
    }

    public static float leqZero(float f, String name) {
        return leqZero(f, name, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (d <= 0)
     */
    public static double leqZero(double d, String name, int numCallsBelowRequirer) {
        if (!(d <= 0)) {
            fail(name + " <= 0", ExceptionMessageUtil.valueAndName(d, name), numCallsBelowRequirer + 1);
        }
        return d;
    }

    public static double leqZero(double d, String name) {
        return leqZero(d, name, 1);
    }

    // ################################################################
    // gtZero (primitiveValue > 0)

    // ----------------------------------------------------------------
    /**
     * require (b > 0)
     */
    public static byte gtZero(byte b, String name, int numCallsBelowRequirer) {
        if (!(b > 0)) {
            fail(name + " > 0", ExceptionMessageUtil.valueAndName(b, name), numCallsBelowRequirer + 1);
        }
        return b;
    }

    public static byte gtZero(byte b, String name) {
        return gtZero(b, name, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (s > 0)
     */
    public static short gtZero(short s, String name, int numCallsBelowRequirer) {
        if (!(s > 0)) {
            fail(name + " > 0", ExceptionMessageUtil.valueAndName(s, name), numCallsBelowRequirer + 1);
        }
        return s;
    }

    public static short gtZero(short s, String name) {
        return gtZero(s, name, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (i > 0)
     */
    public static int gtZero(int i, String name, int numCallsBelowRequirer) {
        if (!(i > 0)) {
            fail(name + " > 0", ExceptionMessageUtil.valueAndName(i, name), numCallsBelowRequirer + 1);
        }
        return i;
    }

    public static int gtZero(int i, String name) {
        return gtZero(i, name, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (l > 0)
     */
    public static long gtZero(long l, String name, int numCallsBelowRequirer) {
        if (!(l > 0)) {
            fail(name + " > 0", ExceptionMessageUtil.valueAndName(l, name), numCallsBelowRequirer + 1);
        }
        return l;
    }

    public static long gtZero(long l, String name) {
        return gtZero(l, name, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (f > 0)
     */
    public static float gtZero(float f, String name, int numCallsBelowRequirer) {
        if (!(f > 0)) {
            fail(name + " > 0", ExceptionMessageUtil.valueAndName(f, name), numCallsBelowRequirer + 1);
        }
        return f;
    }

    public static float gtZero(float f, String name) {
        return gtZero(f, name, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (d > 0)
     */
    public static double gtZero(double d, String name, int numCallsBelowRequirer) {
        if (!(d > 0)) {
            fail(name + " > 0", ExceptionMessageUtil.valueAndName(d, name), numCallsBelowRequirer + 1);
        }
        return d;
    }

    public static double gtZero(double d, String name) {
        return gtZero(d, name, 1);
    }

    // ################################################################
    // geqZero (primitiveValue >= 0)

    // ----------------------------------------------------------------
    /**
     * require (b >= 0)
     */
    public static byte geqZero(byte b, String name, int numCallsBelowRequirer) {
        if (!(b >= 0)) {
            fail(name + " >= 0", ExceptionMessageUtil.valueAndName(b, name), numCallsBelowRequirer + 1);
        }
        return b;
    }

    public static byte geqZero(byte b, String name) {
        return geqZero(b, name, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (s >= 0)
     */
    public static short geqZero(short s, String name, int numCallsBelowRequirer) {
        if (!(s >= 0)) {
            fail(name + " >= 0", ExceptionMessageUtil.valueAndName(s, name), numCallsBelowRequirer + 1);
        }
        return s;
    }

    public static short geqZero(short s, String name) {
        return geqZero(s, name, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (i >= 0)
     */
    public static int geqZero(int i, String name, int numCallsBelowRequirer) {
        if (!(i >= 0)) {
            fail(name + " >= 0", ExceptionMessageUtil.valueAndName(i, name), numCallsBelowRequirer + 1);
        }
        return i;
    }

    public static int geqZero(int i, String name) {
        return geqZero(i, name, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (l >= 0)
     */
    public static long geqZero(long l, String name, int numCallsBelowRequirer) {
        if (!(l >= 0)) {
            fail(name + " >= 0", ExceptionMessageUtil.valueAndName(l, name), numCallsBelowRequirer + 1);
        }
        return l;
    }

    public static long geqZero(long l, String name) {
        return geqZero(l, name, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (f >= 0)
     */
    public static float geqZero(float f, String name, int numCallsBelowRequirer) {
        if (!(f >= 0)) {
            fail(name + " >= 0", ExceptionMessageUtil.valueAndName(f, name), numCallsBelowRequirer + 1);
        }
        return f;
    }

    public static float geqZero(float f, String name) {
        return geqZero(f, name, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (d >= 0)
     */
    public static double geqZero(double d, String name, int numCallsBelowRequirer) {
        if (!(d >= 0)) {
            fail(name + " >= 0", ExceptionMessageUtil.valueAndName(d, name), numCallsBelowRequirer + 1);
        }
        return d;
    }

    public static double geqZero(double d, String name) {
        return geqZero(d, name, 1);
    }

    // ################################################################
    // eq, neq (Object ==/!= Object)

    // ----------------------------------------------------------------

    /**
     * require (o0 == o1)
     */
    public static void eq(Object o0, String name0, Object o1, String name1, int numCallsBelowRequirer) {
        if (!(o0 == o1)) {
            fail(name0 + " == " + name1, ExceptionMessageUtil.valueAndName(o0, name0, o1, name1),
                    numCallsBelowRequirer + 1);
        }
    }

    public static void eq(Object o0, String name0, Object o1, String name1) {
        eq(o0, name0, o1, name1, 1);
    }

    public static void eq(Object o0, String name0, Object o1, int numCallsBelowRequirer) {
        if (!(o0 == o1)) {
            fail(name0 + " == " + ExceptionMessageUtil.valueString(o1), ExceptionMessageUtil.valueAndName(o0, name0),
                    numCallsBelowRequirer + 1);
        }
    }

    public static void eq(Object o0, String name0, Object o1) {
        eq(o0, name0, o1, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (o0 != o1)
     */
    public static void neq(Object o0, String name0, Object o1, String name1, int numCallsBelowRequirer) {
        if (!(o0 != o1)) {
            fail(name0 + " != " + name1, ExceptionMessageUtil.valueAndName(o0, name0, o1, name1),
                    numCallsBelowRequirer + 1);
        }
    }

    public static void neq(Object o0, String name0, Object o1, String name1) {
        neq(o0, name0, o1, name1, 1);
    }

    public static void neq(Object o0, String name0, Object o1, int numCallsBelowRequirer) {
        if (!(o0 != o1)) {
            fail(name0 + " != " + ExceptionMessageUtil.valueString(o1), ExceptionMessageUtil.valueAndName(o0, name0),
                    numCallsBelowRequirer + 1);
        }
    }

    public static void neq(Object o0, String name0, Object o1) {
        neq(o0, name0, o1, 1);
    }


    // ################################################################
    // eqNull, neqNull (Object ==/!= null)

    // ----------------------------------------------------------------
    /**
     * require (o == null)
     */
    public static void eqNull(Object o, String name, int numCallsBelowRequirer) {
        if (!(null == o)) {
            fail(name + " == null", ExceptionMessageUtil.valueAndName(o, name), numCallsBelowRequirer + 1);
        }
    }

    public static void eqNull(Object o, String name) {
        eqNull(o, name, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (o != null)
     */
    @NotNull
    public static <T> T neqNull(T o, String name, int numCallsBelowRequirer) {
        if (!(null != o)) {
            fail(name + " != null", ExceptionMessageUtil.valueAndName(o, name), numCallsBelowRequirer + 1);
        }
        return o;
    }

    @NotNull
    public static <T> T neqNull(T o, String name) {
        return neqNull(o, name, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (o != NaN)
     */
    public static double neqNaN(double o, String name, int numCallsBelowRequirer) {
        if (Double.isNaN(o)) {
            fail(name + " != NaN", ExceptionMessageUtil.valueAndName(o, name), numCallsBelowRequirer + 1);
        }
        return o;
    }

    public static double neqNaN(double o, String name) {
        return neqNaN(o, name, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (o != +/-Inf)
     */
    public static double neqInf(double o, String name, int numCallsBelowRequirer) {
        if (Double.isInfinite(o)) {
            fail(name + " != +/-Inf", ExceptionMessageUtil.valueAndName(o, name), numCallsBelowRequirer + 1);
        }
        return o;
    }

    public static double neqInf(double o, String name) {
        return neqInf(o, name, 1);
    }

    // ################################################################
    // equals (Object.equals(Object))

    // ----------------------------------------------------------------
    /**
     * require (o0 != null && o1 != null && o0.equals(o1))
     */
    public static void equals(Object o0, String name0, Object o1, String name1, int numCallsBelowRequirer) {
        neqNull(o0, name0, numCallsBelowRequirer + 1);
        neqNull(o1, name1, numCallsBelowRequirer + 1);
        if (!(o0.equals(o1))) {
            fail(name0 + ".equals(" + name1 + ")", ExceptionMessageUtil.valueAndName(o0, name0, o1, name1),
                    numCallsBelowRequirer + 1);
        }
    }

    /**
     * require (o0 != null && o1 != null && o0.equals(o1))
     */
    public static void equals(Object o0, String name0, Object o1, String name1) {
        equals(o0, name0, o1, name1, 1);
    }

    /**
     * require (o0 != null && o1 != null && o0.equals(o1))
     */
    public static void equals(Object o0, String name0, Object o1, int numCallsBelowRequirer) {
        neqNull(o0, name0, numCallsBelowRequirer + 1);
        neqNull(o1, "o1", numCallsBelowRequirer + 1);
        if (!(o0.equals(o1))) {
            fail(name0 + ".equals(" + ExceptionMessageUtil.valueString(o1) + ")",
                    ExceptionMessageUtil.valueAndName(o0, name0), numCallsBelowRequirer + 1);
        }
    }

    /**
     * require (o0 != null && o1 != null && o0.equals(o1))
     */
    public static void equals(Object o0, String name0, Object o1) {
        equals(o0, name0, o1, 1);
    }

    // ----------------------------------------------------------------
    /**
     * require (o0 != null && o1 != null && !o0.equals(o1))
     */
    public static void notEquals(Object o0, String name0, Object o1, String name1, int numCallsBelowRequirer) {
        neqNull(o0, name0, numCallsBelowRequirer + 1);
        neqNull(o1, name1, numCallsBelowRequirer + 1);
        if (o0.equals(o1)) {
            fail("!" + name0 + ".equals(" + name1 + ")", ExceptionMessageUtil.valueAndName(o0, name0, o1, name1),
                    numCallsBelowRequirer + 1);
        }
    }

    /**
     * require (o0 != null && o1 != null && !o0.equals(o1))
     */
    public static void notEquals(Object o0, String name0, Object o1, String name1) {
        notEquals(o0, name0, o1, name1, 1);
    }

    /**
     * require (o0 != null && o1 != null && !o0.equals(o1))
     */
    public static void notEquals(Object o0, String name0, Object o1, int numCallsBelowRequirer) {
        neqNull(o0, name0, numCallsBelowRequirer + 1);
        neqNull(o1, "o1", numCallsBelowRequirer + 1);
        if (o0.equals(o1)) {
            fail("!" + name0 + ".equals(" + ExceptionMessageUtil.valueString(o1) + ")",
                    ExceptionMessageUtil.valueAndName(o0, name0), numCallsBelowRequirer + 1);
        }
    }

    /**
     * require (o0 != null && o1 != null && !o0.equals(o1))
     */
    public static void notEquals(Object o0, String name0, Object o1) {
        notEquals(o0, name0, o1, 1);
    }

    // ################################################################
    // nonempty (String.equals(nonempty))

    // ----------------------------------------------------------------
    /**
     * require (s != null && s.length() > 0)
     */
    public static String nonempty(String s, String name, int numCallsBelowRequirer) {
        neqNull(s, name, numCallsBelowRequirer + 1);
        if (!(s.length() > 0)) {
            fail(name + ".length() > 0", ExceptionMessageUtil.valueAndName(s, name), numCallsBelowRequirer + 1);
        }
        return s;
    }

    public static String nonempty(String s, String name) {
        return nonempty(s, name, 1);
    }

    // ################################################################

    // ----------------------------------------------------------------
    /** require (collection != null && collection.contains(element)) */
    public static <C extends Collection<T>, T> C contains(C collection, String collectionName, T element,
            String elementName, int numCallsBelowRequirer) {
        neqNull(collection, collectionName, numCallsBelowRequirer + 1);
        if (!(collection.contains(element))) {
            fail(collectionName + ".contains(" + elementName + ")",
                    ExceptionMessageUtil.valueAndName(element, elementName), numCallsBelowRequirer + 1);
        }
        return collection;
    }

    public static <C extends Collection<T>, T> C contains(C collection, String collectionName, T element,
            String elementName) {
        return contains(collection, collectionName, element, elementName, 1);
    }

    // ----------------------------------------------------------------
    /** require (collection != null && !collection.contains(element)) */
    public static <C extends Collection<T>, T> C notContains(C collection, String collectionName, T element,
            String elementName, int numCallsBelowRequirer) {
        neqNull(collection, collectionName, numCallsBelowRequirer + 1);
        if (collection.contains(element)) {
            fail("!" + collectionName + ".contains(" + elementName + ")",
                    ExceptionMessageUtil.valueAndName(element, elementName), numCallsBelowRequirer + 1);
        }
        return collection;
    }

    public static <C extends Collection<T>, T> C notContains(C collection, String collectionName, T element,
            String elementName) {
        return notContains(collection, collectionName, element, elementName, 1);
    }

    // ----------------------------------------------------------------
    /** require (collection != null && !collection.stream().anyMatch(Objects::isNull) */
    public static <C extends Collection<T>, T> C notContainsNull(C collection, String collectionName,
            int numCallsBelowRequirer) {
        neqNull(collection, collectionName, numCallsBelowRequirer + 1);
        if (collection.stream().anyMatch(Objects::isNull)) {
            fail(collectionName + " does not contain null", numCallsBelowRequirer + 1);
        }
        return collection;
    }

    public static <C extends Collection<T>, T> C notContainsNull(C collection, String collectionName) {
        return notContainsNull(collection, collectionName, 1);
    }

    // ----------------------------------------------------------------
    /** require (map != null && map.containsKey(key)) */
    public static <M extends Map<K, V>, K, V> M containsKey(M map, String mapName, K key, String keyName,
            int numCallsBelowRequirer) {
        neqNull(map, mapName, numCallsBelowRequirer + 1);
        if (!(map.containsKey(key))) {
            fail(mapName + ".containsKey(" + keyName + ")", ExceptionMessageUtil.valueAndName(key, keyName),
                    numCallsBelowRequirer + 1);
        }
        return map;
    }

    public static <M extends Map<K, V>, K, V> M containsKey(M map, String mapName, K key, String keyName) {
        return containsKey(map, mapName, key, keyName, 1);
    }

    // ----------------------------------------------------------------
    /** require (map != null && !map.containsKey(element)) */
    public static <M extends Map<K, V>, K, V> M notContainsKey(M map, String mapName, K key, String keyName,
            int numCallsBelowRequirer) {
        neqNull(map, mapName, numCallsBelowRequirer + 1);
        if (map.containsKey(key)) {
            fail("!" + mapName + ".containsKey(" + keyName + ")", ExceptionMessageUtil.valueAndName(key, keyName),
                    numCallsBelowRequirer + 1);
        }
        return map;
    }

    public static <M extends Map<K, V>, K, V> M notContainsKey(M map, String mapName, K key, String keyName) {
        return notContainsKey(map, mapName, key, keyName, 1);
    }

    // ----------------------------------------------------------------
    /** require (offset >= 0 && offset < length) */
    public static int inRange(int offset, String offsetName, int length, String lengthName, int numCallsBelowRequirer) {
        if (!(offset >= 0)) {
            fail(offsetName + " >= 0", ExceptionMessageUtil.valueAndName(offset, offsetName),
                    numCallsBelowRequirer + 1);
        } else if (!(offset < length)) {
            fail(offsetName + " < " + lengthName,
                    ExceptionMessageUtil.valueAndName(offset, offsetName, length, lengthName),
                    numCallsBelowRequirer + 1);
        }
        return offset;
    }

    /** require (offset >= 0 && offset < length) */
    public static int inRange(int offset, String offsetName, int length, String lengthName) {
        return inRange(offset, offsetName, length, lengthName, 1);
    }

    // ----------------------------------------------------------------
    /** require (offset >= start && offset < end) */
    public static int inRange(int offset, String offsetName, int start, String startName, int end, String endName,
            int numCallsBelowRequirer) {
        if (!(offset >= start)) {
            fail(offsetName + " >= " + startName,
                    ExceptionMessageUtil.valueAndName(offset, offsetName, start, startName), numCallsBelowRequirer + 1);
        } else if (!(offset < end)) {
            fail(offsetName + " < " + endName, ExceptionMessageUtil.valueAndName(offset, offsetName, end, endName),
                    numCallsBelowRequirer + 1);
        }
        return offset;
    }

    /** require (offset >= start && offset < end) */
    public static int inRange(int offset, String offsetName, int start, String startName, int end, String endName) {
        return inRange(offset, offsetName, start, startName, end, endName, 1);
    }

    // ----------------------------------------------------------------
    /** require (offset >= 0 && offset < length) */
    public static long inRange(long offset, String offsetName, long length, String lengthName,
            int numCallsBelowRequirer) {
        if (!(offset >= 0L)) {
            fail(offsetName + " >= 0L", ExceptionMessageUtil.valueAndName(offset, offsetName),
                    numCallsBelowRequirer + 1);
        } else if (!(offset < length)) {
            fail(offsetName + " < " + lengthName,
                    ExceptionMessageUtil.valueAndName(offset, offsetName, length, lengthName),
                    numCallsBelowRequirer + 1);
        }
        return offset;
    }

    /** require (offset >= 0 && offset < length) */
    public static long inRange(long offset, String offsetName, long length, String lengthName) {
        return inRange(offset, offsetName, length, lengthName, 1);
    }

    // ----------------------------------------------------------------
    /** require (offset >= start && offset < end) */
    public static long inRange(long offset, String offsetName, long start, String startName, long end, String endName,
            int numCallsBelowRequirer) {
        if (!(offset >= start)) {
            fail(offsetName + " >= " + startName,
                    ExceptionMessageUtil.valueAndName(offset, offsetName, start, startName), numCallsBelowRequirer + 1);
        } else if (!(offset < end)) {
            fail(offsetName + " < " + endName, ExceptionMessageUtil.valueAndName(offset, offsetName, end, endName),
                    numCallsBelowRequirer + 1);
        }
        return offset;
    }

    /** require (offset >= start && offset < end) */
    public static long inRange(long offset, String offsetName, long start, String startName, long end, String endName) {
        return inRange(offset, offsetName, start, startName, end, endName, 1);
    }

    // ################################################################

    /** require d != {Infinity, -Infinity, NaN}. */
    public static double normalized(double d, String name, int numCallsBelowRequirer) {
        if (!(!Double.isNaN(d) && !Double.isInfinite(d))) {
            fail(name + " is normalized (not infinity or NaN)", ExceptionMessageUtil.valueAndName(d, name),
                    numCallsBelowRequirer + 1);
        }
        return d;
    }

    /** require d != {Infinity, -Infinity, NaN}. */
    public static double normalized(double d, String name) {
        return normalized(d, name, 1);
    }

    public static <T> T[] nonEmpty(final T[] a, final String name, final int numCallsBelowRequirer) {
        neqNull(a, name, numCallsBelowRequirer + 1);
        if (!(a.length > 0)) {
            fail(name + ".length > 0", ExceptionMessageUtil.valueAndName(a, name), numCallsBelowRequirer + 1);
        }
        return a;
    }

    public static <T> T[] nonEmpty(final T[] a, final String name) {
        return nonEmpty(a, name, 1);
    }

    public static int[] lengthEqual(final int[] a, final String name, final int length,
            final int numCallsBelowRequirer) {
        if (!(a.length == length)) {
            fail(name + ".length == " + length, ExceptionMessageUtil.valueAndName(a, name), numCallsBelowRequirer + 1);
        }
        return a;
    }

    public static int[] lengthEqual(final int[] a, final String name, final int length) {
        return lengthEqual(a, name, length, 1);
    }

    public static <T> T[] elementsNeqNull(final T[] elements, final String name) {
        neqNull(elements, name);
        for (int i = 0; i < elements.length; i++) {
            if (elements[i] == null) {
                neqNull(elements[i], name + "[" + i + "]");
            }
        }
        return elements;
    }

    public static void elementsNeqNaN(final double[] elements, String name) {
        neqNull(elements, name);
        for (int i = 0; i < elements.length; i++) {
            if (Double.isNaN(elements[i])) {
                neqNaN(elements[i], name + "[" + i + "]");
            }
        }
    }

    public static void elementsNeqNaN(final double[][] elements, String name) {
        neqNull(elements, name);
        for (int i = 0; i < elements.length; i++) {
            for (int j = 0; j < elements[i].length; j++) {
                if (Double.isNaN(elements[i][j])) {
                    neqNaN(elements[i][j], name + "[" + i + "," + j + "]");
                }
            }
        }
    }

    public static void elementsNeqInf(final double[] elements, String name) {
        neqNull(elements, name);
        for (int i = 0; i < elements.length; i++) {
            if (Double.isInfinite(elements[i])) {
                neqInf(elements[i], name + "[" + i + "]");
            }
        }
    }

    public static void elementsNeqInf(final double[][] elements, String name) {
        neqNull(elements, name);
        for (int i = 0; i < elements.length; i++) {
            for (int j = 0; j < elements[i].length; j++) {
                if (Double.isInfinite(elements[i][j])) {
                    neqInf(elements[i][j], name + "[" + i + "," + j + "]");
                }
            }
        }
    }

    public static void isSquare(double[][] m, String name) {
        for (int i = 0; i < m.length; i++) {
            if (m[i].length != m.length) {
                fail("Matrix is not square: " + name, "matrix is not square: " + name, 1);
            }
        }
    }

    // ----------------------------------------------------------------
    public static double inRange(double trialValue, double endPointA, double endPointB, String name) {
        double minRange = endPointA;
        double maxRange = endPointB;
        if (endPointA > endPointB) {
            minRange = endPointB;
            maxRange = endPointA;
        }
        if (trialValue < minRange || maxRange < trialValue) {
            fail(name + " = " + trialValue + " is expected to be in the range of [" + minRange + "," + maxRange
                    + "] but was not", 1);
        }
        return trialValue;
    }

    // ----------------------------------------------------------------
    public static float inRange(float trialValue, float endPointA, float endPointB, String name) {
        float minRange = endPointA;
        float maxRange = endPointB;
        if (endPointA > endPointB) {
            minRange = endPointB;
            maxRange = endPointA;
        }
        if (trialValue < minRange || maxRange < trialValue) {
            fail(name + " = " + trialValue + " is expected to be in the range of [" + minRange + "," + maxRange
                    + "] but was not", 1);
        }
        return trialValue;
    }
}
