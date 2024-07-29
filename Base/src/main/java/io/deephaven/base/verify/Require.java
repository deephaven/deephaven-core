//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base.verify;

import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

// --------------------------------------------------------------------
/**
 * Requirement methods for simple runtime program verification. Failed requirements throw {@link RequirementFailure}.
 * <p>
 * Methods:
 * <ul>
 * <li>void requirement(boolean condition, String conditionText[, String detailMessage])
 * <li>void requirement(boolean condition, String conditionText, value0, String name0, value1, String name1, ... )
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
 * <li>eq, neq, lt, leq, gt, get correspond to ==, !=, &lt;, &lt;=, &gt;, &gt;=, e.g.,
 * <ul>
 * <li>For Object a and b, Require.eq(a, "a", b, "b") corresponds to require (a == b)
 * <li>For Object o, Require.neqNull(o, "o") corresponds to require (o != null)
 * <li>for int x, Require.eqZero(x, "x") corresponds to require (x == 0)
 * </ul>
 * <li>equals corresponds to Object.equals (preceded by necessary null checks), e.g.,
 * <ul>
 * <li>For Object a and b, Require.equals(a, "a", b, "b") corresponds to require (a!= null &amp;&amp; b != null
 * &amp;&amp; a.equals(b))
 * <li>for String s, Require.nonempty(s, "s") corresponds to require (s != null &amp;&amp; s.length() != 0)
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
    private static void fail(String conditionText) {
        final RequirementFailure requirementFailure = new RequirementFailure(
                ExceptionMessageUtil.failureMessage("Requirement", "required", conditionText, null));
        if (onFailureCallback != null) {
            try {
                onFailureCallback.accept(requirementFailure);
            } catch (Exception ignored) {
            }
        }
        throw requirementFailure;
    }

    // ----------------------------------------------------------------
    private static void fail(String conditionText, String detailMessage) {
        final RequirementFailure requirementFailure = new RequirementFailure(
                ExceptionMessageUtil.failureMessage("Requirement", "required", conditionText, detailMessage));
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

    public static void requirement(boolean condition, String conditionText) {
        if (!(condition)) {
            fail(conditionText);
        }
    }

    // ----------------------------------------------------------------

    public static void requirement(boolean condition, String conditionText, String detailMessage) {
        if (!(condition)) {
            fail(conditionText, detailMessage);
        }
    }

    // ----------------------------------------------------------------

    public static void requirement(boolean condition, String conditionText, Object o0, String name0) {
        if (!(condition)) {
            fail(conditionText, ExceptionMessageUtil.valueAndName(o0, name0));
        }
    }

    public static void requirement(boolean condition, String conditionText, Object o0, String name0, Object o1,
            String name1) {
        if (!(condition)) {
            fail(conditionText, ExceptionMessageUtil.valueAndName(o0, name0, o1, name1));
        }
    }

    public static void requirement(boolean condition, String conditionText, Object o0, String name0, Object o1,
            String name1, Object o2, String name2) {
        if (!(condition)) {
            fail(conditionText, ExceptionMessageUtil.valueAndName(o0, name0, o1, name1, o2, name2));
        }
    }

    public static void requirement(boolean condition, String conditionText, long o0, String name0, long o1,
            String name1, long o2, String name2) {
        if (!(condition)) {
            fail(conditionText, ExceptionMessageUtil.valueAndName(o0, name0, o1, name1, o2, name2));
        }
    }

    public static void requirement(boolean condition, String conditionText, Object o0, String name0, Object o1,
            String name1, Object o2, String name2, Object o3, String name3) {
        if (!(condition)) {
            fail(conditionText, ExceptionMessageUtil.valueAndName(o0, name0, o1, name1, o2, name2, o3, name3));
        }
    }

    // ----------------------------------------------------------------

    public static void requirement(boolean condition, String conditionText, boolean b0, String name0) {
        if (!(condition)) {
            fail(conditionText, ExceptionMessageUtil.valueAndName(b0, name0));
        }
    }

    public static void requirement(boolean condition, String conditionText, boolean b0, String name0, boolean b1,
            String name1) {
        if (!(condition)) {
            fail(conditionText, ExceptionMessageUtil.valueAndName(b0, name0, b1, name1));
        }
    }

    public static void requirement(boolean condition, String conditionText, boolean b0, String name0, double d1,
            String name1) {
        if (!(condition)) {
            fail(conditionText, ExceptionMessageUtil.concat(ExceptionMessageUtil.valueAndName(b0, name0),
                    ExceptionMessageUtil.valueAndName(d1, name1)));
        }
    }

    public static void requirement(boolean condition, String conditionText, boolean b0, String name0, boolean b1,
            String name1, boolean b2, String name2) {
        if (!(condition)) {
            fail(conditionText, ExceptionMessageUtil.valueAndName(b0, name0, b1, name1, b2, name2));
        }
    }

    public static void requirement(boolean condition, String conditionText, boolean b0, String name0, boolean b1,
            String name1, boolean b2, String name2, boolean b3, String name3) {
        if (!(condition)) {
            fail(conditionText, ExceptionMessageUtil.valueAndName(b0, name0, b1, name1, b2, name2, b3, name3));
        }
    }

    // ----------------------------------------------------------------

    public static void requirement(boolean condition, String conditionText, int i0, String name0) {
        if (!(condition)) {
            fail(conditionText, ExceptionMessageUtil.valueAndName(i0, name0));
        }
    }

    public static void requirement(boolean condition, String conditionText, int i0, String name0, int i1,
            String name1) {
        if (!(condition)) {
            fail(conditionText, ExceptionMessageUtil.valueAndName(i0, name0, i1, name1));
        }
    }

    // ----------------------------------------------------------------

    public static void requirement(boolean condition, String conditionText, long l0, String name0) {
        if (!(condition)) {
            fail(conditionText, ExceptionMessageUtil.valueAndName(l0, name0));
        }
    }

    // ################################################################
    // statementNeverExecuted

    // ----------------------------------------------------------------

    public static RequirementFailure statementNeverExecuted() {
        fail("statement is never executed");
        return null;
    }

    // ----------------------------------------------------------------

    public static RequirementFailure statementNeverExecuted(String statementDescription) {
        fail(statementDescription + " is never executed");
        return null;
    }

    // ################################################################
    // exceptionNeverCaught

    // ----------------------------------------------------------------

    public static RequirementFailure exceptionNeverCaught(Exception e) {
        try {
            fail(e.getClass().getName() + " is never caught",
                    e.getClass().getName() + "(" + e.getMessage() + ") caught");
        } catch (RequirementFailure requirementFailure) {
            requirementFailure.initCause(e);
            throw requirementFailure;
        }
        return null;
    }

    // ----------------------------------------------------------------

    public static RequirementFailure exceptionNeverCaught(String tryStatementDescription, Exception e) {
        try {
            fail(tryStatementDescription + " succeeds", e.getClass().getName() + "(" + e.getMessage() + ") caught");
        } catch (RequirementFailure requirementFailure) {
            requirementFailure.initCause(e);
            throw requirementFailure;
        }
        return null;
    }

    // ################################################################
    // valueNeverOccurs

    // ----------------------------------------------------------------

    public static RequirementFailure valueNeverOccurs(Object o, String name) {
        fail(ExceptionMessageUtil.valueAndName(o, name) + " never occurs");
        return null;
    }

    // ----------------------------------------------------------------

    public static RequirementFailure valueNeverOccurs(boolean b, String name) {
        fail(ExceptionMessageUtil.valueAndName(b, name) + " never occurs");
        return null;
    }

    // ----------------------------------------------------------------

    public static RequirementFailure valueNeverOccurs(char c, String name) {
        fail(ExceptionMessageUtil.valueAndName(c, name) + " never occurs");
        return null;
    }

    // ----------------------------------------------------------------

    public static RequirementFailure valueNeverOccurs(byte b, String name) {
        fail(ExceptionMessageUtil.valueAndName(b, name) + " never occurs");
        return null;
    }

    // ----------------------------------------------------------------

    public static RequirementFailure valueNeverOccurs(short s, String name) {
        fail(ExceptionMessageUtil.valueAndName(s, name) + " never occurs");
        return null;
    }

    // ----------------------------------------------------------------

    public static RequirementFailure valueNeverOccurs(int i, String name) {
        fail(ExceptionMessageUtil.valueAndName(i, name) + " never occurs");
        return null;
    }

    // ----------------------------------------------------------------

    public static RequirementFailure valueNeverOccurs(long l, String name) {
        fail(ExceptionMessageUtil.valueAndName(l, name) + " never occurs");
        return null;
    }

    // ----------------------------------------------------------------

    public static RequirementFailure valueNeverOccurs(float f, String name) {
        fail(ExceptionMessageUtil.valueAndName(f, name) + " never occurs");
        return null;
    }

    // ----------------------------------------------------------------

    public static RequirementFailure valueNeverOccurs(double d, String name) {
        fail(ExceptionMessageUtil.valueAndName(d, name) + " never occurs");
        return null;
    }

    // ################################################################
    // eq (primitiveValue == primitiveValue)

    // ----------------------------------------------------------------

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
    /**
     * require (f0 == f1)
     */
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

    public static int neq(int i0, String name0, int i1, String name1) {
        if (!(i0 != i1)) {
            fail(name0 + " != " + name1, ExceptionMessageUtil.valueAndName(i0, name0, i1, name1));
        }
        return i0;
    }

    public static void neq(int i0, String name0, int i1) {
        if (!(i0 != i1)) {
            fail(name0 + " != " + i1, ExceptionMessageUtil.valueAndName(i0, name0));
        }
    }

    // ----------------------------------------------------------------

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

    public static char lt(char c0, String name0, char c1, String name1) {
        if (!(c0 < c1)) {
            fail(name0 + " < " + name1, ExceptionMessageUtil.valueAndName(c0, name0, c1, name1));
        }
        return c0;
    }

    public static char lt(char c0, String name0, char c1) {
        if (!(c0 < c1)) {
            fail(name0 + " < " + ExceptionMessageUtil.valueString(c1), ExceptionMessageUtil.valueAndName(c0, name0));
        }
        return c0;
    }

    // ----------------------------------------------------------------

    public static byte lt(byte b0, String name0, byte b1, String name1) {
        if (!(b0 < b1)) {
            fail(name0 + " < " + name1, ExceptionMessageUtil.valueAndName(b0, name0, b1, name1));
        }
        return b0;
    }

    public static byte lt(byte b0, String name0, byte b1) {
        if (!(b0 < b1)) {
            fail(name0 + " < " + b1, ExceptionMessageUtil.valueAndName(b0, name0));
        }
        return b0;
    }

    // ----------------------------------------------------------------

    public static short lt(short s0, String name0, short s1, String name1) {
        if (!(s0 < s1)) {
            fail(name0 + " < " + name1, ExceptionMessageUtil.valueAndName(s0, name0, s1, name1));
        }
        return s0;
    }

    public static short lt(short s0, String name0, short s1) {
        if (!(s0 < s1)) {
            fail(name0 + " < " + s1, ExceptionMessageUtil.valueAndName(s0, name0));
        }
        return s0;
    }

    // ----------------------------------------------------------------

    public static int lt(int i0, String name0, int i1, String name1) {
        if (!(i0 < i1)) {
            fail(name0 + " < " + name1, ExceptionMessageUtil.valueAndName(i0, name0, i1, name1));
        }
        return i0;
    }

    public static int lt(int i0, String name0, int i1) {
        if (!(i0 < i1)) {
            fail(name0 + " < " + i1, ExceptionMessageUtil.valueAndName(i0, name0));
        }
        return i0;
    }

    // ----------------------------------------------------------------

    public static long lt(long l0, String name0, long l1, String name1) {
        if (!(l0 < l1)) {
            fail(name0 + " < " + name1, ExceptionMessageUtil.valueAndName(l0, name0, l1, name1));
        }
        return l0;
    }

    public static long lt(long l0, String name0, long l1) {
        if (!(l0 < l1)) {
            fail(name0 + " < " + l1, ExceptionMessageUtil.valueAndName(l0, name0));
        }
        return l0;
    }

    // ----------------------------------------------------------------

    public static float lt(float f0, String name0, float f1, String name1) {
        if (!(f0 < f1)) {
            fail(name0 + " < " + name1, ExceptionMessageUtil.valueAndName(f0, name0, f1, name1));
        }
        return f0;
    }

    public static float lt(float f0, String name0, float f1) {
        if (!(f0 < f1)) {
            fail(name0 + " < " + f1, ExceptionMessageUtil.valueAndName(f0, name0));
        }
        return f0;
    }

    // ----------------------------------------------------------------

    public static double lt(double d0, String name0, double d1, String name1) {
        if (!(d0 < d1)) {
            fail(name0 + " < " + name1, ExceptionMessageUtil.valueAndName(d0, name0, d1, name1));
        }
        return d0;
    }

    public static double lt(double d0, String name0, double d1) {
        if (!(d0 < d1)) {
            fail(name0 + " < " + d1, ExceptionMessageUtil.valueAndName(d0, name0));
        }
        return d0;
    }

    // ################################################################
    // leq (primitiveValue <= primitiveValue)

    // ----------------------------------------------------------------

    public static char leq(char c0, String name0, char c1, String name1) {
        if (!(c0 <= c1)) {
            fail(name0 + " <= " + name1, ExceptionMessageUtil.valueAndName(c0, name0, c1, name1));
        }
        return c0;
    }

    public static char leq(char c0, String name0, char c1) {
        if (!(c0 <= c1)) {
            fail(name0 + " <= " + ExceptionMessageUtil.valueString(c1), ExceptionMessageUtil.valueAndName(c0, name0));
        }
        return c0;
    }

    // ----------------------------------------------------------------

    public static byte leq(byte b0, String name0, byte b1, String name1) {
        if (!(b0 <= b1)) {
            fail(name0 + " <= " + name1, ExceptionMessageUtil.valueAndName(b0, name0, b1, name1));
        }
        return b0;
    }

    public static byte leq(byte b0, String name0, byte b1) {
        if (!(b0 <= b1)) {
            fail(name0 + " <= " + b1, ExceptionMessageUtil.valueAndName(b0, name0));
        }
        return b0;
    }

    // ----------------------------------------------------------------

    public static short leq(short s0, String name0, short s1, String name1) {
        if (!(s0 <= s1)) {
            fail(name0 + " <= " + name1, ExceptionMessageUtil.valueAndName(s0, name0, s1, name1));
        }
        return s0;
    }

    public static short leq(short s0, String name0, short s1) {
        if (!(s0 <= s1)) {
            fail(name0 + " <= " + s1, ExceptionMessageUtil.valueAndName(s0, name0));
        }
        return s0;
    }

    // ----------------------------------------------------------------

    public static int leq(int i0, String name0, int i1, String name1) {
        if (!(i0 <= i1)) {
            fail(name0 + " <= " + name1, ExceptionMessageUtil.valueAndName(i0, name0, i1, name1));
        }
        return i0;
    }

    public static int leq(int i0, String name0, int i1) {
        if (!(i0 <= i1)) {
            fail(name0 + " <= " + i1, ExceptionMessageUtil.valueAndName(i0, name0));
        }
        return i0;
    }

    // ----------------------------------------------------------------

    public static long leq(long l0, String name0, long l1, String name1) {
        if (!(l0 <= l1)) {
            fail(name0 + " <= " + name1, ExceptionMessageUtil.valueAndName(l0, name0, l1, name1));
        }
        return l0;
    }

    public static long leq(long l0, String name0, long l1) {
        if (!(l0 <= l1)) {
            fail(name0 + " <= " + l1, ExceptionMessageUtil.valueAndName(l0, name0));
        }
        return l0;
    }

    // ----------------------------------------------------------------

    public static float leq(float f0, String name0, float f1, String name1) {
        if (!(f0 <= f1)) {
            fail(name0 + " <= " + name1, ExceptionMessageUtil.valueAndName(f0, name0, f1, name1));
        }
        return f0;
    }

    public static float leq(float f0, String name0, float f1) {
        if (!(f0 <= f1)) {
            fail(name0 + " <= " + f1, ExceptionMessageUtil.valueAndName(f0, name0));
        }
        return f0;
    }

    // ----------------------------------------------------------------

    public static double leq(double d0, String name0, double d1, String name1) {
        if (!(d0 <= d1)) {
            fail(name0 + " <= " + name1, ExceptionMessageUtil.valueAndName(d0, name0, d1, name1));
        }
        return d0;
    }

    public static double leq(double d0, String name0, double d1) {
        if (!(d0 <= d1)) {
            fail(name0 + " <= " + d1, ExceptionMessageUtil.valueAndName(d0, name0));
        }
        return d0;
    }

    // ################################################################
    // gt (primitiveValue > primitiveValue)

    // ----------------------------------------------------------------

    public static char gt(char c0, String name0, char c1, String name1) {
        if (!(c0 > c1)) {
            fail(name0 + " > " + name1, ExceptionMessageUtil.valueAndName(c0, name0, c1, name1));
        }
        return c0;
    }

    public static char gt(char c0, String name0, char c1) {
        if (!(c0 > c1)) {
            fail(name0 + " > " + ExceptionMessageUtil.valueString(c1), ExceptionMessageUtil.valueAndName(c0, name0));
        }
        return c0;
    }

    // ----------------------------------------------------------------

    public static byte gt(byte b0, String name0, byte b1, String name1) {
        if (!(b0 > b1)) {
            fail(name0 + " > " + name1, ExceptionMessageUtil.valueAndName(b0, name0, b1, name1));
        }
        return b0;
    }

    public static byte gt(byte b0, String name0, byte b1) {
        if (!(b0 > b1)) {
            fail(name0 + " > " + b1, ExceptionMessageUtil.valueAndName(b0, name0));
        }
        return b0;
    }

    // ----------------------------------------------------------------

    public static short gt(short s0, String name0, short s1, String name1) {
        if (!(s0 > s1)) {
            fail(name0 + " > " + name1, ExceptionMessageUtil.valueAndName(s0, name0, s1, name1));
        }
        return s0;
    }

    public static short gt(short s0, String name0, short s1) {
        if (!(s0 > s1)) {
            fail(name0 + " > " + s1, ExceptionMessageUtil.valueAndName(s0, name0));
        }
        return s0;
    }

    // ----------------------------------------------------------------

    public static int gt(int i0, String name0, int i1, String name1) {
        if (!(i0 > i1)) {
            fail(name0 + " > " + name1, ExceptionMessageUtil.valueAndName(i0, name0, i1, name1));
        }
        return i0;
    }

    public static int gt(int i0, String name0, int i1) {
        if (!(i0 > i1)) {
            fail(name0 + " > " + i1, ExceptionMessageUtil.valueAndName(i0, name0));
        }
        return i0;
    }

    // ----------------------------------------------------------------

    public static long gt(long l0, String name0, long l1, String name1) {
        if (!(l0 > l1)) {
            fail(name0 + " > " + name1, ExceptionMessageUtil.valueAndName(l0, name0, l1, name1));
        }
        return l0;
    }

    public static long gt(long l0, String name0, long l1) {
        if (!(l0 > l1)) {
            fail(name0 + " > " + l1, ExceptionMessageUtil.valueAndName(l0, name0));
        }
        return l0;
    }

    // ----------------------------------------------------------------

    public static float gt(float f0, String name0, float f1, String name1) {
        if (!(f0 > f1)) {
            fail(name0 + " > " + name1, ExceptionMessageUtil.valueAndName(f0, name0, f1, name1));
        }
        return f0;
    }

    public static float gt(float f0, String name0, float f1) {
        if (!(f0 > f1)) {
            fail(name0 + " > " + f1, ExceptionMessageUtil.valueAndName(f0, name0));
        }
        return f0;
    }

    // ----------------------------------------------------------------

    public static double gt(double d0, String name0, double d1, String name1) {
        if (!(d0 > d1)) {
            fail(name0 + " > " + name1, ExceptionMessageUtil.valueAndName(d0, name0, d1, name1));
        }
        return d0;
    }

    public static double gt(double d0, String name0, double d1) {
        if (!(d0 > d1)) {
            fail(name0 + " > " + d1, ExceptionMessageUtil.valueAndName(d0, name0));
        }
        return d0;
    }

    // ################################################################
    // geq (primitiveValue >= primitiveValue)

    // ----------------------------------------------------------------

    public static char geq(char c0, String name0, char c1, String name1) {
        if (!(c0 >= c1)) {
            fail(name0 + " >= " + name1, ExceptionMessageUtil.valueAndName(c0, name0, c1, name1));
        }
        return c0;
    }

    public static char geq(char c0, String name0, char c1) {
        if (!(c0 >= c1)) {
            fail(name0 + " >= " + ExceptionMessageUtil.valueString(c1), ExceptionMessageUtil.valueAndName(c0, name0));
        }
        return c0;
    }

    // ----------------------------------------------------------------

    public static byte geq(byte b0, String name0, byte b1, String name1) {
        if (!(b0 >= b1)) {
            fail(name0 + " >= " + name1, ExceptionMessageUtil.valueAndName(b0, name0, b1, name1));
        }
        return b0;
    }

    public static byte geq(byte b0, String name0, byte b1) {
        if (!(b0 >= b1)) {
            fail(name0 + " >= " + b1, ExceptionMessageUtil.valueAndName(b0, name0));
        }
        return b0;
    }

    // ----------------------------------------------------------------

    public static short geq(short s0, String name0, short s1, String name1) {
        if (!(s0 >= s1)) {
            fail(name0 + " >= " + name1, ExceptionMessageUtil.valueAndName(s0, name0, s1, name1));
        }
        return s0;
    }

    public static short geq(short s0, String name0, short s1) {
        if (!(s0 >= s1)) {
            fail(name0 + " >= " + s1, ExceptionMessageUtil.valueAndName(s0, name0));
        }
        return s0;
    }

    // ----------------------------------------------------------------

    public static int geq(int i0, String name0, int i1, String name1) {
        if (!(i0 >= i1)) {
            fail(name0 + " >= " + name1, ExceptionMessageUtil.valueAndName(i0, name0, i1, name1));
        }
        return i0;
    }

    public static int geq(int i0, String name0, int i1) {
        if (!(i0 >= i1)) {
            fail(name0 + " >= " + i1, ExceptionMessageUtil.valueAndName(i0, name0));
        }
        return i0;
    }

    // ----------------------------------------------------------------

    public static long geq(long l0, String name0, long l1, String name1) {
        if (!(l0 >= l1)) {
            fail(name0 + " >= " + name1, ExceptionMessageUtil.valueAndName(l0, name0, l1, name1));
        }
        return l0;
    }

    public static long geq(long l0, String name0, long l1) {
        if (!(l0 >= l1)) {
            fail(name0 + " >= " + l1, ExceptionMessageUtil.valueAndName(l0, name0));
        }
        return l0;
    }

    // ----------------------------------------------------------------

    public static float geq(float f0, String name0, float f1, String name1) {
        if (!(f0 >= f1)) {
            fail(name0 + " >= " + name1, ExceptionMessageUtil.valueAndName(f0, name0, f1, name1));
        }
        return f0;
    }

    public static float geq(float f0, String name0, float f1) {
        if (!(f0 >= f1)) {
            fail(name0 + " >= " + f1, ExceptionMessageUtil.valueAndName(f0, name0));
        }
        return f0;
    }

    // ----------------------------------------------------------------

    public static double geq(double d0, String name0, double d1, String name1) {
        if (!(d0 >= d1)) {
            fail(name0 + " >= " + name1, ExceptionMessageUtil.valueAndName(d0, name0, d1, name1));
        }
        return d0;
    }

    public static double geq(double d0, String name0, double d1) {
        if (!(d0 >= d1)) {
            fail(name0 + " >= " + d1, ExceptionMessageUtil.valueAndName(d0, name0));
        }
        return d0;
    }

    // ################################################################
    // eqFalse, neqFalse, eqTrue, neqTrue (boolean ==/!= false/true)

    // ----------------------------------------------------------------

    public static void eqFalse(boolean b, String name) {
        if (!(false == b)) {
            fail(name + " == false", ExceptionMessageUtil.valueAndName(b, name));
        }
    }

    // ----------------------------------------------------------------

    public static void neqFalse(boolean b, String name) {
        if (!(false != b)) {
            fail(name + " != false", ExceptionMessageUtil.valueAndName(b, name));
        }
    }

    // ----------------------------------------------------------------

    public static void eqTrue(boolean b, String name) {
        if (!(true == b)) {
            fail(name + " == true", ExceptionMessageUtil.valueAndName(b, name));
        }
    }

    // ----------------------------------------------------------------

    public static void neqTrue(boolean b, String name) {
        if (!(true != b)) {
            fail(name + " != true", ExceptionMessageUtil.valueAndName(b, name));
        }
    }

    // ################################################################
    // eqZero (primitiveValue == 0)

    // ----------------------------------------------------------------

    public static void eqZero(char c, String name) {
        if (!(0 == c)) {
            fail(name + " == 0", ExceptionMessageUtil.valueAndName(c, name));
        }
    }

    // ----------------------------------------------------------------

    public static void eqZero(byte b, String name) {
        if (!(0 == b)) {
            fail(name + " == 0", ExceptionMessageUtil.valueAndName(b, name));
        }
    }

    // ----------------------------------------------------------------

    public static void eqZero(short s, String name) {
        if (!(0 == s)) {
            fail(name + " == 0", ExceptionMessageUtil.valueAndName(s, name));
        }
    }

    // ----------------------------------------------------------------

    public static void eqZero(int i, String name) {
        if (!(0 == i)) {
            fail(name + " == 0", ExceptionMessageUtil.valueAndName(i, name));
        }
    }

    // ----------------------------------------------------------------

    public static void eqZero(long l, String name) {
        if (!(0 == l)) {
            fail(name + " == 0", ExceptionMessageUtil.valueAndName(l, name));
        }
    }

    // ----------------------------------------------------------------

    public static void eqZero(float f, String name) {
        if (!(0 == f)) {
            fail(name + " == 0", ExceptionMessageUtil.valueAndName(f, name));
        }
    }

    // ----------------------------------------------------------------

    public static void eqZero(double d, String name) {
        if (!(0 == d)) {
            fail(name + " == 0", ExceptionMessageUtil.valueAndName(d, name));
        }
    }

    // ################################################################
    // neqZero (primitiveValue != 0)

    // ----------------------------------------------------------------

    public static char neqZero(char c, String name) {
        if (!(0 != c)) {
            fail(name + " != 0", ExceptionMessageUtil.valueAndName(c, name));
        }
        return c;
    }

    // ----------------------------------------------------------------

    public static byte neqZero(byte b, String name) {
        if (!(0 != b)) {
            fail(name + " != 0", ExceptionMessageUtil.valueAndName(b, name));
        }
        return b;
    }

    // ----------------------------------------------------------------

    public static short neqZero(short s, String name) {
        if (!(0 != s)) {
            fail(name + " != 0", ExceptionMessageUtil.valueAndName(s, name));
        }
        return s;
    }

    // ----------------------------------------------------------------

    public static int neqZero(int i, String name) {
        if (!(0 != i)) {
            fail(name + " != 0", ExceptionMessageUtil.valueAndName(i, name));
        }
        return i;
    }

    // ----------------------------------------------------------------

    public static long neqZero(long l, String name) {
        if (!(0 != l)) {
            fail(name + " != 0", ExceptionMessageUtil.valueAndName(l, name));
        }
        return l;
    }

    // ----------------------------------------------------------------

    public static float neqZero(float f, String name) {
        if (!(0 != f)) {
            fail(name + " != 0", ExceptionMessageUtil.valueAndName(f, name));
        }
        return f;
    }

    // ----------------------------------------------------------------

    public static double neqZero(double d, String name) {
        if (!(0 != d)) {
            fail(name + " != 0", ExceptionMessageUtil.valueAndName(d, name));
        }
        return d;
    }

    // ################################################################
    // ltZero (primitiveValue < 0)

    // ----------------------------------------------------------------

    public static byte ltZero(byte b, String name) {
        if (!(b < 0)) {
            fail(name + " < 0", ExceptionMessageUtil.valueAndName(b, name));
        }
        return b;
    }

    // ----------------------------------------------------------------

    public static short ltZero(short s, String name) {
        if (!(s < 0)) {
            fail(name + " < 0", ExceptionMessageUtil.valueAndName(s, name));
        }
        return s;
    }

    // ----------------------------------------------------------------

    public static int ltZero(int i, String name) {
        if (!(i < 0)) {
            fail(name + " < 0", ExceptionMessageUtil.valueAndName(i, name));
        }
        return i;
    }

    // ----------------------------------------------------------------

    public static long ltZero(long l, String name) {
        if (!(l < 0)) {
            fail(name + " < 0", ExceptionMessageUtil.valueAndName(l, name));
        }
        return l;
    }

    // ----------------------------------------------------------------

    public static float ltZero(float f, String name) {
        if (!(f < 0)) {
            fail(name + " < 0", ExceptionMessageUtil.valueAndName(f, name));
        }
        return f;
    }

    // ----------------------------------------------------------------

    public static double ltZero(double d, String name) {
        if (!(d < 0)) {
            fail(name + " < 0", ExceptionMessageUtil.valueAndName(d, name));
        }
        return d;
    }

    // ################################################################
    // leqZero (primitiveValue <= 0)

    // ----------------------------------------------------------------

    public static byte leqZero(byte b, String name) {
        if (!(b <= 0)) {
            fail(name + " <= 0", ExceptionMessageUtil.valueAndName(b, name));
        }
        return b;
    }

    // ----------------------------------------------------------------

    public static short leqZero(short s, String name) {
        if (!(s <= 0)) {
            fail(name + " <= 0", ExceptionMessageUtil.valueAndName(s, name));
        }
        return s;
    }

    // ----------------------------------------------------------------

    public static int leqZero(int i, String name) {
        if (!(i <= 0)) {
            fail(name + " <= 0", ExceptionMessageUtil.valueAndName(i, name));
        }
        return i;
    }

    // ----------------------------------------------------------------

    public static long leqZero(long l, String name) {
        if (!(l <= 0)) {
            fail(name + " <= 0", ExceptionMessageUtil.valueAndName(l, name));
        }
        return l;
    }

    // ----------------------------------------------------------------

    public static float leqZero(float f, String name) {
        if (!(f <= 0)) {
            fail(name + " <= 0", ExceptionMessageUtil.valueAndName(f, name));
        }
        return f;
    }

    // ----------------------------------------------------------------

    public static double leqZero(double d, String name) {
        if (!(d <= 0)) {
            fail(name + " <= 0", ExceptionMessageUtil.valueAndName(d, name));
        }
        return d;
    }

    // ################################################################
    // gtZero (primitiveValue > 0)

    // ----------------------------------------------------------------

    public static byte gtZero(byte b, String name) {
        if (!(b > 0)) {
            fail(name + " > 0", ExceptionMessageUtil.valueAndName(b, name));
        }
        return b;
    }

    // ----------------------------------------------------------------

    public static short gtZero(short s, String name) {
        if (!(s > 0)) {
            fail(name + " > 0", ExceptionMessageUtil.valueAndName(s, name));
        }
        return s;
    }

    // ----------------------------------------------------------------

    public static int gtZero(int i, String name) {
        if (!(i > 0)) {
            fail(name + " > 0", ExceptionMessageUtil.valueAndName(i, name));
        }
        return i;
    }

    // ----------------------------------------------------------------

    public static long gtZero(long l, String name) {
        if (!(l > 0)) {
            fail(name + " > 0", ExceptionMessageUtil.valueAndName(l, name));
        }
        return l;
    }

    // ----------------------------------------------------------------

    public static float gtZero(float f, String name) {
        if (!(f > 0)) {
            fail(name + " > 0", ExceptionMessageUtil.valueAndName(f, name));
        }
        return f;
    }

    // ----------------------------------------------------------------

    public static double gtZero(double d, String name) {
        if (!(d > 0)) {
            fail(name + " > 0", ExceptionMessageUtil.valueAndName(d, name));
        }
        return d;
    }

    // ################################################################
    // geqZero (primitiveValue >= 0)

    // ----------------------------------------------------------------

    public static byte geqZero(byte b, String name) {
        if (!(b >= 0)) {
            fail(name + " >= 0", ExceptionMessageUtil.valueAndName(b, name));
        }
        return b;
    }

    // ----------------------------------------------------------------

    public static short geqZero(short s, String name) {
        if (!(s >= 0)) {
            fail(name + " >= 0", ExceptionMessageUtil.valueAndName(s, name));
        }
        return s;
    }

    // ----------------------------------------------------------------

    public static int geqZero(int i, String name) {
        if (!(i >= 0)) {
            fail(name + " >= 0", ExceptionMessageUtil.valueAndName(i, name));
        }
        return i;
    }

    // ----------------------------------------------------------------

    public static long geqZero(long l, String name) {
        if (!(l >= 0)) {
            fail(name + " >= 0", ExceptionMessageUtil.valueAndName(l, name));
        }
        return l;
    }

    // ----------------------------------------------------------------

    public static float geqZero(float f, String name) {
        if (!(f >= 0)) {
            fail(name + " >= 0", ExceptionMessageUtil.valueAndName(f, name));
        }
        return f;
    }

    // ----------------------------------------------------------------

    public static double geqZero(double d, String name) {
        if (!(d >= 0)) {
            fail(name + " >= 0", ExceptionMessageUtil.valueAndName(d, name));
        }
        return d;
    }

    // ################################################################
    // eq, neq (Object ==/!= Object)

    // ----------------------------------------------------------------

    public static void eq(Object o0, String name0, Object o1, String name1) {
        if (!(o0 == o1)) {
            fail(name0 + " == " + name1, ExceptionMessageUtil.valueAndName(o0, name0, o1, name1));
        }
    }

    public static void eq(Object o0, String name0, Object o1) {
        if (!(o0 == o1)) {
            fail(name0 + " == " + ExceptionMessageUtil.valueString(o1), ExceptionMessageUtil.valueAndName(o0, name0));
        }
    }

    // ----------------------------------------------------------------

    public static void neq(Object o0, String name0, Object o1, String name1) {
        if (!(o0 != o1)) {
            fail(name0 + " != " + name1, ExceptionMessageUtil.valueAndName(o0, name0, o1, name1));
        }
    }

    public static void neq(Object o0, String name0, Object o1) {
        if (!(o0 != o1)) {
            fail(name0 + " != " + ExceptionMessageUtil.valueString(o1), ExceptionMessageUtil.valueAndName(o0, name0));
        }
    }


    // ################################################################
    // eqNull, neqNull (Object ==/!= null)

    // ----------------------------------------------------------------

    public static void eqNull(Object o, String name) {
        if (!(null == o)) {
            fail(name + " == null", ExceptionMessageUtil.valueAndName(o, name));
        }
    }

    // ----------------------------------------------------------------

    @NotNull
    public static <T> T neqNull(T o, String name) {
        if (!(null != o)) {
            fail(name + " != null", ExceptionMessageUtil.valueAndName(o, name));
        }
        return o;
    }

    // ----------------------------------------------------------------

    public static double neqNaN(double o, String name) {
        if (Double.isNaN(o)) {
            fail(name + " != NaN", ExceptionMessageUtil.valueAndName(o, name));
        }
        return o;
    }

    // ----------------------------------------------------------------

    public static double neqInf(double o, String name) {
        if (Double.isInfinite(o)) {
            fail(name + " != +/-Inf", ExceptionMessageUtil.valueAndName(o, name));
        }
        return o;
    }

    // ################################################################
    // equals (Object.equals(Object))

    // ----------------------------------------------------------------

    /**
     * require (o0 != null &amp;&amp; o1 != null &amp;&amp; o0.equals(o1))
     */
    public static void equals(Object o0, String name0, Object o1, String name1) {
        neqNull(o0, name0);
        neqNull(o1, name1);
        if (!(o0.equals(o1))) {
            fail(name0 + ".equals(" + name1 + ")", ExceptionMessageUtil.valueAndName(o0, name0, o1, name1));
        }
    }

    /**
     * require (o0 != null &amp;&amp; o1 != null &amp;&amp; o0.equals(o1))
     */
    public static void equals(Object o0, String name0, Object o1) {
        neqNull(o0, name0);
        neqNull(o1, "o1");
        if (!(o0.equals(o1))) {
            fail(name0 + ".equals(" + ExceptionMessageUtil.valueString(o1) + ")",
                    ExceptionMessageUtil.valueAndName(o0, name0));
        }
    }

    // ----------------------------------------------------------------

    /**
     * require (o0 != null &amp;&amp; o1 != null &amp;&amp; !o0.equals(o1))
     */
    public static void notEquals(Object o0, String name0, Object o1, String name1) {
        neqNull(o0, name0);
        neqNull(o1, name1);
        if (o0.equals(o1)) {
            fail("!" + name0 + ".equals(" + name1 + ")", ExceptionMessageUtil.valueAndName(o0, name0, o1, name1));
        }
    }

    /**
     * require (o0 != null &amp;&amp; o1 != null &amp;&amp; !o0.equals(o1))
     */
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

    public static String nonempty(String s, String name) {
        neqNull(s, name);
        if (!(s.length() > 0)) {
            fail(name + ".length() > 0", ExceptionMessageUtil.valueAndName(s, name));
        }
        return s;
    }

    // ################################################################

    // ----------------------------------------------------------------

    public static <C extends Collection<T>, T> C contains(C collection, String collectionName, T element,
            String elementName) {
        neqNull(collection, collectionName);
        if (!(collection.contains(element))) {
            fail(collectionName + ".contains(" + elementName + ")",
                    ExceptionMessageUtil.valueAndName(element, elementName));
        }
        return collection;
    }

    // ----------------------------------------------------------------

    public static <C extends Collection<T>, T> C notContains(C collection, String collectionName, T element,
            String elementName) {
        neqNull(collection, collectionName);
        if (collection.contains(element)) {
            fail("!" + collectionName + ".contains(" + elementName + ")",
                    ExceptionMessageUtil.valueAndName(element, elementName));
        }
        return collection;
    }

    // ----------------------------------------------------------------

    public static <C extends Collection<T>, T> C notContainsNull(C collection, String collectionName) {
        neqNull(collection, collectionName);
        if (collection.stream().anyMatch(Objects::isNull)) {
            fail(collectionName + " does not contain null");
        }
        return collection;
    }

    // ----------------------------------------------------------------

    public static <M extends Map<K, V>, K, V> M containsKey(M map, String mapName, K key, String keyName) {
        neqNull(map, mapName);
        if (!(map.containsKey(key))) {
            fail(mapName + ".containsKey(" + keyName + ")", ExceptionMessageUtil.valueAndName(key, keyName));
        }
        return map;
    }

    // ----------------------------------------------------------------

    public static <M extends Map<K, V>, K, V> M notContainsKey(M map, String mapName, K key, String keyName) {
        neqNull(map, mapName);
        if (map.containsKey(key)) {
            fail("!" + mapName + ".containsKey(" + keyName + ")", ExceptionMessageUtil.valueAndName(key, keyName));
        }
        return map;
    }

    // ----------------------------------------------------------------

    /** require (offset &gt;= 0 &amp;&amp; offset &lt; length) */
    public static int inRange(int offset, String offsetName, int length, String lengthName) {
        if (!(offset >= 0)) {
            fail(offsetName + " >= 0", ExceptionMessageUtil.valueAndName(offset, offsetName));
        } else if (!(offset < length)) {
            fail(offsetName + " < " + lengthName,
                    ExceptionMessageUtil.valueAndName(offset, offsetName, length, lengthName));
        }
        return offset;
    }

    // ----------------------------------------------------------------

    /** require (offset &gt;= start &amp;&amp; offset &lt; end) */
    public static int inRange(int offset, String offsetName, int start, String startName, int end, String endName) {
        if (!(offset >= start)) {
            fail(offsetName + " >= " + startName,
                    ExceptionMessageUtil.valueAndName(offset, offsetName, start, startName));
        } else if (!(offset < end)) {
            fail(offsetName + " < " + endName, ExceptionMessageUtil.valueAndName(offset, offsetName, end, endName));
        }
        return offset;
    }

    // ----------------------------------------------------------------

    /** require (offset &gt;= 0 &amp;&amp; offset &lt; length) */
    public static long inRange(long offset, String offsetName, long length, String lengthName) {
        if (!(offset >= 0L)) {
            fail(offsetName + " >= 0L", ExceptionMessageUtil.valueAndName(offset, offsetName));
        } else if (!(offset < length)) {
            fail(offsetName + " < " + lengthName,
                    ExceptionMessageUtil.valueAndName(offset, offsetName, length, lengthName));
        }
        return offset;
    }

    // ----------------------------------------------------------------

    /** require (offset &gt;= start &amp;&amp; offset &lt; end) */
    public static long inRange(long offset, String offsetName, long start, String startName, long end, String endName) {
        if (!(offset >= start)) {
            fail(offsetName + " >= " + startName,
                    ExceptionMessageUtil.valueAndName(offset, offsetName, start, startName));
        } else if (!(offset < end)) {
            fail(offsetName + " < " + endName, ExceptionMessageUtil.valueAndName(offset, offsetName, end, endName));
        }
        return offset;
    }

    // ################################################################

    /** require d != {Infinity, -Infinity, NaN}. */
    public static double normalized(double d, String name) {
        if (!(!Double.isNaN(d) && !Double.isInfinite(d))) {
            fail(name + " is normalized (not infinity or NaN)", ExceptionMessageUtil.valueAndName(d, name));
        }
        return d;
    }

    public static <T> T[] nonEmpty(final T[] a, final String name) {
        neqNull(a, name);
        if (!(a.length > 0)) {
            fail(name + ".length > 0", ExceptionMessageUtil.valueAndName(a, name));
        }
        return a;
    }

    public static int[] lengthEqual(final int[] a, final String name, final int length) {
        if (!(a.length == length)) {
            fail(name + ".length == " + length, ExceptionMessageUtil.valueAndName(a, name));
        }
        return a;
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
                fail("Matrix is not square: " + name, "matrix is not square: " + name);
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
                    + "] but was not");
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
                    + "] but was not");
        }
        return trialValue;
    }
}
