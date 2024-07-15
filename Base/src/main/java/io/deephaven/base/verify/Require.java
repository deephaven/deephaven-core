//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base.verify;

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
 * <li>void holdsLock/notHoldsLock(Object, String name)
 * </ul>
 * <ul>
 * <li>void instanceOf/notInstanceOf(Object, String name, Class type)
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
public final class Require extends RequireBase {

    // we should only have static methods
    private Require() {}

    // ################################################################
    // holdsLock, notHoldsLock

    // ----------------------------------------------------------------

    public static void holdsLock(Object o, String name) {
        neqNull(o, "o");
        if (!Thread.holdsLock(o)) {
            fail("\"" + Thread.currentThread().getName() + "\".holdsLock(" + name + ")");
        }
    }

    // ----------------------------------------------------------------

    public static void notHoldsLock(Object o, String name) {
        neqNull(o, "o");
        if (Thread.holdsLock(o)) {
            fail("!\"" + Thread.currentThread().getName() + "\".holdsLock(" + name + ")");
        }
    }


    // ################################################################
    // instanceOf, notInstanceOf

    // ----------------------------------------------------------------

    public static <T> void instanceOf(Object o, String name, Class<T> type) {
        if (!type.isInstance(o)) {
            fail(name + " instanceof " + type, null == o ? ExceptionMessageUtil.valueAndName(o, name)
                    : name + " instanceof " + o.getClass() + " (" + ExceptionMessageUtil.valueAndName(o, name) + ")");
        }
    }

    // ----------------------------------------------------------------

    public static <T> void notInstanceOf(Object o, String name, Class<T> type) {
        if (type.isInstance(o)) {
            fail("!(" + name + " instanceof " + type + ")",
                    name + " instanceof " + o.getClass() + " (" + ExceptionMessageUtil.valueAndName(o, name) + ")");
        }
    }
}
