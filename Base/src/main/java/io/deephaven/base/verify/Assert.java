//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base.verify;

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
 * <li>eq, neq, lt, leq, gt, get correspond to ==, !=, &lt;, &lt;=, &gt;, &gt;=, e.g.,
 * <ul>
 * <li>For Object a and b, Assert.eq(a, "a", b, "b") corresponds to assert (a == b)
 * <li>For Object o, Assert.neqNull(o, "o") corresponds to assert (o != null)
 * <li>for int x, Assert.eqZero(x, "x") corresponds to assert (x == 0)
 * </ul>
 * <li>equals corresponds to Object.equals (preceded by necessary null checks), e.g.,
 * <ul>
 * <li>For Object a and b, Assert.equals(a, "a", b, "b") corresponds to assert (a!= null &amp;&amp; b != null &amp;&amp;
 * a.equals(b))
 * <li>for String s, Assert.nonempty(s, "s") corresponds to assert (s != null &amp;&amp; s.length() != 0)
 * </ul>
 * </ul>
 */
public final class Assert extends AssertBase {

    // we should only have static methods
    private Assert() {}

    // ################################################################
    // holdsLock, notHoldsLock

    // ----------------------------------------------------------------
    /** assert (o != null &amp;&amp; (current thread holds o's lock)) */
    public static void holdsLock(Object o, String name) {
        neqNull(o, "o");
        if (!Thread.holdsLock(o)) {
            fail("\"" + Thread.currentThread().getName() + "\".holdsLock(" + name + ")");
        }
    }

    // ----------------------------------------------------------------
    /** assert (o != null &amp;&amp; !(current thread holds o's lock)) */
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
}
