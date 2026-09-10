//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.lang;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import org.junit.Rule;
import org.junit.Test;

import java.math.BigInteger;
import java.time.LocalDate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * DH-23557 finding 15: an ordering comparison between mutually incomparable types failed with a
 * {@link ClassCastException} thrown from inside {@link Comparable#compareTo}, whose message named neither the
 * comparison nor which operand was which:
 *
 * <pre>
 * ClassCastException: class java.lang.String cannot be cast to class java.time.chrono.ChronoLocalDate
 * </pre>
 *
 * <p>
 * {@code <}, {@code <=}, {@code >} and {@code >=} between two non-numeric types all resolve to
 * {@code QueryLanguageFunctionUtils.less/lessEquals/greater/greaterEquals(Comparable, Comparable)}, which delegate to
 * one {@code compareTo(Comparable, Comparable)}. {@code compareTo} is specified only for mutually comparable arguments,
 * and erasure makes an incomparable one fail inside the callee.
 *
 * <p>
 * The parser deliberately accepts these — {@code TestQueryLanguageParser.testComparisonConversion} asserts that
 * {@code myTestClass > myIntObj} converts rather than being rejected — so the fix is to the diagnostic, not to what
 * compiles. Equality is unaffected and was always safe: {@code eq} compares with {@code equals}, so incomparable
 * operands simply do not match.
 *
 * <p>
 * The fuzzer reached this through the chained comparison {@code lo <= col <= hi}, which Deephaven does not support:
 * Java's grammar parses it as {@code (lo <= col) <= hi}, comparing a {@code Boolean} against {@code hi}. Fuzzer seeds
 * {@code -849809232336840231L}, {@code -2945246487059582836L} and {@code -8652567192287095591L}; the bench no longer
 * generates that form.
 */
public class IncomparableOrderingMessageTest {

    @Rule
    public final EngineCleanup cleanup = new EngineCleanup();

    private static Table mixedTypes() {
        return TableTools.newTable(
                TableTools.intCol("I", 0, 1, 2),
                TableTools.stringCol("Str", "0", "1", "2"),
                TableTools.col("Ld", LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(1), LocalDate.ofEpochDay(2)),
                TableTools.col("Bi", BigInteger.ZERO, BigInteger.ONE, BigInteger.TWO));
    }

    private static String deepestMessage(final RuntimeException thrown) {
        Throwable last = thrown;
        for (Throwable t = thrown; t != null; t = t.getCause()) {
            if (t instanceof IllegalArgumentException) {
                return String.valueOf(t.getMessage());
            }
            last = t;
        }
        return last.getClass().getName() + ": " + last.getMessage();
    }

    /** A String column ordered against a number: the message must name both types. */
    @Test
    public void stringAgainstNumber() {
        final RuntimeException thrown =
                assertThrows(RuntimeException.class, () -> mixedTypes().where("Str <= 1").size());
        final String message = deepestMessage(thrown);
        assertTrue(message, message.contains("Cannot order"));
        assertTrue(message, message.contains("java.lang.String"));
        assertTrue(message, message.contains("java.lang.Integer"));
    }

    /** A LocalDate column ordered against a String. */
    @Test
    public void localDateAgainstString() {
        final RuntimeException thrown =
                assertThrows(RuntimeException.class, () -> mixedTypes().where("Ld <= Str").size());
        final String message = deepestMessage(thrown);
        assertTrue(message, message.contains("Cannot order"));
        assertTrue(message, message.contains("java.time.LocalDate"));
        assertTrue(message, message.contains("java.lang.String"));
    }

    /**
     * The chained comparison, which is what the fuzzer generated. Java parses {@code 0 <= I <= 1} as
     * {@code (0 <= I) <= 1}, so a {@code Boolean} is ordered against an {@code Integer}.
     */
    @Test
    public void chainedComparisonNamesBoolean() {
        final RuntimeException thrown =
                assertThrows(RuntimeException.class, () -> mixedTypes().where("0 <= I <= 1").size());
        final String message = deepestMessage(thrown);
        assertTrue(message, message.contains("Cannot order"));
        assertTrue(message, message.contains("java.lang.Boolean"));
    }

    /** Every ordering operator reaches the same helper. */
    @Test
    public void allFourOrderingOperators() {
        for (final String expression : new String[] {"Ld < Str", "Ld <= Str", "Ld > Str", "Ld >= Str"}) {
            final RuntimeException thrown =
                    assertThrows(expression, RuntimeException.class, () -> mixedTypes().where(expression).size());
            assertTrue(expression + " -> " + deepestMessage(thrown),
                    deepestMessage(thrown).contains("Cannot order"));
        }
    }

    /** Numeric cross-type ordering must keep working, including BigInteger against a boxed int. */
    @Test
    public void numericCrossTypeOrderingStillWorks() {
        final Table table = mixedTypes();
        assertEquals(2, table.where("I <= 1").size());
        assertEquals(2, table.where("I <= 1.5").size());
        assertEquals(2, table.where("Bi <= 1").size());
        // Row-wise, not against a bound: 0<=0, 1<=1, 2<=2.
        assertEquals(3, table.where("Bi <= I").size());
    }

    /** Same-type ordering must keep working. */
    @Test
    public void sameTypeOrderingStillWorks() {
        final Table table = mixedTypes();
        assertEquals(2, table.where("Str <= `1`").size());
        assertEquals(2, table.where("Ld <= '1970-01-02'").size());
    }

    /** Equality across incomparable types was always safe and must stay so: it matches nothing. */
    @Test
    public void equalityAcrossIncomparableTypesStillMatchesNothing() {
        final Table table = mixedTypes();
        assertEquals(0, table.where("I == Str").size());
        assertEquals(0, table.where("Bi == Ld").size());
    }

    /**
     * Null operands are handled above {@code compareTo} -- a null first operand compares as less than anything -- so a
     * null matches {@code <=}. That is pre-existing behaviour, pinned here to show the new catch block does not disturb
     * it.
     */
    @Test
    public void nullOperandsAreUnaffected() {
        final Table table = TableTools.newTable(
                TableTools.col("Ld", LocalDate.ofEpochDay(0), null, LocalDate.ofEpochDay(2)));
        assertEquals(3, table.where("Ld <= '1970-01-03'").size());
        assertEquals(1, table.where("isNull(Ld)").size());
    }
}
