//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.api.ColumnName;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.filter.FilterIn;
import io.deephaven.api.literal.Literal;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.table.MatchOptions;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import org.junit.Rule;
import org.junit.Test;

import java.math.BigInteger;

import static io.deephaven.engine.util.TableTools.byteCol;
import static io.deephaven.engine.util.TableTools.charCol;
import static io.deephaven.engine.util.TableTools.floatCol;
import static io.deephaven.engine.util.TableTools.intCol;
import static io.deephaven.engine.util.TableTools.longCol;
import static io.deephaven.engine.util.TableTools.newTable;
import static io.deephaven.engine.util.TableTools.stringCol;
import static io.deephaven.util.QueryConstants.NULL_INT;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Filter values -- query-scope parameters and directly supplied match values -- must select exactly the rows a Java
 * comparison between the column and the value would select. A value that cannot be represented in the column's type is
 * never truncated or wrapped: filters that can evaluate the comparison another way fall back to a
 * {@link ConditionFilter}, and the rest reject it.
 */
public class MatchFilterCoercionTest {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    private static void assertRejected(final Runnable filterOperation) {
        final RuntimeException err = assertThrows(RuntimeException.class, filterOperation::run);
        Throwable cause = err;
        while (cause != null) {
            if (cause instanceof IllegalArgumentException && cause.getMessage() != null
                    && cause.getMessage().startsWith("Cannot convert value")) {
                return;
            }
            cause = cause.getCause();
        }
        throw new AssertionError("expected a value conversion error", err);
    }

    @Test
    public void fractionalParamRangeFallsBackToTheLanguage() {
        QueryScope.addParam("coercionVal", 5.7);
        final Table t = newTable(intCol("X", 4, 5, 6));

        // 5 < 5.7 is true; the parameter was narrowed to 5, which made it false
        assertEquals(2, t.where("X < coercionVal").size());
        assertEquals(1, t.where("X > coercionVal").size());
    }

    @Test
    public void fractionalParamEqualityFallsBackToTheLanguage() {
        QueryScope.addParam("coercionVal", 5.7);
        final Table t = newTable(intCol("X", 5));

        assertEquals(0, t.where("X == coercionVal").size());
        assertEquals(1, t.where("X != coercionVal").size());
    }

    @Test
    public void fractionalParamInIsRejected() {
        QueryScope.addParam("coercionVal", 5.7);
        final Table t = newTable(intCol("X", 5));

        // `in` has no fallback, the same as the literal `X in 5.7`
        assertRejected(() -> t.where("X in coercionVal"));
    }

    @Test
    public void outOfRangeLongParam() {
        QueryScope.addParam("coercionVal", 5_000_000_000L);
        // 705032704 is (int) 5_000_000_000L
        final Table t = newTable(intCol("X", 705032704));

        assertEquals(0, t.where("X == coercionVal").size());
        assertEquals(1, t.where("X < coercionVal").size());
        assertRejected(() -> t.where("X in coercionVal"));
    }

    @Test
    public void inexactDoubleParamAgainstFloatColumn() {
        QueryScope.addParam("coercionVal", 5.7);
        final Table t = newTable(floatCol("F", 5.7f));

        // Java compares (double) 5.7f == 5.7, which is false, and (double) 5.7f < 5.7, which is true
        assertEquals(0, t.where("F == coercionVal").size());
        assertEquals(1, t.where("F < coercionVal").size());
        assertRejected(() -> t.where("F in coercionVal"));
    }

    @Test
    public void losslessParamsStillMatch() {
        QueryScope.addParam("coercionLong", 5L);
        QueryScope.addParam("coercionDouble", 5.0);
        QueryScope.addParam("coercionArray", new double[] {4.0, 6.0});
        QueryScope.addParam("coercionInt", 65);
        final Table t = newTable(intCol("X", 4, 5, 6), charCol("C", 'A', 'B', 'C'));

        assertEquals(1, t.where("X in coercionLong").size());
        assertEquals(1, t.where("X in coercionDouble").size());
        assertEquals(2, t.where("X in coercionArray").size());
        assertEquals(2, t.where("X <= coercionDouble").size());
        assertEquals(1, t.where("C in coercionInt").size());
    }

    @Test
    public void lossyValueInAnArrayParamIsRejected() {
        QueryScope.addParam("coercionArray", new double[] {4.0, 5.5});
        final Table t = newTable(intCol("X", 4, 5, 6));

        assertRejected(() -> t.where("X in coercionArray"));
    }

    @Test
    public void bigIntegerParamAgainstLongColumn() {
        QueryScope.addParam("coercionBig", BigInteger.valueOf(5));
        QueryScope.addParam("coercionHuge", BigInteger.ONE.shiftLeft(64));
        final Table t = newTable(longCol("X", 5L, 0L));

        assertEquals(1, t.where("X in coercionBig").size());
        assertRejected(() -> t.where("X in coercionHuge"));
    }

    @Test
    public void directValueOutOfRangeIsRejected() {
        // 44 is (byte) 300
        final Table t = newTable(byteCol("X", (byte) 44));

        assertRejected(() -> t.where(new MatchFilter(MatchOptions.REGULAR, "X", 300)));
    }

    @Test
    public void directValuesAreConvertedWithoutModifyingTheCallersArray() {
        final Object[] supplied = {5L, 6.0};
        final MatchFilter filter = new MatchFilter(MatchOptions.REGULAR, "X", supplied);
        filter.init(TableDefinition.of(ColumnDefinition.ofInt("X")));

        assertArrayEquals(new Object[] {5, 6}, filter.getValues());
        assertArrayEquals(new Object[] {5L, 6.0}, supplied);
    }

    @Test
    public void literalFilterInAgainstNarrowerColumn() {
        // literals arrive as long and double; they match whenever they are exactly representable
        final Table t = newTable(intCol("X", 4, 5, 6));
        final Filter in = FilterIn.of(ColumnName.of("X"), Literal.of(5L), Literal.of(6.0));
        assertEquals(2, t.where(in).size());

        final Filter lossy = FilterIn.of(ColumnName.of("X"), Literal.of(5L), Literal.of(5.5));
        assertRejected(() -> t.where(lossy));
    }

    @Test
    public void isNaNOnIntegralColumnsMatchesNothing() {
        // Float.NaN converted to int is 0; an int column has no NaN, so neither 0 nor anything else matches
        final Table t = newTable(intCol("X", 0, 1), longCol("L", 0L, 1L), charCol("C", '\0', 'a'));

        assertEquals(0, t.where(Filter.isNaN(ColumnName.of("X"))).size());
        assertEquals(2, t.where(Filter.not(Filter.isNaN(ColumnName.of("X")))).size());
        assertEquals(0, t.where(Filter.isNaN(ColumnName.of("L"))).size());
        assertEquals(0, t.where(Filter.isNaN(ColumnName.of("C"))).size());
    }

    @Test
    public void nullIsAlwaysAccepted() {
        final Table t = newTable(stringCol("S", "a", null), intCol("X", 1, NULL_INT));

        assertEquals(1, t.where(new MatchFilter(MatchOptions.REGULAR, "S", (Object) null)).size());
        assertEquals(1, t.where(new MatchFilter(MatchOptions.REGULAR, "X", (Object) null)).size());
    }

    @Test
    public void conversionFailureWithFailoverUsesTheFailover() {
        QueryScope.addParam("coercionVal", 5.7);
        final MatchFilter filter = (MatchFilter) WhereFilterFactory.getExpression("X == coercionVal");
        filter.init(TableDefinition.of(ColumnDefinition.ofInt("X")));

        assertNotNull(filter.getFailoverFilterIfCached());
        assertNull(filter.getValues());
        assertTrue(MatchFilter.extractMatchFilter(filter).isEmpty());
    }
}
