//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.api.RawString;
import io.deephaven.api.Strings;
import io.deephaven.api.filter.Filter;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * DH-23557 finding 14: {@link Strings#of(Filter)} cannot render a {@link WhereFilter}, so using it to build an error
 * message replaces the real exception with an unrelated one.
 *
 * <p>
 * {@code Strings} renders the declarative {@code io.deephaven.api} filter API through {@code Filter.Visitor}, and
 * {@code WhereFilter}'s default {@code walk(Filter.Visitor)} throws
 * {@code UnsupportedOperationException("WhereFilters do not implement walk")}. That is by design — a
 * {@code WhereFilter} is the engine's own representation — but it makes {@code Strings.of} a trap inside a
 * {@code catch} block: the message is evaluated before the wrapping exception is constructed, so the original cause is
 * discarded and the caller sees the rendering failure instead.
 *
 * <p>
 * {@code DataIndexPushdownManager.pushdownDataIndex} did exactly that when applying a filter to a data-index table
 * failed. The fuzzer surfaced it as a divergence rather than as a lost message: for one underlying
 * {@code ClassCastException} the in-memory path died rendering the message while the parquet path swallowed the failure
 * and declined the index, so the two paths reported different exception types. Fuzzer case seed
 * {@code 2423783905725303439L}.
 *
 * <p>
 * This test pins the constraint itself, so the trap is not walked into again. The end-to-end regression is that seed,
 * replayed by {@code PushdownFuzzerTest.testInterestingSeeds}.
 */
public class WhereFilterStringsRenderingTest {

    @Rule
    public final EngineCleanup cleanup = new EngineCleanup();

    private static WhereFilter initializedConditionFilter() {
        final WhereFilter filter = WhereFilter.of(RawString.of("A > 1 && B != null"));
        filter.init(TableDefinition.of(ColumnDefinition.ofInt("A"), ColumnDefinition.ofString("B")));
        return filter;
    }

    /** The trap: rendering an engine filter with the declarative renderer throws. */
    @Test
    public void stringsOfAWhereFilterThrows() {
        final WhereFilter filter = initializedConditionFilter();
        final UnsupportedOperationException thrown =
                assertThrows(UnsupportedOperationException.class, () -> Strings.of(filter));
        assertTrue(thrown.getMessage(), thrown.getMessage().contains("do not implement walk"));
    }

    /** What to use instead, and what the fix now uses: the filter's own rendering. */
    @Test
    public void toStringOfAWhereFilterWorks() {
        final String rendered = initializedConditionFilter().toString();
        assertNotNull(rendered);
        assertTrue(rendered, rendered.contains("A") && rendered.contains("B"));
    }

    /** A match filter takes the same default, so the constraint is not specific to formula filters. */
    @Test
    public void stringsOfAMatchFilterThrowsToo() {
        final WhereFilter filter = WhereFilter.of(Filter.isNull(io.deephaven.api.ColumnName.of("A")));
        filter.init(TableDefinition.of(ColumnDefinition.ofInt("A")));
        assertThrows(UnsupportedOperationException.class, () -> Strings.of(filter));
        assertNotNull(filter.toString());
    }

    /** The declarative filter it was built from renders fine, which is what Strings.of is for. */
    @Test
    public void stringsOfADeclarativeFilterWorks() {
        assertTrue(Strings.of(RawString.of("A > 1")).contains("A > 1"));
    }
}
