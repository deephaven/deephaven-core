/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.api.RangeJoinMatch;
import io.deephaven.chunk.util.pools.ChunkPoolReleaseTracking;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import io.deephaven.test.types.OutOfBandTest;

import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collection;
import java.util.List;

import static io.deephaven.api.agg.Aggregation.AggGroup;
import static io.deephaven.engine.table.TableFactory.emptyTable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

/**
 * Unit tests for the {@link QueryTable#rangeJoin(Table, Collection, RangeJoinMatch, Collection)} and related delegation
 * from {@link io.deephaven.api.TableOperations#rangeJoin(Object, Collection, Collection)}.
 */
@Category(OutOfBandTest.class)
public class QueryTableRangeJoinTest {

    private static final boolean PRINT_EXPECTED_EXCEPTIONS = true;

    @Rule
    public final EngineCleanup base = new EngineCleanup();

    @Before
    public void setUp() throws Exception {
        ChunkPoolReleaseTracking.enableStrict();
    }

    @After
    public void tearDown() throws Exception {
        ChunkPoolReleaseTracking.checkAndDisable();
    }

    @Test
    public void testRefreshingUnsupported() {
        final Table t1 = emptyTable(100).update("II=ii", "BB=II % 5", "LSV=ii / 0.7", "LEV=ii / 0.1");
        final Table t2 = emptyTable(100).update("II=ii", "BB=II % 5", "RRV=ii / 0.3");
        final Runnable test = () -> t1.rangeJoin(t2, List.of("BB", "LSV < RRV < LEV"), List.of(AggGroup("II")));

        // {static, static} works
        test.run();

        // {refreshing, static} fails
        t1.setRefreshing(true);
        expectException(test, UnsupportedOperationException.class);

        // {refreshing, refreshing} fails
        t2.setRefreshing(true);
        expectException(test, UnsupportedOperationException.class);

        // {static, refreshing} fails
        t1.setRefreshing(false);
        expectException(test, UnsupportedOperationException.class);
    }

    @Test
    public void testInvalidExactMatches() {
        final Table t1 = emptyTable(100).update("II=ii", "BB=i % 5", "LSV=ii / 0.7", "LEV=ii / 0.1");
        final Table t2 = emptyTable(100).update("II=ii", "BB=i % 5", "RRV=ii / 0.3");
        final Runnable[] tests = new Runnable[] {
                // Missing both
                () -> t1.rangeJoin(t2, List.of("WRONG", "LSV < RRV < LEV"), List.of(AggGroup("II"))),
                // Missing left
                () -> t1.rangeJoin(t2, List.of("WRONG=BB", "LSV < RRV < LEV"), List.of(AggGroup("II"))),
                // Missing right
                () -> t1.rangeJoin(t2, List.of("BB=WRONG", "LSV < RRV < LEV"), List.of(AggGroup("II"))),
                // Missing both after valid
                () -> t1.rangeJoin(t2, List.of("II", "WRONG", "LSV < RRV < LEV"), List.of(AggGroup("II"))),
                // Missing left after valid
                () -> t1.rangeJoin(t2, List.of("II", "WRONG=BB", "LSV < RRV < LEV"), List.of(AggGroup("II"))),
                // Missing right after valid
                () -> t1.rangeJoin(t2, List.of("II", "BB=WRONG", "LSV < RRV < LEV"), List.of(AggGroup("II"))),
                // Missing both before valid
                () -> t1.rangeJoin(t2, List.of("WRONG", "II", "LSV < RRV < LEV"), List.of(AggGroup("II"))),
                // Missing left before valid
                () -> t1.rangeJoin(t2, List.of("WRONG=BB", "II", "LSV < RRV < LEV"), List.of(AggGroup("II"))),
                // Missing right before valid
                () -> t1.rangeJoin(t2, List.of("BB=WRONG", "II", "LSV < RRV < LEV"), List.of(AggGroup("II"))),
                // Missing both and bad data type
                () -> t1.rangeJoin(t2, List.of("WRONG", "II=BB", "LSV < RRV < LEV"), List.of(AggGroup("II"))),
                // Missing left and bad data type
                () -> t1.rangeJoin(t2, List.of("WRONG=BB", "II=BB", "LSV < RRV < LEV"), List.of(AggGroup("II"))),
                // Missing right and bad data type
                () -> t1.rangeJoin(t2, List.of("BB=WRONG", "II=BB", "LSV < RRV < LEV"), List.of(AggGroup("II"))),
                // Missing both and bad component type
                () -> t1.update("II=Long.toString(II)").groupBy("BB")
                        .rangeJoin(t2.update("II=(CharSequence) Long.toString(II)").groupBy("BB"),
                                List.of("WRONG", "II", "LSV < RRV < LEV"), List.of(AggGroup("II"))),
                // Missing left and bad component type (none for left)
                () -> t1.rangeJoin(t2.update("II=(CharSequence) Long.toString(II)").groupBy("BB"),
                        List.of("WRONG=BB", "II", "LSV < RRV < LEV"), List.of(AggGroup("II"))),
                // Missing right and bad component type (none for right)
                () -> t1.update("II=Long.toString(II)").groupBy("BB")
                        .rangeJoin(t2, List.of("BB=WRONG", "II", "LSV < RRV < LEV"), List.of(AggGroup("II"))),
        };
        for (final Runnable test : tests) {
            expectException(test, IllegalArgumentException.class);
        }
    }

    @Test
    public void testInvalidRangeMatch() {
        final Table t1 = emptyTable(100).update("II=ii", "BB=i % 5", "LSV=ii / 0.7", "LEV=ii / 0.1");
        final Table t2 = emptyTable(100).update("II=ii", "BB=i % 5", "RRV=ii / 0.3");
        final Runnable[] tests = new Runnable[] {
                // Missing all
                () -> t1.rangeJoin(t2, List.of("WRONG1 < WRONG < WRONG2"), List.of(AggGroup("II"))),
                // Incompatible types
                () -> t1.rangeJoin(t2.update("RRV=(int) RRV"), List.of("LSV < RRV < LEV"), List.of(AggGroup("II"))),
                // Invalid type
                () -> t1.update("LSV=new double[] {LSV}", "LEV=new double[] {LEV}")
                        .rangeJoin(t2.update("RRV = new double[] {RRV}"),
                                List.of("LSV < RRV < LEV"), List.of(AggGroup("II"))),
        };
        for (final Runnable test : tests) {
            expectException(test, IllegalArgumentException.class);
        }
    }

    private static void expectException(
            @NotNull final Runnable test,
            @NotNull final Class<? extends Exception> exceptionClass) {
        try {
            test.run();
            failBecauseExceptionWasNotThrown(exceptionClass);
        } catch (Throwable expected) {
            assertThat(expected).isInstanceOf(exceptionClass);
            if (PRINT_EXPECTED_EXCEPTIONS) {
                expected.printStackTrace(System.out);
            }
        }
    }

    // TODO:
    // 1. Test all types
    // 2. Test NaN filtering for floats and doubles
    // 3. Test empty rights
    // 4. Test re-sizing lefts
    // 5. Test re-sizing rights
    // 6. Test all start rules
    // 7. Test all end rules
}
