//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil.assertj;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.QueryTableTestBase;
import io.deephaven.engine.util.TableTools;
import junit.framework.ComparisonFailure;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import java.time.Duration;

import static io.deephaven.engine.testutil.assertj.TableAssert.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class TableAssertTest {

    private EngineState engineState;

    static class EngineState extends QueryTableTestBase {
    }

    @BeforeEach
    void setUp() throws Exception {
        engineState = new EngineState();
        engineState.setUp();
    }

    @AfterEach
    void tearDown() throws Exception {
        engineState.tearDown();
    }

    @Test
    void isEqualTo() {
        final Table foo = TableTools.newTable(TableTools.intCol("Foo", 1, 2, 3));
        final Table fooB = TableTools.newTable(TableTools.intCol("Foo", 1, 2, 3));
        final Table foo2 = TableTools.newTable(TableTools.intCol("Foo", 1, 2, 4));
        assertThat(foo).isEqualTo(fooB);
        assertThat(fooB).isEqualTo(foo);
        assertFailsCmp(() -> assertThat(foo).isEqualTo(foo2));
        assertFailsCmp(() -> assertThat(foo2).isEqualTo(foo));
    }

    @Test
    void isNotEqualTo() {
        final Table foo = TableTools.newTable(TableTools.intCol("Foo", 1, 2, 3));
        final Table foo2 = TableTools.newTable(TableTools.intCol("Foo", 1, 2, 4));
        try {
            assertThat(foo).isNotEqualTo(foo2);
            failBecauseExceptionWasNotThrown(UnsupportedOperationException.class);
        } catch (UnsupportedOperationException e) {
            // expected
        }
    }

    @Test
    void rowSet() {
        assertThat(TableTools.emptyTable(2L)).rowSet().size().isEqualTo(2L);
    }

    @Test
    void columnSource() {
        final Table foo = TableTools.newTable(TableTools.intCol("Foo", 1, 2, 3));
        assertThat(foo).columnSource("Foo").type().isEqualTo(int.class);
        assertThat(foo).columnSource("Foo", int.class).type().isEqualTo(int.class);
        assertThat(foo).columnSource("Foo", int.class, null).type().isEqualTo(int.class);
    }

    @Test
    void columnSourceValues() {
        final Table foo = TableTools.newTable(TableTools.intCol("Foo", 1, 2, 3));
        assertThat(foo).columnSourceValues("Foo").ints().containsExactly(1, 2, 3);
        assertThat(foo).columnSourceValues("Foo", int.class).ints().containsExactly(1, 2, 3);
        assertThat(foo).columnSourceValues("Foo", int.class, null).ints().containsExactly(1, 2, 3);
    }

    @Test
    void isEmpty() {
        assertThat(TableTools.emptyTable(0L)).isEmpty();
        assertFails(() -> assertThat(TableTools.emptyTable(2L)).isEmpty());
    }

    @Test
    void isNotEmpty() {
        assertFails(() -> assertThat(TableTools.emptyTable(0L)).isNotEmpty());
        assertThat(TableTools.emptyTable(2L)).isNotEmpty();
    }

    @Test
    void isRefreshing() {
        assertThat(TableTools.timeTableBuilder().period(Duration.ofSeconds(1)).build()).isRefreshing();
        assertFails(() -> assertThat(TableTools.emptyTable(2L)).isRefreshing());
    }

    @Test
    void isNotRefreshing() {
        assertFails(() -> assertThat(TableTools.timeTableBuilder().period(Duration.ofSeconds(1)).build())
                .isNotRefreshing());
        assertThat(TableTools.emptyTable(2L)).isNotRefreshing();
    }

    @Test
    void isFlat() {
        assertThat(TableTools.emptyTable(2L)).isFlat();
        assertFails(() -> assertThat(TableTools.emptyTable(2L).tail(1)).isFlat());
    }

    @Test
    void isNotFlat() {
        assertFails(() -> assertThat(TableTools.emptyTable(2L)).isNotFlat());
        assertThat(TableTools.emptyTable(2L).tail(1)).isNotFlat();
    }

    private static void assertFails(Runnable r) {
        try {
            r.run();
            failBecauseExceptionWasNotThrown(AssertionFailedError.class);
        } catch (AssertionFailedError e) {
            // expected
        }
    }

    private static void assertFailsCmp(Runnable r) {
        try {
            r.run();
            failBecauseExceptionWasNotThrown(ComparisonFailure.class);
        } catch (ComparisonFailure e) {
            // expected
        }
    }
}
