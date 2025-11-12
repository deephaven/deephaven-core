//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil.assertj;

import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.impl.sources.IntegerArraySource;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableIntArraySource;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import static io.deephaven.engine.testutil.assertj.ColumnSourceAssert.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class ColumnSourceAssertTest {

    @Test
    void isImmutable() {
        assertThat(new ImmutableIntArraySource()).isImmutable();
        assertFails(() -> assertThat(new IntegerArraySource()).isImmutable());

    }

    @Test
    void isNotImmutable() {
        assertFails(() -> assertThat(new ImmutableIntArraySource()).isNotImmutable());
        assertThat(new IntegerArraySource()).isNotImmutable();
    }

    @Test
    void type() {
        assertThat(new IntegerArraySource()).type().isEqualTo(int.class);
    }

    @Test
    void componentType() {
        assertThat(new IntegerArraySource()).componentType().isNull();
    }

    @Test
    void values() {
        assertThat(new IntegerArraySource()).values(RowSetFactory.empty()).ints().isEmpty();
    }

    private static void assertFails(Runnable r) {
        try {
            r.run();
            failBecauseExceptionWasNotThrown(AssertionFailedError.class);
        } catch (AssertionFailedError e) {
            // expected
        }
    }
}
