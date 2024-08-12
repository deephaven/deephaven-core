//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil.assertj;

import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import static io.deephaven.engine.testutil.assertj.RowSetAssert.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class RowSetAssertTest {
    @Test
    void keys() {
        assertThat(RowSetFactory.empty()).keys().isEmpty();
        assertThat(RowSetFactory.flat(2L)).keys().containsExactly(0L, 1L);
    }

    @Test
    void firstRowKey() {
        assertThat(RowSetFactory.empty()).firstRowKey().isNotPresent();
        assertThat(RowSetFactory.flat(2L)).firstRowKey().hasValue(0L);
    }

    @Test
    void lastRowKey() {
        assertThat(RowSetFactory.empty()).lastRowKey().isNotPresent();
        assertThat(RowSetFactory.flat(2L)).lastRowKey().hasValue(1L);
    }

    @Test
    void find() {
        assertThat(RowSetFactory.empty()).find(42L).isEqualTo(-1L);
        assertThat(RowSetFactory.flat(2L)).find(1L).isEqualTo(1L);
    }

    @Test
    void get() {
        assertThat(RowSetFactory.flat(2L)).get(1L).isEqualTo(1L);
    }

    @Test
    void size() {
        assertThat(RowSetFactory.empty()).size().isEqualTo(0L);
        assertThat(RowSetFactory.flat(2L)).size().isEqualTo(2L);
    }

    @Test
    void containsRange() {
        assertThat(RowSetFactory.fromRange(0L, 100L)).containsRange(49L, 51L);
        assertFails(() -> assertThat(RowSetFactory.fromRange(0L, 100L)).containsRange(99L, 101L));
    }

    @Test
    void doesNotContainsRange() {
        assertFails(() -> assertThat(RowSetFactory.fromRange(0L, 100L)).doesNotContainsRange(49L, 51L));
        assertThat(RowSetFactory.fromRange(0L, 100L)).doesNotContainsRange(99L, 101L);
    }

    @Test
    void overlapsRange() {
        assertThat(RowSetFactory.fromRange(0L, 100L)).overlapsRange(99L, 101L);
        assertFails(() -> assertThat(RowSetFactory.fromRange(0L, 100L)).overlapsRange(101L, 199L));
    }

    @Test
    void doesNotOverlapsRange() {
        assertFails(() -> assertThat(RowSetFactory.fromRange(0L, 100L)).doesNotOverlapsRange(99L, 101L));
        assertThat(RowSetFactory.fromRange(0L, 100L)).doesNotOverlapsRange(101L, 199L);
    }

    @Test
    void isFlat() {
        assertThat(RowSetFactory.flat(2L)).isFlat();
        assertFails(() -> assertThat(RowSetFactory.fromRange(1L, 2L)).isFlat());
    }

    @Test
    void isNotFlat() {
        assertFails(() -> assertThat(RowSetFactory.flat(2L)).isNotFlat());
        assertThat(RowSetFactory.fromRange(1L, 2L)).isNotFlat();
    }

    @Test
    void isEmpty() {
        assertThat(RowSetFactory.empty()).isEmpty();
        assertFails(() -> assertThat(RowSetFactory.flat(1L)).isEmpty());
    }

    @Test
    void isNotEmpty() {
        assertFails(() -> assertThat(RowSetFactory.empty()).isNotEmpty());
        assertThat(RowSetFactory.flat(2L)).isNotEmpty();
    }

    @Test
    void isTracking() {
        assertFails(() -> assertThat(RowSetFactory.flat(2L)).isTracking());
        try (final TrackingWritableRowSet tracking = RowSetFactory.flat(2L).toTracking()) {
            assertThat(tracking).isTracking();
        }
    }

    @Test
    void isNotTracking() {
        assertThat(RowSetFactory.flat(2L)).isNotTracking();
        assertFails(() -> {
            try (final TrackingWritableRowSet tracking = RowSetFactory.flat(2L).toTracking()) {
                assertThat(tracking).isNotTracking();
            }
        });
    }

    @Test
    void isWritable() {
        assertThat(RowSetFactory.empty()).isWritable();
        // we don't have any instances impls that are not writable
    }

    @Test
    void isNotWritable() {
        assertFails(() -> assertThat(RowSetFactory.empty()).isNotWritable());
        // we don't have any instances impls that are not writable
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
