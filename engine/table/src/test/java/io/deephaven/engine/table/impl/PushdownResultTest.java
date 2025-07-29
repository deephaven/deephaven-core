//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class PushdownResultTest {

    @Test
    public void allMatch() {
        try (
                final WritableRowSet selection = RowSetFactory.fromRange(0, 29);
                final PushdownResult r = PushdownResult.allMatch(selection)) {
            assertThat(r.match()).isEqualTo(selection);
            assertThat(r.maybeMatch().isEmpty()).isTrue();
        }
    }

    @Test
    public void allMaybeMatch() {
        try (
                final WritableRowSet selection = RowSetFactory.fromRange(0, 29);
                final PushdownResult r = PushdownResult.allMaybeMatch(selection)) {
            assertThat(r.match().isEmpty()).isTrue();
            assertThat(r.maybeMatch()).isEqualTo(selection);
        }
    }

    @Test
    public void allNoMatch() {
        try (
                final WritableRowSet selection = RowSetFactory.fromRange(0, 29);
                final PushdownResult r = PushdownResult.allNoMatch(selection)) {
            assertThat(r.match().isEmpty()).isTrue();
            assertThat(r.maybeMatch().isEmpty()).isTrue();
        }
    }

    @Test
    public void basicConstruction() {
        try (
                final WritableRowSet selection = RowSetFactory.fromRange(0, 29);
                final WritableRowSet match = RowSetFactory.fromRange(0, 9);
                final WritableRowSet maybeMatch = RowSetFactory.fromRange(10, 19);
                final PushdownResult r = PushdownResult.of(selection, match, maybeMatch)) {
            assertThat(r.match()).isEqualTo(match);
            assertThat(r.maybeMatch()).isEqualTo(maybeMatch);
        }
    }

    @Test
    public void overlappingSets() {
        try (
                final WritableRowSet selection = RowSetFactory.fromRange(0, 29);
                final WritableRowSet match = RowSetFactory.fromRange(0, 9);
                final WritableRowSet maybeMatch = RowSetFactory.fromRange(9, 19)) {
            if (PushdownResult.FORCE_VALIDATION) {
                try {
                    PushdownResult.of(selection, match, maybeMatch);
                    failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
                } catch (IllegalArgumentException e) {
                    assertThat(e).hasMessageContaining("match and maybeMatch should be non-overlapping row sets");
                }
            } else {
                // does not catch precondition failure
                PushdownResult.of(selection, match, maybeMatch);
            }
        }
    }

    @Test
    public void matchNotSubset() {
        try (
                final WritableRowSet selection = RowSetFactory.fromRange(1, 29);
                final WritableRowSet match = RowSetFactory.fromRange(0, 9);
                final WritableRowSet maybeMatch = RowSetFactory.fromRange(10, 19)) {
            if (PushdownResult.FORCE_VALIDATION) {
                try {
                    PushdownResult.of(selection, match, maybeMatch);
                    failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
                } catch (IllegalArgumentException e) {
                    assertThat(e).hasMessageContaining("match must be a subset of selection");
                }
            } else {
                // does not catch precondition failure
                PushdownResult.of(selection, match, maybeMatch);
            }
        }
    }

    @Test
    public void maybeMatchNotSubset() {
        try (
                final WritableRowSet selection = RowSetFactory.fromRange(1, 29);
                final WritableRowSet match = RowSetFactory.fromRange(10, 19);
                final WritableRowSet maybeMatch = RowSetFactory.fromRange(0, 9)) {
            if (PushdownResult.FORCE_VALIDATION) {
                try {
                    PushdownResult.of(selection, match, maybeMatch);
                    failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
                } catch (IllegalArgumentException e) {
                    assertThat(e).hasMessageContaining("maybeMatch must be a subset of selection");
                }
            } else {
                // does not catch precondition failure
                PushdownResult.of(selection, match, maybeMatch);
            }
        }
    }

    @Test
    public void obviouslyBadMatch() {
        try (
                final WritableRowSet selection = RowSetFactory.fromRange(0, 29);
                final WritableRowSet match = RowSetFactory.fromRange(0, 30);
                final WritableRowSet maybeMatch = RowSetFactory.empty()) {
            try {
                PushdownResult.of(selection, match, maybeMatch);
                failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
            } catch (IllegalArgumentException e) {
                assertThat(e).hasMessageContaining(PushdownResult.FORCE_VALIDATION
                        ? "match must be a subset of selection"
                        : "matchSize + maybeMatchSize > selectionSize, 31 + 0 > 30");
            }
        }
    }

    @Test
    public void obviouslyBadMaybeMatch() {
        try (
                final WritableRowSet selection = RowSetFactory.fromRange(0, 29);
                final WritableRowSet match = RowSetFactory.empty();
                final WritableRowSet maybeMatch = RowSetFactory.fromRange(0, 30)) {
            try {
                PushdownResult.of(selection, match, maybeMatch);
                failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
            } catch (IllegalArgumentException e) {
                assertThat(e).hasMessageContaining(PushdownResult.FORCE_VALIDATION
                        ? "maybeMatch must be a subset of selection"
                        : "matchSize + maybeMatchSize > selectionSize, 31 + 0 > 30");
            }
        }
    }
}
