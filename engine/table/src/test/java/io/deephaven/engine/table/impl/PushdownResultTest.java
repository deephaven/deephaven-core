//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import org.junit.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

public class PushdownResultTest {

    @Test
    public void match() {
        try (
                final WritableRowSet selection = RowSetFactory.fromRange(0, 29);
                final PushdownResult r = PushdownResult.match(selection)) {
            assertThat(r.isFinished()).isTrue();
            assertThat(r.selection()).isEqualTo(selection);
            assertThat(r.match()).isEqualTo(selection);
            assertThat(r.maybeMatch().isEmpty()).isTrue();
            try (final WritableRowSet noMatchCopy = r.noMatchCopy()) {
                assertThat(noMatchCopy.isEmpty()).isTrue();
            }
            assertThat(r.noMatchSize()).isZero();
        }
    }

    @Test
    public void maybeMatch() {
        try (
                final WritableRowSet selection = RowSetFactory.fromRange(0, 29);
                final PushdownResult r = PushdownResult.maybeMatch(selection)) {
            assertThat(r.isFinished()).isFalse();
            assertThat(r.selection()).isEqualTo(selection);
            assertThat(r.match().isEmpty()).isTrue();
            assertThat(r.maybeMatch()).isEqualTo(selection);
            try (final WritableRowSet noMatchCopy = r.noMatchCopy()) {
                assertThat(noMatchCopy.isEmpty()).isTrue();
            }
            assertThat(r.noMatchSize()).isZero();
        }
    }

    @Test
    public void noMatch() {
        try (
                final WritableRowSet selection = RowSetFactory.fromRange(0, 29);
                final PushdownResult r = PushdownResult.noMatch(selection)) {
            assertThat(r.isFinished()).isTrue();
            assertThat(r.selection()).isEqualTo(selection);
            assertThat(r.match().isEmpty()).isTrue();
            assertThat(r.maybeMatch().isEmpty()).isTrue();
            try (final WritableRowSet noMatchCopy = r.noMatchCopy()) {
                assertThat(noMatchCopy).isEqualTo(selection);
            }
            assertThat(r.noMatchSize()).isEqualTo(30);
        }
    }

    @Test
    public void basicConstruction() {
        try (
                final WritableRowSet selection = RowSetFactory.fromRange(0, 29);
                final WritableRowSet match = RowSetFactory.fromRange(0, 9);
                final WritableRowSet maybeMatch = RowSetFactory.fromRange(10, 19);
                final WritableRowSet noMatch = RowSetFactory.fromRange(20, 29);
                final PushdownResult r1 = PushdownResult.of(selection, match, maybeMatch);
                final PushdownResult r2 = PushdownResult.ofUnsafe(selection, match, maybeMatch)) {
            for (final PushdownResult r : Arrays.asList(r1, r2)) {
                assertThat(r.isFinished()).isFalse();
                assertThat(r.selection()).isEqualTo(selection);
                assertThat(r.match()).isEqualTo(match);
                assertThat(r.maybeMatch()).isEqualTo(maybeMatch);
                try (final WritableRowSet noMatchCopy = r.noMatchCopy()) {
                    assertThat(noMatchCopy).isEqualTo(noMatch);
                }
                assertThat(r.noMatchSize()).isEqualTo(10);
            }
        }
    }

    @Test
    public void overlappingSets() {
        try (
                final WritableRowSet selection = RowSetFactory.fromRange(0, 29);
                final WritableRowSet match = RowSetFactory.fromRange(0, 9);
                final WritableRowSet maybeMatch = RowSetFactory.fromRange(9, 19)) {
            try {
                PushdownResult.of(selection, match, maybeMatch);
            } catch (IllegalArgumentException e) {
                assertThat(e).hasMessageContaining("match and maybeMatch should be non-overlapping row sets");
            }
            // Not testing the result of this besides the fact that it does not throw an error
            PushdownResult.ofUnsafe(selection, match, maybeMatch);
        }
    }

    @Test
    public void matchNotSubset() {
        try (
                final WritableRowSet selection = RowSetFactory.fromRange(1, 29);
                final WritableRowSet match = RowSetFactory.fromRange(0, 9);
                final WritableRowSet maybeMatch = RowSetFactory.fromRange(10, 19)) {
            try {
                PushdownResult.of(selection, match, maybeMatch);
            } catch (IllegalArgumentException e) {
                assertThat(e).hasMessageContaining("match must be a subset of selection");
            }
            // Not testing the result of this besides the fact that it does not throw an error
            PushdownResult.ofUnsafe(selection, match, maybeMatch);
        }
    }

    @Test
    public void maybeMatchNotSubset() {
        try (
                final WritableRowSet selection = RowSetFactory.fromRange(1, 29);
                final WritableRowSet match = RowSetFactory.fromRange(10, 19);
                final WritableRowSet maybeMatch = RowSetFactory.fromRange(0, 9)) {
            try {
                PushdownResult.of(selection, match, maybeMatch);
            } catch (IllegalArgumentException e) {
                assertThat(e).hasMessageContaining("maybeMatch must be a subset of selection");
            }
            // Not testing the result of this besides the fact that it does not throw an error
            PushdownResult.ofUnsafe(selection, match, maybeMatch);
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
            } catch (IllegalArgumentException e) {
                assertThat(e).hasMessageContaining("match must be a subset of selection");
            }
            try {
                PushdownResult.ofUnsafe(selection, match, maybeMatch);
            } catch (IllegalArgumentException e) {
                assertThat(e).hasMessageContaining("matchSize + maybeMatchSize > selectionSize, 31 + 0 > 30");
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
            } catch (IllegalArgumentException e) {
                assertThat(e).hasMessageContaining("maybeMatch must be a subset of selection");
            }
            try {
                PushdownResult.ofUnsafe(selection, match, maybeMatch);
            } catch (IllegalArgumentException e) {
                assertThat(e).hasMessageContaining("matchSize + maybeMatchSize > selectionSize, 0 + 31 > 30");
            }
        }
    }
}
