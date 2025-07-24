//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset;

import org.junit.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class RowSetFactoryUnionInsertTest {

    @Test
    public void empty() {
        try (
                final WritableRowSet actual = RowSetFactory.unionInsert(List.of())) {
            assertThat(actual.isEmpty()).isTrue();
        }
        try (
                final WritableRowSet r1 = RowSetFactory.empty();
                final WritableRowSet actual = RowSetFactory.unionInsert(List.of(r1))) {
            assertThat(actual.isEmpty()).isTrue();
        }
    }

    @Test
    public void unionInsert() {
        try (
                final WritableRowSet r1 = RowSetFactory.fromRange(0, 9);
                final WritableRowSet actual = RowSetFactory.unionInsert(List.of(r1))) {
            assertThat(actual).isEqualTo(r1);
        }
        try (
                final WritableRowSet r1 = RowSetFactory.fromRange(0, 9);
                final WritableRowSet r2 = RowSetFactory.fromRange(20, 29);
                final WritableRowSet expected = r1.union(r2);
                final WritableRowSet actual = RowSetFactory.unionInsert(List.of(r1, r2))) {
            assertThat(actual).isEqualTo(expected);
        }
        try (
                final WritableRowSet r1 = RowSetFactory.fromRange(0, 9);
                final WritableRowSet r2 = RowSetFactory.fromRange(20, 29);
                final WritableRowSet r3 = RowSetFactory.fromKeys(42);
                final WritableRowSet e1 = r1.union(r2);
                final WritableRowSet expected = e1.union(r3);
                final WritableRowSet actual = RowSetFactory.unionInsert(List.of(r1, r2, r3))) {
            assertThat(actual).isEqualTo(expected);
        }
    }

    @Test
    public void overlappingUnionInsert() {
        try (
                final WritableRowSet r1 = RowSetFactory.fromRange(0, 9);
                final WritableRowSet r2 = RowSetFactory.fromRange(5, 19);
                final WritableRowSet expected = r1.union(r2);
                final WritableRowSet actual = RowSetFactory.unionInsert(List.of(r1, r2))) {
            assertThat(actual).isEqualTo(expected);
        }
    }

    @Test
    public void outOfOrder() {
        try (
                final WritableRowSet r1 = RowSetFactory.fromRange(0, 9);
                final WritableRowSet r2 = RowSetFactory.fromRange(20, 29);
                final WritableRowSet r3 = RowSetFactory.fromKeys(42);
                final WritableRowSet e1 = r1.union(r2);
                final WritableRowSet expected = e1.union(r3);
                final WritableRowSet actual = RowSetFactory.unionInsert(List.of(r3, r2, r1))) {
            assertThat(actual).isEqualTo(expected);
        }
    }
}
