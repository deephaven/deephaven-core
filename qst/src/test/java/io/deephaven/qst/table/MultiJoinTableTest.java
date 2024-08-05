//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.table;

import io.deephaven.api.ColumnName;
import io.deephaven.api.JoinAddition;
import io.deephaven.api.JoinMatch;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class MultiJoinTableTest {

    @Test
    void inputMissingTable() {
        try {
            MultiJoinInput.<TableSpec>builder().build();
            failBecauseExceptionWasNotThrown(IllegalStateException.class);
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("not set [table]");
        }
    }

    @Test
    void inputOverlappingAdditions() {
        try {
            MultiJoinInput.<TableSpec>builder()
                    .table(TableSpec.empty(1))
                    .addAdditions(ColumnName.of("Foo"))
                    .addAdditions(ColumnName.of("Foo"))
                    .build();
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("must not use the same output column multiple times");
        }
    }

    @Test
    void input() {
        final MultiJoinInput<TableSpec> input = MultiJoinInput.of(
                TableSpec.empty(1),
                "Foo,Bar=Baz",
                "Bap,Bom=Blip");
        assertThat(input.table()).isEqualTo(TableSpec.empty(1));
        assertThat(input.matches()).containsExactly(
                ColumnName.of("Foo"),
                JoinMatch.of(ColumnName.of("Bar"), ColumnName.of("Baz")));
        assertThat(input.additions()).containsExactly(
                ColumnName.of("Bap"),
                JoinAddition.of(ColumnName.of("Bom"), ColumnName.of("Blip")));
    }

    @Test
    void emptyInputs() {
        for (Supplier<MultiJoinTable> supplier : Arrays.<Supplier<MultiJoinTable>>asList(
                () -> MultiJoinTable.builder().build(),
                MultiJoinTable::of,
                () -> MultiJoinTable.from("Key"),
                () -> MultiJoinTable.from(List.of("Key1", "Key2")))) {
            try {
                supplier.get();
                failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
            } catch (IllegalArgumentException e) {
                assertThat(e).hasMessageContaining("MultiJoin inputs must be non-empty");
            }
        }
    }

    @Test
    void overlappingAdditions() {
        try {
            MultiJoinTable.of(
                    MultiJoinInput.<TableSpec>builder()
                            .table(TableSpec.empty(1))
                            .addAdditions(ColumnName.of("Foo"))
                            .build(),
                    MultiJoinInput.<TableSpec>builder()
                            .table(TableSpec.empty(1))
                            .addAdditions(ColumnName.of("Foo"))
                            .build());
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("must not use the same output column multiple times");
        }
    }

    @Test
    void keyHelpers() {
        for (MultiJoinTable multiJoinTable : Arrays.asList(
                MultiJoinTable.from(
                        "Key,Key2=Key3",
                        TableSpec.empty(1),
                        TableSpec.empty(2)),
                MultiJoinTable.from(
                        List.of("Key", "Key2=Key3"),
                        TableSpec.empty(1),
                        TableSpec.empty(2)))) {
            assertThat(multiJoinTable.inputs()).containsExactly(
                    MultiJoinInput.<TableSpec>builder()
                            .table(TableSpec.empty(1))
                            .addMatches(
                                    ColumnName.of("Key"),
                                    JoinMatch.of(ColumnName.of("Key2"), ColumnName.of("Key3")))
                            .build(),
                    MultiJoinInput.<TableSpec>builder()
                            .table(TableSpec.empty(2))
                            .addMatches(
                                    ColumnName.of("Key"),
                                    JoinMatch.of(ColumnName.of("Key2"), ColumnName.of("Key3")))
                            .build());
        }
    }

    @Test
    void multiJoinTable() {
        final MultiJoinInput<TableSpec> i1 = MultiJoinInput.of(TableSpec.empty(1), "Foo", "Bar");
        final MultiJoinInput<TableSpec> i2 = MultiJoinInput.of(TableSpec.empty(2), "Baz", "Bap");
        for (MultiJoinTable mjt : Arrays.asList(
                MultiJoinTable.builder().addInputs(i1).addInputs(i2).build(),
                MultiJoinTable.of(i1, i2),
                MultiJoinTable.of(List.of(i1, i2)))) {
            assertThat(mjt.inputs()).containsExactly(i1, i2);
        }
    }
}
