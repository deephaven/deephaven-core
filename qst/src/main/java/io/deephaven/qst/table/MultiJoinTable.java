//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.table;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.api.JoinAddition;
import io.deephaven.api.JoinMatch;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

import java.util.Collection;
import java.util.List;

@BuildableStyle
@Immutable
public abstract class MultiJoinTable extends TableBase {

    public static Builder builder() {
        return ImmutableMultiJoinTable.builder();
    }

    public static MultiJoinTable of(MultiJoinInput<TableSpec>... elements) {
        return builder().addInputs(elements).build();
    }

    public static MultiJoinTable of(Iterable<? extends MultiJoinInput<TableSpec>> elements) {
        return builder().addAllInputs(elements).build();
    }

    public static MultiJoinTable from(String columnsToMatch, TableSpec... inputs) {
        return of(inputs, JoinMatch.from(columnsToMatch.split(",")));
    }

    public static MultiJoinTable from(Collection<String> columnsToMatch, TableSpec... inputs) {
        return of(inputs, JoinMatch.from(columnsToMatch));
    }

    public abstract List<MultiJoinInput<TableSpec>> inputs();

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder {
        Builder addInputs(MultiJoinInput<TableSpec> element);

        Builder addInputs(MultiJoinInput<TableSpec>... elements);

        Builder addAllInputs(Iterable<? extends MultiJoinInput<TableSpec>> elements);

        MultiJoinTable build();
    }

    @Check
    final void checkInputs() {
        if (inputs().isEmpty()) {
            throw new IllegalArgumentException("MultiJoin inputs must be non-empty");
        }
    }

    @Check
    final void checkAdditions() {
        if (inputs().stream()
                .map(MultiJoinInput::additions)
                .flatMap(Collection::stream)
                .map(JoinAddition::newColumn)
                .distinct()
                .count() != inputs().stream()
                        .map(MultiJoinInput::additions)
                        .mapToLong(Collection::size)
                        .sum()) {
            throw new IllegalArgumentException(
                    "Invalid MultiJoin additions, must not use the same output column multiple times.");
        }
    }

    private static MultiJoinTable of(TableSpec[] inputs, List<JoinMatch> matches) {
        final Builder builder = builder();
        for (TableSpec input : inputs) {
            builder.addInputs(MultiJoinInput.<TableSpec>builder()
                    .table(input)
                    .addAllMatches(matches)
                    .build());
        }
        return builder.build();
    }
}
