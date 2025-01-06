//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.table;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.api.JoinAddition;
import io.deephaven.api.JoinMatch;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

import java.util.List;

@Immutable
@BuildableStyle
public abstract class MultiJoinInput<T> {

    public static <T> Builder<T> builder() {
        return ImmutableMultiJoinInput.builder();
    }

    public static <T> MultiJoinInput<T> of(T table, String columnsToMatch, String columnsToAdd) {
        final Builder<T> builder = builder();
        builder.table(table);
        if (columnsToMatch != null) {
            builder.addAllMatches(JoinMatch.from(columnsToMatch.split(",")));
        }
        if (columnsToAdd != null) {
            builder.addAllAdditions(JoinAddition.from(columnsToAdd.split(",")));
        }
        return builder.build();
    }

    public abstract T table();

    public abstract List<JoinMatch> matches();

    public abstract List<JoinAddition> additions();

    @Check
    final void checkAdditions() {
        if (additions().stream().map(JoinAddition::newColumn).distinct().count() != additions()
                .size()) {
            throw new IllegalArgumentException(
                    "Invalid MultiJoinInput additions, must not use the same output column multiple times.");
        }
    }

    public interface Builder<T> {

        Builder<T> table(T table);

        Builder<T> addMatches(JoinMatch element);

        Builder<T> addMatches(JoinMatch... elements);

        Builder<T> addAllMatches(Iterable<? extends JoinMatch> elements);

        Builder<T> addAdditions(JoinAddition element);

        Builder<T> addAdditions(JoinAddition... elements);

        Builder<T> addAllAdditions(Iterable<? extends JoinAddition> elements);

        MultiJoinInput<T> build();
    }
}
