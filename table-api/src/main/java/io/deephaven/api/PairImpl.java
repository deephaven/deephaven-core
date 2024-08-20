//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;


@Immutable
@SimpleStyle
abstract class PairImpl implements Pair {
    public static PairImpl of(ColumnName input, ColumnName output) {
        return ImmutablePairImpl.of(input, output);
    }

    @Parameter
    public abstract ColumnName input();

    @Parameter
    public abstract ColumnName output();

    @Check
    final void checkNotSameColumns() {
        if (input().equals(output())) {
            // To make sure that Pair#equals() works as we would expect, we should always use
            // canonical ColumnName when applicable.
            throw new IllegalArgumentException(
                    "Should not construct PairImpl with the same columns, use the ColumnName directly");
        }
    }
}
