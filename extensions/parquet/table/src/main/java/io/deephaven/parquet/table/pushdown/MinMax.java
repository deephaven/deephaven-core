//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pushdown;

import io.deephaven.annotations.CopyableStyle;
import org.immutables.value.Value.Immutable;
import org.jetbrains.annotations.NotNull;

@Immutable
@CopyableStyle
abstract class MinMax<T extends Comparable<T>> {

    @NotNull
    abstract T min();

    @NotNull
    abstract T max();

    static <T extends Comparable<T>> MinMax<T> of(@NotNull final T min, @NotNull final T max) {
        return ImmutableMinMax.<T>builder()
                .min(min)
                .max(max)
                .build();
    }
}
