//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.annotations.CopyableStyle;
import org.immutables.value.Value.Immutable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.VisibleForTesting;

@Immutable
@CopyableStyle
@VisibleForTesting
public abstract class MinMax<T extends Comparable<T>> {

    @NotNull
    public abstract T min();

    @NotNull
    public abstract T max();

    static <T extends Comparable<T>> MinMax<T> of(@NotNull final T min, @NotNull final T max) {
        return ImmutableMinMax.<T>builder()
                .min(min)
                .max(max)
                .build();
    }
}
