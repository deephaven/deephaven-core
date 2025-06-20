//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.annotations.CopyableStyle;
import io.deephaven.util.compare.ObjectComparisons;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import javax.annotation.Nullable;

@Immutable
@CopyableStyle
abstract class MinMax<T extends Comparable<T>> {

    @Nullable
    abstract T min();

    @Nullable
    abstract T max();

    static <T extends Comparable<T>> MinMax<T> of(@Nullable T min, @Nullable T max) {
        return ImmutableMinMax.<T>builder()
                .min(min)
                .max(max)
                .build();
    }

    @Check
    final void checkOrder() {
        if (ObjectComparisons.compare(min(), max()) > 0) {
            throw new IllegalStateException(
                    "Min value cannot be greater than max value: " + min() + " > " + max());
        }
    }
}
