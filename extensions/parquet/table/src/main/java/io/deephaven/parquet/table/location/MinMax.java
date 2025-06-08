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
abstract class MinMax {

    @Nullable
    abstract Object min();

    @Nullable
    abstract Object max();

    static ImmutableMinMax of(@Nullable Object min, @Nullable Object max) {
        return ImmutableMinMax.builder()
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
