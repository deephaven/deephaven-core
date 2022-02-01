package io.deephaven.extensions.barrage.util;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.extensions.barrage.BarrageSnapshotOptions;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.extensions.barrage.ColumnConversionMode;
import org.immutables.value.Value;

public abstract class StreamReaderOptions {
    @Value.Parameter
    public abstract boolean useDeephavenNulls();

    @Value.Parameter
    public abstract ColumnConversionMode columnConversionMode();
}
