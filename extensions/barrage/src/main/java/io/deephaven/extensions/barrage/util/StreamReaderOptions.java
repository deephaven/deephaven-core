package io.deephaven.extensions.barrage.util;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.extensions.barrage.BarrageSnapshotOptions;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.extensions.barrage.ColumnConversionMode;
import org.immutables.value.Value;

@Value.Immutable
@SimpleStyle
public abstract class StreamReaderOptions {
    public static StreamReaderOptions of(BarrageSnapshotOptions options) {
        return ImmutableStreamReaderOptions.of(options.useDeephavenNulls(), options.columnConversionMode());
    }

    public static StreamReaderOptions of(BarrageSubscriptionOptions options) {
        return ImmutableStreamReaderOptions.of(options.useDeephavenNulls(), options.columnConversionMode());
    }

    @Value.Parameter
    public abstract boolean useDeephavenNulls();

    @Value.Parameter
    public abstract ColumnConversionMode columnConversionMode();
}
