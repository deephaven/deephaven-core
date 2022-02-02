/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.extensions.barrage;

import com.google.flatbuffers.FlatBufferBuilder;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.barrage.flatbuf.BarrageSnapshotRequest;
import io.deephaven.extensions.barrage.util.StreamReaderOptions;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

@Immutable
@BuildableStyle
public abstract class BarrageSnapshotOptions implements StreamReaderOptions {
    public static Builder builder() {
        return ImmutableBarrageSnapshotOptions.builder();
    }

    public static BarrageSnapshotOptions of(final io.deephaven.barrage.flatbuf.BarrageSnapshotOptions options) {
        if (options == null) {
            return builder().build();
        }
        final byte mode = options.columnConversionMode();
        return builder()
                .useDeephavenNulls(options.useDeephavenNulls())
                .columnConversionMode(ColumnConversionMode.conversionModeFbToEnum(mode))
                .batchSize(options.batchSize())
                .build();
    }

    public static BarrageSnapshotOptions of(final BarrageSnapshotRequest snapshotRequest) {
        return of(snapshotRequest.snapshotOptions());
    }

    /**
     * By default, prefer to communicate null values using the arrow-compatible validity structure.
     *
     * @return whether to use deephaven nulls
     */
    @Override
    @Default
    public boolean useDeephavenNulls() {
        return false;
    }

    /**
     * @return the preferred batch size if specified
     */
    @Default
    public int batchSize() {
        return 0;
    }

    @Override
    @Default
    public ColumnConversionMode columnConversionMode() {
        return ColumnConversionMode.Stringify;
    }

    public int appendTo(FlatBufferBuilder builder) {
        return io.deephaven.barrage.flatbuf.BarrageSnapshotOptions.createBarrageSnapshotOptions(
                builder, ColumnConversionMode.conversionModeEnumToFb(columnConversionMode()), useDeephavenNulls(),
                batchSize());
    }

    public interface Builder {

        Builder useDeephavenNulls(boolean useDeephavenNulls);

        Builder columnConversionMode(ColumnConversionMode columnConversionMode);

        Builder batchSize(int batchSize);

        BarrageSnapshotOptions build();
    }
}
