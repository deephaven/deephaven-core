//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage;

import com.google.flatbuffers.FlatBufferBuilder;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.barrage.flatbuf.BarrageSnapshotRequest;
import io.deephaven.extensions.barrage.util.StreamReaderOptions;
import io.deephaven.util.annotations.FinalDefault;
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
        return builder()
                .useDeephavenNulls(options.useDeephavenNulls())
                .batchSize(options.batchSize())
                .maxMessageSize(options.maxMessageSize())
                .previewListLengthLimit(options.previewListLengthLimit())
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
    @Override
    @Default
    public int batchSize() {
        return 0;
    }

    /**
     * @return the maximum GRPC message size if specified
     */
    @Override
    @Default
    public int maxMessageSize() {
        return 0;
    }

    @Override
    @Default
    public int previewListLengthLimit() {
        return 0;
    }

    public int appendTo(FlatBufferBuilder builder) {
        return io.deephaven.barrage.flatbuf.BarrageSnapshotOptions.createBarrageSnapshotOptions(
                builder, useDeephavenNulls(),
                batchSize(),
                maxMessageSize(),
                previewListLengthLimit());
    }

    public interface Builder {

        Builder useDeephavenNulls(boolean useDeephavenNulls);

        /**
         * Deprecated since 0.37.0 and is marked for removal. (our GWT artifacts do not yet support the attributes)
         */
        @FinalDefault
        @Deprecated
        default Builder columnConversionMode(ColumnConversionMode columnConversionMode) {
            return this;
        }

        Builder batchSize(int batchSize);

        Builder maxMessageSize(int messageSize);

        Builder previewListLengthLimit(int previewListLengthLimit);

        BarrageSnapshotOptions build();
    }
}
