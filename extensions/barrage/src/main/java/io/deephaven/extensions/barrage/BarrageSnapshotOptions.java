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

    @Override
    @Default
    public boolean useDeephavenNulls() {
        return false;
    }

    @Override
    @Default
    public int batchSize() {
        return 0;
    }

    @Override
    @Default
    public int maxMessageSize() {
        return 0;
    }

    @Override
    @Default
    public long previewListLengthLimit() {
        return 0;
    }

    public int appendTo(FlatBufferBuilder builder) {
        return io.deephaven.barrage.flatbuf.BarrageSnapshotOptions.createBarrageSnapshotOptions(builder,
                useDeephavenNulls(),
                batchSize(),
                maxMessageSize(),
                previewListLengthLimit());
    }

    public interface Builder {

        /**
         * See {@link StreamReaderOptions#useDeephavenNulls()} for details.
         *
         * @param useDeephavenNulls whether to use deephaven nulls
         * @return this builder
         */
        Builder useDeephavenNulls(boolean useDeephavenNulls);

        /**
         * The default conversion mode is to Stringify all objects that do not have a registered encoding. Column
         * conversion modes are no longer supported.
         *
         * @deprecated Since 0.37.0 and is marked for removal. (Note, GWT does not support encoding this context via
         *             annotation values.)
         */
        @FinalDefault
        @Deprecated
        default Builder columnConversionMode(ColumnConversionMode columnConversionMode) {
            return this;
        }

        /**
         * See {@link StreamReaderOptions#batchSize()} for details.
         *
         * @param batchSize the ideal number of records to send per record batch
         * @return this builder
         */
        Builder batchSize(int batchSize);

        /**
         * See {@link StreamReaderOptions#maxMessageSize()} for details.
         *
         * @param messageSize the maximum size of a GRPC message in bytes
         * @return this builder
         */
        Builder maxMessageSize(int messageSize);

        /**
         * See {@link StreamReaderOptions#previewListLengthLimit()} for details.
         *
         * @param previewListLengthLimit the magnitude of the number of elements to include in a preview list
         * @return this builder
         */
        Builder previewListLengthLimit(long previewListLengthLimit);

        /**
         * @return a new BarrageSnapshotOptions instance
         */
        BarrageSnapshotOptions build();
    }
}
