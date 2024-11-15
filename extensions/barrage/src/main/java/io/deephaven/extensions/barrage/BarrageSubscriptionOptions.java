//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage;

import com.google.flatbuffers.FlatBufferBuilder;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.barrage.flatbuf.BarrageSubscriptionRequest;
import io.deephaven.extensions.barrage.util.StreamReaderOptions;
import io.deephaven.util.annotations.FinalDefault;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

@Immutable
@BuildableStyle
public abstract class BarrageSubscriptionOptions implements StreamReaderOptions {

    public static Builder builder() {
        return ImmutableBarrageSubscriptionOptions.builder();
    }

    public static BarrageSubscriptionOptions of(final io.deephaven.barrage.flatbuf.BarrageSubscriptionOptions options) {
        if (options == null) {
            return builder().build();
        }
        return builder()
                .useDeephavenNulls(options.useDeephavenNulls())
                .minUpdateIntervalMs(options.minUpdateIntervalMs())
                .batchSize(options.batchSize())
                .maxMessageSize(options.maxMessageSize())
                .columnsAsList(options.columnsAsList())
                .previewListLengthLimit(options.previewListLengthLimit())
                .build();
    }

    public static BarrageSubscriptionOptions of(final BarrageSubscriptionRequest subscriptionRequest) {
        return of(subscriptionRequest.subscriptionOptions());
    }

    @Override
    @Default
    public boolean useDeephavenNulls() {
        return false;
    }

    @Override
    @Default
    public boolean columnsAsList() {
        return false;
    }

    /**
     * By default, we should not specify anything; the server will use whatever it is configured with. If multiple
     * subscriptions exist on a table (via the same client or via multiple clients) then the server will re-use state
     * needed to perform barrage-acrobatics for both of them. This greatly reduces the burden each client adds to the
     * server's workload. If a given table does want a shorter interval, consider using that shorter interval for all
     * subscriptions to that table.
     * <p>
     * The default interval can be set on the server with the flag
     * {@code io.deephaven.server.arrow.ArrowFlightUtil#DEFAULT_UPDATE_INTERVAL_MS}, or
     * {@code -Dbarrage.minUpdateInterval=1000}.
     * <p>
     * Related, when shortening the minUpdateInterval, you typically want to shorten the server's UGP cycle enough to
     * update at least as quickly. This can be done on the server with the flag
     * {@code io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph#defaultTargetCycleTime}, or
     * {@code -DPeriodicUpdateGraph.targetcycletime=1000}.
     *
     * @return the update interval to subscribe for
     */
    @Default
    public int minUpdateIntervalMs() {
        return 0;
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
        return io.deephaven.barrage.flatbuf.BarrageSubscriptionOptions.createBarrageSubscriptionOptions(builder,
                useDeephavenNulls(),
                minUpdateIntervalMs(),
                batchSize(),
                maxMessageSize(),
                columnsAsList(),
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
         * See {@link StreamReaderOptions#columnsAsList() } for details.
         *
         * @param columnsAsList whether to wrap columns in a list to be compatible with native Flight clients
         * @return this builder
         */
        Builder columnsAsList(boolean columnsAsList);

        /**
         * The default conversion mode is to Stringify all objects that do not have a registered encoding. Column
         * conversion modes are no longer supported.
         *
         * @deprecated Since 0.37.0 and is marked for removal. (Note, GWT does not support encoding this context via
         *             annotation values.)
         * @return this builder
         */
        @FinalDefault
        @Deprecated
        default Builder columnConversionMode(ColumnConversionMode columnConversionMode) {
            return this;
        }

        /**
         * See {@link BarrageSubscriptionOptions#minUpdateIntervalMs()} for details.
         *
         * @param minUpdateIntervalMs the update interval used to limit barrage message frequency
         * @return this builder
         */
        Builder minUpdateIntervalMs(int minUpdateIntervalMs);

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
         * @return a new BarrageSubscriptionOptions instance
         */
        BarrageSubscriptionOptions build();
    }
}
