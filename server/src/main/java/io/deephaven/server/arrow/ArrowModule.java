//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.arrow;

import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import dagger.multibindings.IntoSet;
import io.deephaven.barrage.flatbuf.BarrageSnapshotRequest;
import io.deephaven.barrage.flatbuf.BarrageSubscriptionRequest;
import io.deephaven.extensions.barrage.BarrageSnapshotOptions;
import io.deephaven.extensions.barrage.BarrageMessageWriter;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.server.barrage.BarrageMessageProducer;
import io.deephaven.extensions.barrage.BarrageMessageWriterImpl;
import io.grpc.BindableService;

import javax.inject.Singleton;

@Module
public abstract class ArrowModule {
    @Binds
    @IntoSet
    abstract BindableService bindFlightServiceBinding(FlightServiceGrpcBinding service);

    @Binds
    @IntoSet
    abstract BindableService bindBrowserFlightServiceBinding(BrowserFlightServiceGrpcBinding service);

    @Provides
    @Singleton
    static BarrageMessageWriter.Factory bindStreamGenerator() {
        return new BarrageMessageWriterImpl.Factory();
    }

    @Provides
    static BarrageMessageProducer.Adapter<BarrageSubscriptionRequest, BarrageSubscriptionOptions> subscriptionOptAdapter() {
        return BarrageSubscriptionOptions::of;
    }

    @Provides
    static BarrageMessageProducer.Adapter<BarrageSnapshotRequest, BarrageSnapshotOptions> snapshotOptAdapter() {
        return BarrageSnapshotOptions::of;
    }
}
