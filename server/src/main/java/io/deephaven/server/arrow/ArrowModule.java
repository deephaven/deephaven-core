//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.arrow;

import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import dagger.multibindings.ElementsIntoSet;
import dagger.multibindings.IntoSet;
import io.deephaven.barrage.flatbuf.BarrageSnapshotRequest;
import io.deephaven.barrage.flatbuf.BarrageSubscriptionRequest;
import io.deephaven.extensions.barrage.BarrageSnapshotOptions;
import io.deephaven.extensions.barrage.BarrageMessageWriter;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.server.barrage.BarrageMessageProducer;
import io.deephaven.extensions.barrage.BarrageMessageWriterImpl;
import io.deephaven.server.session.ActionResolver;
import io.deephaven.server.session.TicketResolver;
import io.grpc.BindableService;

import javax.inject.Singleton;
import java.util.Set;

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
    @ElementsIntoSet
    static Set<TicketResolver> primesEmptyTicketResolvers() {
        return Set.of();
    }

    @Provides
    @ElementsIntoSet
    static Set<ActionResolver> primesEmptyActionResolvers() {
        return Set.of();
    }
}
