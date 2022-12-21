/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.arrow;

import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import dagger.multibindings.IntoSet;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.barrage.flatbuf.BarrageSnapshotRequest;
import io.deephaven.barrage.flatbuf.BarrageSubscriptionRequest;
import io.deephaven.extensions.barrage.BarrageSnapshotOptions;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.extensions.barrage.BarrageMessageProducer;
import io.deephaven.extensions.barrage.BarrageStreamGenerator;
import io.grpc.BindableService;
import io.grpc.stub.StreamObserver;

import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;

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
    static BarrageMessageProducer.StreamGenerator.Factory<BarrageStreamGenerator.View> bindStreamGenerator() {
        return new BarrageStreamGenerator.Factory();
    }

    @Provides
    static BarrageMessageProducer.Adapter<StreamObserver<InputStream>, StreamObserver<BarrageStreamGenerator.View>> provideListenerAdapter() {
        return delegate -> new StreamObserver<>() {
            @Override
            public void onNext(final BarrageStreamGenerator.View view) {
                try {
                    synchronized (delegate) {
                        view.forEachStream(delegate::onNext);
                    }
                } catch (final IOException ioe) {
                    throw new UncheckedDeephavenException(ioe);
                }
            }

            @Override
            public void onError(Throwable t) {
                synchronized (delegate) {
                    delegate.onError(t);
                }
            }

            @Override
            public void onCompleted() {
                synchronized (delegate) {
                    delegate.onCompleted();
                }
            }
        };
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
