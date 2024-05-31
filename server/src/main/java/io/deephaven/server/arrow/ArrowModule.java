//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.arrow;

import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import dagger.multibindings.IntoSet;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.barrage.flatbuf.BarrageSnapshotRequest;
import io.deephaven.barrage.flatbuf.BarrageSubscriptionRequest;
import io.deephaven.extensions.barrage.BarrageSnapshotOptions;
import io.deephaven.extensions.barrage.BarrageStreamGenerator;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.server.barrage.BarrageMessageProducer;
import io.deephaven.extensions.barrage.BarrageStreamGeneratorImpl;
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

    @Binds
    @Singleton
    static BarrageStreamGenerator.Factory bindStreamGenerator() {
        return new BarrageStreamGeneratorImpl.Factory();
    }


    // TODO before commit, try getting rid of this
    @Provides
    static BarrageMessageProducer.Adapter<StreamObserver<InputStream>, StreamObserver<BarrageStreamGenerator.MessageView>> provideListenerAdapter() {
        return delegate -> new StreamObserver<>() {
            @Override
            public void onNext(final BarrageStreamGenerator.MessageView view) {
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
