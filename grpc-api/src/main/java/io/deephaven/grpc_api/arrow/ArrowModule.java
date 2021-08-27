package io.deephaven.grpc_api.arrow;

import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import dagger.multibindings.IntoSet;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.barrage.flatbuf.BarrageSubscriptionRequest;
import io.deephaven.grpc_api.barrage.BarrageMessageProducer;
import io.deephaven.grpc_api.barrage.BarrageStreamGenerator;
import io.deephaven.grpc_api_client.barrage.chunk.ChunkInputStreamGenerator;
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
    abstract BarrageMessageProducer.StreamGenerator.Factory<ChunkInputStreamGenerator.Options, BarrageStreamGenerator.View> bindStreamGenerator(
            BarrageStreamGenerator.Factory factory);

    @Provides
    static BarrageMessageProducer.Adapter<StreamObserver<InputStream>, StreamObserver<BarrageStreamGenerator.View>> provideListenerAdapter() {
        return delegate -> new StreamObserver<BarrageStreamGenerator.View>() {
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
    static BarrageMessageProducer.Adapter<BarrageSubscriptionRequest, ChunkInputStreamGenerator.Options> optionsAdapter() {
        return ChunkInputStreamGenerator.Options::of;
    }
}
