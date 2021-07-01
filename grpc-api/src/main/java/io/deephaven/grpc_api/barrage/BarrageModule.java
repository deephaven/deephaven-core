package io.deephaven.grpc_api.barrage;

import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import dagger.multibindings.IntoSet;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.grpc_api.util.GrpcUtil;
import io.deephaven.grpc_api_client.barrage.chunk.ChunkInputStreamGenerator;
import io.deephaven.proto.backplane.grpc.SubscriptionRequest;
import io.grpc.BindableService;
import io.grpc.stub.StreamObserver;

import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;

@Module
public abstract class BarrageModule {
    @Binds @IntoSet
    public abstract BindableService bindBarrageServiceBinding(BarrageServiceGrpcBinding<ChunkInputStreamGenerator.Options, BarrageStreamGenerator.View> service);

    @Binds @Singleton
    public abstract BarrageMessageProducer.StreamGenerator.Factory<ChunkInputStreamGenerator.Options, BarrageStreamGenerator.View> bindStreamGenerator(BarrageStreamGenerator.Factory factory);

    @Provides
    public static BarrageMessageProducer.Adapter<StreamObserver<InputStream>, StreamObserver<BarrageStreamGenerator.View>> provideListenerAdapter() {
        return delegate -> GrpcUtil.mapOnNext(delegate, (view) -> {
            try {
                return view.getInputStream();
            } catch (final IOException ioe) {
                throw new UncheckedDeephavenException(ioe);
            }
        });
    }

    @Provides
    public static BarrageMessageProducer.Adapter<SubscriptionRequest, ChunkInputStreamGenerator.Options> optionsAdapter() {
        return subscriptionRequest -> new ChunkInputStreamGenerator.Options.Builder()
                .setIsViewport(!subscriptionRequest.getViewport().isEmpty())
                .setUseDeephavenNulls(subscriptionRequest.getUseDeephavenNulls())
                .build();
    }
}
