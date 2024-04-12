package io.deephaven.server.jetty;

import dagger.internal.DaggerGenerated;
import dagger.internal.Factory;
import dagger.internal.QualifierMetadata;
import dagger.internal.ScopeMetadata;
import io.grpc.servlet.jakarta.ServletAdapter;
import javax.annotation.processing.Generated;
import javax.inject.Provider;

@ScopeMetadata
@QualifierMetadata
@DaggerGenerated
@Generated(
    value = "dagger.internal.codegen.ComponentProcessor",
    comments = "https://dagger.dev"
)
@SuppressWarnings({
    "unchecked",
    "rawtypes"
})
public final class GrpcFilter_Factory implements Factory<GrpcFilter> {
  private final Provider<ServletAdapter> grpcAdapterProvider;

  public GrpcFilter_Factory(Provider<ServletAdapter> grpcAdapterProvider) {
    this.grpcAdapterProvider = grpcAdapterProvider;
  }

  @Override
  public GrpcFilter get() {
    return newInstance(grpcAdapterProvider.get());
  }

  public static GrpcFilter_Factory create(Provider<ServletAdapter> grpcAdapterProvider) {
    return new GrpcFilter_Factory(grpcAdapterProvider);
  }

  public static GrpcFilter newInstance(ServletAdapter grpcAdapter) {
    return new GrpcFilter(grpcAdapter);
  }
}
