package io.deephaven.server.jetty;

import dagger.internal.DaggerGenerated;
import dagger.internal.Factory;
import dagger.internal.Preconditions;
import dagger.internal.QualifierMetadata;
import dagger.internal.ScopeMetadata;
import io.grpc.BindableService;
import io.grpc.ServerInterceptor;
import io.grpc.servlet.jakarta.ServletAdapter;
import java.util.Set;
import javax.annotation.processing.Generated;
import javax.inject.Provider;

@ScopeMetadata
@QualifierMetadata("javax.inject.Named")
@DaggerGenerated
@Generated(
    value = "dagger.internal.codegen.ComponentProcessor",
    comments = "https://dagger.dev"
)
@SuppressWarnings({
    "unchecked",
    "rawtypes",
    "KotlinInternal",
    "KotlinInternalInJava",
    "cast"
})
public final class JettyServerModule_ProvideGrpcServletAdapterFactory implements Factory<ServletAdapter> {
  private final Provider<Integer> maxMessageSizeProvider;

  private final Provider<Set<BindableService>> servicesProvider;

  private final Provider<Set<ServerInterceptor>> interceptorsProvider;

  public JettyServerModule_ProvideGrpcServletAdapterFactory(
      Provider<Integer> maxMessageSizeProvider, Provider<Set<BindableService>> servicesProvider,
      Provider<Set<ServerInterceptor>> interceptorsProvider) {
    this.maxMessageSizeProvider = maxMessageSizeProvider;
    this.servicesProvider = servicesProvider;
    this.interceptorsProvider = interceptorsProvider;
  }

  @Override
  public ServletAdapter get() {
    return provideGrpcServletAdapter(maxMessageSizeProvider.get(), servicesProvider.get(), interceptorsProvider.get());
  }

  public static JettyServerModule_ProvideGrpcServletAdapterFactory create(
      Provider<Integer> maxMessageSizeProvider, Provider<Set<BindableService>> servicesProvider,
      Provider<Set<ServerInterceptor>> interceptorsProvider) {
    return new JettyServerModule_ProvideGrpcServletAdapterFactory(maxMessageSizeProvider, servicesProvider, interceptorsProvider);
  }

  public static ServletAdapter provideGrpcServletAdapter(int maxMessageSize,
      Set<BindableService> services, Set<ServerInterceptor> interceptors) {
    return Preconditions.checkNotNullFromProvides(JettyServerModule.provideGrpcServletAdapter(maxMessageSize, services, interceptors));
  }
}
