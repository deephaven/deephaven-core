package io.deephaven.server.netty;

import dagger.internal.DaggerGenerated;
import dagger.internal.Factory;
import dagger.internal.Preconditions;
import dagger.internal.QualifierMetadata;
import dagger.internal.ScopeMetadata;
import io.deephaven.server.runner.GrpcServer;
import io.grpc.BindableService;
import io.grpc.ServerInterceptor;
import java.util.Set;
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
    "rawtypes",
    "KotlinInternal",
    "KotlinInternalInJava",
    "cast"
})
public final class NettyServerModule_ServerBuilderFactory implements Factory<GrpcServer> {
  private final Provider<NettyConfig> serverConfigProvider;

  private final Provider<Set<BindableService>> servicesProvider;

  private final Provider<Set<ServerInterceptor>> interceptorsProvider;

  public NettyServerModule_ServerBuilderFactory(Provider<NettyConfig> serverConfigProvider,
      Provider<Set<BindableService>> servicesProvider,
      Provider<Set<ServerInterceptor>> interceptorsProvider) {
    this.serverConfigProvider = serverConfigProvider;
    this.servicesProvider = servicesProvider;
    this.interceptorsProvider = interceptorsProvider;
  }

  @Override
  public GrpcServer get() {
    return serverBuilder(serverConfigProvider.get(), servicesProvider.get(), interceptorsProvider.get());
  }

  public static NettyServerModule_ServerBuilderFactory create(
      Provider<NettyConfig> serverConfigProvider, Provider<Set<BindableService>> servicesProvider,
      Provider<Set<ServerInterceptor>> interceptorsProvider) {
    return new NettyServerModule_ServerBuilderFactory(serverConfigProvider, servicesProvider, interceptorsProvider);
  }

  public static GrpcServer serverBuilder(NettyConfig serverConfig, Set<BindableService> services,
      Set<ServerInterceptor> interceptors) {
    return Preconditions.checkNotNullFromProvides(NettyServerModule.serverBuilder(serverConfig, services, interceptors));
  }
}
