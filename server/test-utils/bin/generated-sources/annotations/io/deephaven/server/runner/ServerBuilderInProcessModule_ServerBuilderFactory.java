package io.deephaven.server.runner;

import dagger.internal.DaggerGenerated;
import dagger.internal.Factory;
import dagger.internal.Preconditions;
import dagger.internal.QualifierMetadata;
import dagger.internal.ScopeMetadata;
import io.grpc.BindableService;
import io.grpc.ServerInterceptor;
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
public final class ServerBuilderInProcessModule_ServerBuilderFactory implements Factory<GrpcServer> {
  private final Provider<String> serverNameProvider;

  private final Provider<Set<BindableService>> servicesProvider;

  private final Provider<Set<ServerInterceptor>> interceptorsProvider;

  public ServerBuilderInProcessModule_ServerBuilderFactory(Provider<String> serverNameProvider,
      Provider<Set<BindableService>> servicesProvider,
      Provider<Set<ServerInterceptor>> interceptorsProvider) {
    this.serverNameProvider = serverNameProvider;
    this.servicesProvider = servicesProvider;
    this.interceptorsProvider = interceptorsProvider;
  }

  @Override
  public GrpcServer get() {
    return serverBuilder(serverNameProvider.get(), servicesProvider.get(), interceptorsProvider.get());
  }

  public static ServerBuilderInProcessModule_ServerBuilderFactory create(
      Provider<String> serverNameProvider, Provider<Set<BindableService>> servicesProvider,
      Provider<Set<ServerInterceptor>> interceptorsProvider) {
    return new ServerBuilderInProcessModule_ServerBuilderFactory(serverNameProvider, servicesProvider, interceptorsProvider);
  }

  public static GrpcServer serverBuilder(String serverName, Set<BindableService> services,
      Set<ServerInterceptor> interceptors) {
    return Preconditions.checkNotNullFromProvides(ServerBuilderInProcessModule.serverBuilder(serverName, services, interceptors));
  }
}
