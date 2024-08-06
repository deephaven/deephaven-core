package io.deephaven.server.runner;

import dagger.internal.DaggerGenerated;
import dagger.internal.Factory;
import dagger.internal.Preconditions;
import dagger.internal.QualifierMetadata;
import dagger.internal.ScopeMetadata;
import io.grpc.ManagedChannelBuilder;
import javax.annotation.processing.Generated;
import javax.inject.Provider;

@ScopeMetadata("dagger.Reusable")
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
public final class ServerBuilderInProcessModule_ChannelBuilderFactory implements Factory<ManagedChannelBuilder<?>> {
  private final Provider<String> serverNameProvider;

  public ServerBuilderInProcessModule_ChannelBuilderFactory(Provider<String> serverNameProvider) {
    this.serverNameProvider = serverNameProvider;
  }

  @Override
  public ManagedChannelBuilder<?> get() {
    return channelBuilder(serverNameProvider.get());
  }

  public static ServerBuilderInProcessModule_ChannelBuilderFactory create(
      Provider<String> serverNameProvider) {
    return new ServerBuilderInProcessModule_ChannelBuilderFactory(serverNameProvider);
  }

  public static ManagedChannelBuilder<?> channelBuilder(String serverName) {
    return Preconditions.checkNotNullFromProvides(ServerBuilderInProcessModule.channelBuilder(serverName));
  }
}
