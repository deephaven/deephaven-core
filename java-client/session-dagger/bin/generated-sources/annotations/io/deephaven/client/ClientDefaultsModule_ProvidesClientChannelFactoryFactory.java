package io.deephaven.client;

import dagger.internal.DaggerGenerated;
import dagger.internal.Factory;
import dagger.internal.Preconditions;
import dagger.internal.QualifierMetadata;
import dagger.internal.ScopeMetadata;
import io.deephaven.client.impl.ClientChannelFactory;
import javax.annotation.processing.Generated;

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
public final class ClientDefaultsModule_ProvidesClientChannelFactoryFactory implements Factory<ClientChannelFactory> {
  @Override
  public ClientChannelFactory get() {
    return providesClientChannelFactory();
  }

  public static ClientDefaultsModule_ProvidesClientChannelFactoryFactory create() {
    return InstanceHolder.INSTANCE;
  }

  public static ClientChannelFactory providesClientChannelFactory() {
    return Preconditions.checkNotNullFromProvides(ClientDefaultsModule.providesClientChannelFactory());
  }

  private static final class InstanceHolder {
    private static final ClientDefaultsModule_ProvidesClientChannelFactoryFactory INSTANCE = new ClientDefaultsModule_ProvidesClientChannelFactoryFactory();
  }
}
