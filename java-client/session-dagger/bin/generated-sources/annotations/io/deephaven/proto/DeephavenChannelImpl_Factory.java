package io.deephaven.proto;

import dagger.internal.DaggerGenerated;
import dagger.internal.Factory;
import dagger.internal.QualifierMetadata;
import dagger.internal.ScopeMetadata;
import io.grpc.Channel;
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
public final class DeephavenChannelImpl_Factory implements Factory<DeephavenChannelImpl> {
  private final Provider<Channel> channelProvider;

  public DeephavenChannelImpl_Factory(Provider<Channel> channelProvider) {
    this.channelProvider = channelProvider;
  }

  @Override
  public DeephavenChannelImpl get() {
    return newInstance(channelProvider.get());
  }

  public static DeephavenChannelImpl_Factory create(Provider<Channel> channelProvider) {
    return new DeephavenChannelImpl_Factory(channelProvider);
  }

  public static DeephavenChannelImpl newInstance(Channel channel) {
    return new DeephavenChannelImpl(channel);
  }
}
