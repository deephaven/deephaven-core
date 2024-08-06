package io.deephaven.client;

import dagger.internal.DaggerGenerated;
import dagger.internal.Factory;
import dagger.internal.Preconditions;
import dagger.internal.QualifierMetadata;
import dagger.internal.ScopeMetadata;
import io.deephaven.client.impl.SessionImplConfig;
import io.deephaven.proto.DeephavenChannel;
import java.util.concurrent.ScheduledExecutorService;
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
public final class SessionImplModule_ProvidesSessionImplConfigFactory implements Factory<SessionImplConfig> {
  private final Provider<DeephavenChannel> channelProvider;

  private final Provider<ScheduledExecutorService> schedulerProvider;

  private final Provider<String> authenticationTypeAndValueProvider;

  public SessionImplModule_ProvidesSessionImplConfigFactory(
      Provider<DeephavenChannel> channelProvider,
      Provider<ScheduledExecutorService> schedulerProvider,
      Provider<String> authenticationTypeAndValueProvider) {
    this.channelProvider = channelProvider;
    this.schedulerProvider = schedulerProvider;
    this.authenticationTypeAndValueProvider = authenticationTypeAndValueProvider;
  }

  @Override
  public SessionImplConfig get() {
    return providesSessionImplConfig(channelProvider.get(), schedulerProvider.get(), authenticationTypeAndValueProvider.get());
  }

  public static SessionImplModule_ProvidesSessionImplConfigFactory create(
      Provider<DeephavenChannel> channelProvider,
      Provider<ScheduledExecutorService> schedulerProvider,
      Provider<String> authenticationTypeAndValueProvider) {
    return new SessionImplModule_ProvidesSessionImplConfigFactory(channelProvider, schedulerProvider, authenticationTypeAndValueProvider);
  }

  public static SessionImplConfig providesSessionImplConfig(DeephavenChannel channel,
      ScheduledExecutorService scheduler, String authenticationTypeAndValue) {
    return Preconditions.checkNotNullFromProvides(SessionImplModule.providesSessionImplConfig(channel, scheduler, authenticationTypeAndValue));
  }
}
