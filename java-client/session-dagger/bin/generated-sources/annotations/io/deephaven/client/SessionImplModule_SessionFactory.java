package io.deephaven.client;

import dagger.internal.DaggerGenerated;
import dagger.internal.Factory;
import dagger.internal.Preconditions;
import dagger.internal.QualifierMetadata;
import dagger.internal.ScopeMetadata;
import io.deephaven.client.impl.SessionImpl;
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
    "rawtypes"
})
public final class SessionImplModule_SessionFactory implements Factory<SessionImpl> {
  private final Provider<DeephavenChannel> channelProvider;

  private final Provider<ScheduledExecutorService> schedulerProvider;

  private final Provider<String> authenticationTypeAndValueProvider;

  public SessionImplModule_SessionFactory(Provider<DeephavenChannel> channelProvider,
      Provider<ScheduledExecutorService> schedulerProvider,
      Provider<String> authenticationTypeAndValueProvider) {
    this.channelProvider = channelProvider;
    this.schedulerProvider = schedulerProvider;
    this.authenticationTypeAndValueProvider = authenticationTypeAndValueProvider;
  }

  @Override
  public SessionImpl get() {
    return session(channelProvider.get(), schedulerProvider.get(), authenticationTypeAndValueProvider.get());
  }

  public static SessionImplModule_SessionFactory create(Provider<DeephavenChannel> channelProvider,
      Provider<ScheduledExecutorService> schedulerProvider,
      Provider<String> authenticationTypeAndValueProvider) {
    return new SessionImplModule_SessionFactory(channelProvider, schedulerProvider, authenticationTypeAndValueProvider);
  }

  public static SessionImpl session(DeephavenChannel channel, ScheduledExecutorService scheduler,
      String authenticationTypeAndValue) {
    return Preconditions.checkNotNullFromProvides(SessionImplModule.session(channel, scheduler, authenticationTypeAndValue));
  }
}
