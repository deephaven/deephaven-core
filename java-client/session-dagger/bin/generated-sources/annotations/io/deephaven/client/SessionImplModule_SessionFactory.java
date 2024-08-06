package io.deephaven.client;

import dagger.internal.DaggerGenerated;
import dagger.internal.Factory;
import dagger.internal.Preconditions;
import dagger.internal.QualifierMetadata;
import dagger.internal.ScopeMetadata;
import io.deephaven.client.impl.SessionImpl;
import io.deephaven.client.impl.SessionImplConfig;
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
public final class SessionImplModule_SessionFactory implements Factory<SessionImpl> {
  private final Provider<SessionImplConfig> configProvider;

  public SessionImplModule_SessionFactory(Provider<SessionImplConfig> configProvider) {
    this.configProvider = configProvider;
  }

  @Override
  public SessionImpl get() {
    return session(configProvider.get());
  }

  public static SessionImplModule_SessionFactory create(
      Provider<SessionImplConfig> configProvider) {
    return new SessionImplModule_SessionFactory(configProvider);
  }

  public static SessionImpl session(SessionImplConfig config) {
    return Preconditions.checkNotNullFromProvides(SessionImplModule.session(config));
  }
}
