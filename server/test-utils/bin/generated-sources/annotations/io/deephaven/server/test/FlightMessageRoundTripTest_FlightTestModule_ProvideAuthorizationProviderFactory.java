package io.deephaven.server.test;

import dagger.internal.DaggerGenerated;
import dagger.internal.Factory;
import dagger.internal.Preconditions;
import dagger.internal.QualifierMetadata;
import dagger.internal.ScopeMetadata;
import io.deephaven.server.auth.AuthorizationProvider;
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
public final class FlightMessageRoundTripTest_FlightTestModule_ProvideAuthorizationProviderFactory implements Factory<AuthorizationProvider> {
  private final FlightMessageRoundTripTest.FlightTestModule module;

  private final Provider<TestAuthorizationProvider> providerProvider;

  public FlightMessageRoundTripTest_FlightTestModule_ProvideAuthorizationProviderFactory(
      FlightMessageRoundTripTest.FlightTestModule module,
      Provider<TestAuthorizationProvider> providerProvider) {
    this.module = module;
    this.providerProvider = providerProvider;
  }

  @Override
  public AuthorizationProvider get() {
    return provideAuthorizationProvider(module, providerProvider.get());
  }

  public static FlightMessageRoundTripTest_FlightTestModule_ProvideAuthorizationProviderFactory create(
      FlightMessageRoundTripTest.FlightTestModule module,
      Provider<TestAuthorizationProvider> providerProvider) {
    return new FlightMessageRoundTripTest_FlightTestModule_ProvideAuthorizationProviderFactory(module, providerProvider);
  }

  public static AuthorizationProvider provideAuthorizationProvider(
      FlightMessageRoundTripTest.FlightTestModule instance, TestAuthorizationProvider provider) {
    return Preconditions.checkNotNullFromProvides(instance.provideAuthorizationProvider(provider));
  }
}
