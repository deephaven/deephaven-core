package io.deephaven.server.test;

import dagger.internal.DaggerGenerated;
import dagger.internal.Factory;
import dagger.internal.Preconditions;
import dagger.internal.QualifierMetadata;
import dagger.internal.ScopeMetadata;
import javax.annotation.processing.Generated;

@ScopeMetadata("javax.inject.Singleton")
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
public final class FlightMessageRoundTripTest_FlightTestModule_ProvideTestAuthorizationProviderFactory implements Factory<TestAuthorizationProvider> {
  private final FlightMessageRoundTripTest.FlightTestModule module;

  public FlightMessageRoundTripTest_FlightTestModule_ProvideTestAuthorizationProviderFactory(
      FlightMessageRoundTripTest.FlightTestModule module) {
    this.module = module;
  }

  @Override
  public TestAuthorizationProvider get() {
    return provideTestAuthorizationProvider(module);
  }

  public static FlightMessageRoundTripTest_FlightTestModule_ProvideTestAuthorizationProviderFactory create(
      FlightMessageRoundTripTest.FlightTestModule module) {
    return new FlightMessageRoundTripTest_FlightTestModule_ProvideTestAuthorizationProviderFactory(module);
  }

  public static TestAuthorizationProvider provideTestAuthorizationProvider(
      FlightMessageRoundTripTest.FlightTestModule instance) {
    return Preconditions.checkNotNullFromProvides(instance.provideTestAuthorizationProvider());
  }
}
