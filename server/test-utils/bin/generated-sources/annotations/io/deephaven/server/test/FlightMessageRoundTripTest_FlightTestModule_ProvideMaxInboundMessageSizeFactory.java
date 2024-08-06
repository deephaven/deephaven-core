package io.deephaven.server.test;

import dagger.internal.DaggerGenerated;
import dagger.internal.Factory;
import dagger.internal.QualifierMetadata;
import dagger.internal.ScopeMetadata;
import javax.annotation.processing.Generated;

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
public final class FlightMessageRoundTripTest_FlightTestModule_ProvideMaxInboundMessageSizeFactory implements Factory<Integer> {
  private final FlightMessageRoundTripTest.FlightTestModule module;

  public FlightMessageRoundTripTest_FlightTestModule_ProvideMaxInboundMessageSizeFactory(
      FlightMessageRoundTripTest.FlightTestModule module) {
    this.module = module;
  }

  @Override
  public Integer get() {
    return provideMaxInboundMessageSize(module);
  }

  public static FlightMessageRoundTripTest_FlightTestModule_ProvideMaxInboundMessageSizeFactory create(
      FlightMessageRoundTripTest.FlightTestModule module) {
    return new FlightMessageRoundTripTest_FlightTestModule_ProvideMaxInboundMessageSizeFactory(module);
  }

  public static int provideMaxInboundMessageSize(
      FlightMessageRoundTripTest.FlightTestModule instance) {
    return instance.provideMaxInboundMessageSize();
  }
}
