package io.deephaven.server.test;

import dagger.internal.DaggerGenerated;
import dagger.internal.Factory;
import dagger.internal.QualifierMetadata;
import dagger.internal.ScopeMetadata;
import java.util.concurrent.ScheduledExecutorService;
import javax.annotation.processing.Generated;
import org.jetbrains.annotations.Nullable;

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
public final class FlightMessageRoundTripTest_FlightTestModule_ProvideExecutorServiceFactory implements Factory<ScheduledExecutorService> {
  private final FlightMessageRoundTripTest.FlightTestModule module;

  public FlightMessageRoundTripTest_FlightTestModule_ProvideExecutorServiceFactory(
      FlightMessageRoundTripTest.FlightTestModule module) {
    this.module = module;
  }

  @Override
  @Nullable
  public ScheduledExecutorService get() {
    return provideExecutorService(module);
  }

  public static FlightMessageRoundTripTest_FlightTestModule_ProvideExecutorServiceFactory create(
      FlightMessageRoundTripTest.FlightTestModule module) {
    return new FlightMessageRoundTripTest_FlightTestModule_ProvideExecutorServiceFactory(module);
  }

  @Nullable
  public static ScheduledExecutorService provideExecutorService(
      FlightMessageRoundTripTest.FlightTestModule instance) {
    return instance.provideExecutorService();
  }
}
