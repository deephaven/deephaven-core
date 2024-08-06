package io.deephaven.server.test;

import dagger.internal.DaggerGenerated;
import dagger.internal.Factory;
import dagger.internal.Preconditions;
import dagger.internal.QualifierMetadata;
import dagger.internal.ScopeMetadata;
import io.deephaven.server.util.Scheduler;
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
    "rawtypes",
    "KotlinInternal",
    "KotlinInternalInJava",
    "cast"
})
public final class FlightMessageRoundTripTest_FlightTestModule_ProvideSchedulerFactory implements Factory<Scheduler> {
  private final FlightMessageRoundTripTest.FlightTestModule module;

  public FlightMessageRoundTripTest_FlightTestModule_ProvideSchedulerFactory(
      FlightMessageRoundTripTest.FlightTestModule module) {
    this.module = module;
  }

  @Override
  public Scheduler get() {
    return provideScheduler(module);
  }

  public static FlightMessageRoundTripTest_FlightTestModule_ProvideSchedulerFactory create(
      FlightMessageRoundTripTest.FlightTestModule module) {
    return new FlightMessageRoundTripTest_FlightTestModule_ProvideSchedulerFactory(module);
  }

  public static Scheduler provideScheduler(FlightMessageRoundTripTest.FlightTestModule instance) {
    return Preconditions.checkNotNullFromProvides(instance.provideScheduler());
  }
}
