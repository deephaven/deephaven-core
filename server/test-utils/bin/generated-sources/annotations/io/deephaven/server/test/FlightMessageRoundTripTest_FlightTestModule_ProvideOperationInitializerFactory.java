package io.deephaven.server.test;

import dagger.internal.DaggerGenerated;
import dagger.internal.Factory;
import dagger.internal.Preconditions;
import dagger.internal.QualifierMetadata;
import dagger.internal.ScopeMetadata;
import io.deephaven.engine.updategraph.OperationInitializer;
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
public final class FlightMessageRoundTripTest_FlightTestModule_ProvideOperationInitializerFactory implements Factory<OperationInitializer> {
  @Override
  public OperationInitializer get() {
    return provideOperationInitializer();
  }

  public static FlightMessageRoundTripTest_FlightTestModule_ProvideOperationInitializerFactory create(
      ) {
    return InstanceHolder.INSTANCE;
  }

  public static OperationInitializer provideOperationInitializer() {
    return Preconditions.checkNotNullFromProvides(FlightMessageRoundTripTest.FlightTestModule.provideOperationInitializer());
  }

  private static final class InstanceHolder {
    private static final FlightMessageRoundTripTest_FlightTestModule_ProvideOperationInitializerFactory INSTANCE = new FlightMessageRoundTripTest_FlightTestModule_ProvideOperationInitializerFactory();
  }
}
