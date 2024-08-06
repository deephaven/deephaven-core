package io.deephaven.server.test;

import dagger.internal.DaggerGenerated;
import dagger.internal.Factory;
import dagger.internal.Preconditions;
import dagger.internal.QualifierMetadata;
import dagger.internal.ScopeMetadata;
import io.deephaven.engine.updategraph.UpdateGraph;
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
public final class FlightMessageRoundTripTest_FlightTestModule_ProvideUpdateGraphFactory implements Factory<UpdateGraph> {
  @Override
  public UpdateGraph get() {
    return provideUpdateGraph();
  }

  public static FlightMessageRoundTripTest_FlightTestModule_ProvideUpdateGraphFactory create() {
    return InstanceHolder.INSTANCE;
  }

  public static UpdateGraph provideUpdateGraph() {
    return Preconditions.checkNotNullFromProvides(FlightMessageRoundTripTest.FlightTestModule.provideUpdateGraph());
  }

  private static final class InstanceHolder {
    private static final FlightMessageRoundTripTest_FlightTestModule_ProvideUpdateGraphFactory INSTANCE = new FlightMessageRoundTripTest_FlightTestModule_ProvideUpdateGraphFactory();
  }
}
