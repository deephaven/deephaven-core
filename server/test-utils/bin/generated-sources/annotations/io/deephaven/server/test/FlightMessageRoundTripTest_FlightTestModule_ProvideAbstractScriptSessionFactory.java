package io.deephaven.server.test;

import dagger.internal.DaggerGenerated;
import dagger.internal.Factory;
import dagger.internal.Preconditions;
import dagger.internal.QualifierMetadata;
import dagger.internal.ScopeMetadata;
import io.deephaven.engine.updategraph.OperationInitializer;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.util.AbstractScriptSession;
import javax.annotation.processing.Generated;
import javax.inject.Provider;

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
public final class FlightMessageRoundTripTest_FlightTestModule_ProvideAbstractScriptSessionFactory implements Factory<AbstractScriptSession<?>> {
  private final FlightMessageRoundTripTest.FlightTestModule module;

  private final Provider<UpdateGraph> updateGraphProvider;

  private final Provider<OperationInitializer> operationInitializerProvider;

  public FlightMessageRoundTripTest_FlightTestModule_ProvideAbstractScriptSessionFactory(
      FlightMessageRoundTripTest.FlightTestModule module, Provider<UpdateGraph> updateGraphProvider,
      Provider<OperationInitializer> operationInitializerProvider) {
    this.module = module;
    this.updateGraphProvider = updateGraphProvider;
    this.operationInitializerProvider = operationInitializerProvider;
  }

  @Override
  public AbstractScriptSession<?> get() {
    return provideAbstractScriptSession(module, updateGraphProvider.get(), operationInitializerProvider.get());
  }

  public static FlightMessageRoundTripTest_FlightTestModule_ProvideAbstractScriptSessionFactory create(
      FlightMessageRoundTripTest.FlightTestModule module, Provider<UpdateGraph> updateGraphProvider,
      Provider<OperationInitializer> operationInitializerProvider) {
    return new FlightMessageRoundTripTest_FlightTestModule_ProvideAbstractScriptSessionFactory(module, updateGraphProvider, operationInitializerProvider);
  }

  public static AbstractScriptSession<?> provideAbstractScriptSession(
      FlightMessageRoundTripTest.FlightTestModule instance, UpdateGraph updateGraph,
      OperationInitializer operationInitializer) {
    return Preconditions.checkNotNullFromProvides(instance.provideAbstractScriptSession(updateGraph, operationInitializer));
  }
}
