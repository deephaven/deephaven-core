package io.deephaven.server.test;

import dagger.internal.DaggerGenerated;
import dagger.internal.Factory;
import dagger.internal.Preconditions;
import dagger.internal.QualifierMetadata;
import dagger.internal.ScopeMetadata;
import io.deephaven.engine.util.AbstractScriptSession;
import io.deephaven.engine.util.ScriptSession;
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
public final class FlightMessageRoundTripTest_FlightTestModule_ProvideScriptSessionFactory implements Factory<ScriptSession> {
  private final FlightMessageRoundTripTest.FlightTestModule module;

  private final Provider<AbstractScriptSession<?>> scriptSessionProvider;

  public FlightMessageRoundTripTest_FlightTestModule_ProvideScriptSessionFactory(
      FlightMessageRoundTripTest.FlightTestModule module,
      Provider<AbstractScriptSession<?>> scriptSessionProvider) {
    this.module = module;
    this.scriptSessionProvider = scriptSessionProvider;
  }

  @Override
  public ScriptSession get() {
    return provideScriptSession(module, scriptSessionProvider.get());
  }

  public static FlightMessageRoundTripTest_FlightTestModule_ProvideScriptSessionFactory create(
      FlightMessageRoundTripTest.FlightTestModule module,
      Provider<AbstractScriptSession<?>> scriptSessionProvider) {
    return new FlightMessageRoundTripTest_FlightTestModule_ProvideScriptSessionFactory(module, scriptSessionProvider);
  }

  public static ScriptSession provideScriptSession(
      FlightMessageRoundTripTest.FlightTestModule instance,
      AbstractScriptSession<?> scriptSession) {
    return Preconditions.checkNotNullFromProvides(instance.provideScriptSession(scriptSession));
  }
}
