package io.deephaven.server.runner;

import dagger.internal.DaggerGenerated;
import dagger.internal.Factory;
import dagger.internal.Preconditions;
import dagger.internal.QualifierMetadata;
import dagger.internal.ScopeMetadata;
import io.deephaven.engine.context.ExecutionContext;
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
    "rawtypes"
})
public final class ExecutionContextUnitTestModule_ProvideExecutionContextFactory implements Factory<ExecutionContext> {
  private final ExecutionContextUnitTestModule module;

  public ExecutionContextUnitTestModule_ProvideExecutionContextFactory(
      ExecutionContextUnitTestModule module) {
    this.module = module;
  }

  @Override
  public ExecutionContext get() {
    return provideExecutionContext(module);
  }

  public static ExecutionContextUnitTestModule_ProvideExecutionContextFactory create(
      ExecutionContextUnitTestModule module) {
    return new ExecutionContextUnitTestModule_ProvideExecutionContextFactory(module);
  }

  public static ExecutionContext provideExecutionContext(ExecutionContextUnitTestModule instance) {
    return Preconditions.checkNotNullFromProvides(instance.provideExecutionContext());
  }
}
