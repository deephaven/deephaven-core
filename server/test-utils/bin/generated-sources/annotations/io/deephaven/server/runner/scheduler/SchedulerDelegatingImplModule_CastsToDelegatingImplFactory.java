package io.deephaven.server.runner.scheduler;

import dagger.internal.DaggerGenerated;
import dagger.internal.Factory;
import dagger.internal.Preconditions;
import dagger.internal.QualifierMetadata;
import dagger.internal.ScopeMetadata;
import io.deephaven.server.util.Scheduler;
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
public final class SchedulerDelegatingImplModule_CastsToDelegatingImplFactory implements Factory<Scheduler.DelegatingImpl> {
  private final Provider<Scheduler> schedulerProvider;

  public SchedulerDelegatingImplModule_CastsToDelegatingImplFactory(
      Provider<Scheduler> schedulerProvider) {
    this.schedulerProvider = schedulerProvider;
  }

  @Override
  public Scheduler.DelegatingImpl get() {
    return castsToDelegatingImpl(schedulerProvider.get());
  }

  public static SchedulerDelegatingImplModule_CastsToDelegatingImplFactory create(
      Provider<Scheduler> schedulerProvider) {
    return new SchedulerDelegatingImplModule_CastsToDelegatingImplFactory(schedulerProvider);
  }

  public static Scheduler.DelegatingImpl castsToDelegatingImpl(Scheduler scheduler) {
    return Preconditions.checkNotNullFromProvides(SchedulerDelegatingImplModule.castsToDelegatingImpl(scheduler));
  }
}
