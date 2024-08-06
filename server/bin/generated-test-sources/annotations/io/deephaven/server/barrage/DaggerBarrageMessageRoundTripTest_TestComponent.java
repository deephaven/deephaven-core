package io.deephaven.server.barrage;

import dagger.internal.DaggerGenerated;
import dagger.internal.DoubleCheck;
import dagger.internal.Preconditions;
import dagger.internal.Provider;
import io.deephaven.extensions.barrage.BarrageStreamGenerator;
import io.deephaven.server.arrow.ArrowModule_BindStreamGeneratorFactory;
import io.deephaven.server.util.Scheduler;
import javax.annotation.processing.Generated;

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
public final class DaggerBarrageMessageRoundTripTest_TestComponent {
  private DaggerBarrageMessageRoundTripTest_TestComponent() {
  }

  public static BarrageMessageRoundTripTest.TestComponent.Builder builder() {
    return new Builder();
  }

  private static final class Builder implements BarrageMessageRoundTripTest.TestComponent.Builder {
    private Scheduler withScheduler;

    @Override
    public Builder withScheduler(Scheduler scheduler) {
      this.withScheduler = Preconditions.checkNotNull(scheduler);
      return this;
    }

    @Override
    public BarrageMessageRoundTripTest.TestComponent build() {
      Preconditions.checkBuilderRequirement(withScheduler, Scheduler.class);
      return new TestComponentImpl(withScheduler);
    }
  }

  private static final class TestComponentImpl implements BarrageMessageRoundTripTest.TestComponent {
    private final TestComponentImpl testComponentImpl = this;

    private Provider<BarrageStreamGenerator.Factory> bindStreamGeneratorProvider;

    private TestComponentImpl(Scheduler withSchedulerParam) {

      initialize(withSchedulerParam);

    }

    @SuppressWarnings("unchecked")
    private void initialize(final Scheduler withSchedulerParam) {
      this.bindStreamGeneratorProvider = DoubleCheck.provider(ArrowModule_BindStreamGeneratorFactory.create());
    }

    @Override
    public BarrageStreamGenerator.Factory getStreamGeneratorFactory() {
      return bindStreamGeneratorProvider.get();
    }
  }
}
