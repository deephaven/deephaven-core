package io.deephaven.python.server;

import dagger.internal.DaggerGenerated;
import dagger.internal.Factory;
import dagger.internal.Preconditions;
import dagger.internal.QualifierMetadata;
import dagger.internal.ScopeMetadata;
import io.deephaven.base.system.StandardStreamState;
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
public final class EmbeddedPyLogModule_ProvidesStandardStreamStateFactory implements Factory<StandardStreamState> {
  @Override
  public StandardStreamState get() {
    return providesStandardStreamState();
  }

  public static EmbeddedPyLogModule_ProvidesStandardStreamStateFactory create() {
    return InstanceHolder.INSTANCE;
  }

  public static StandardStreamState providesStandardStreamState() {
    return Preconditions.checkNotNullFromProvides(EmbeddedPyLogModule.providesStandardStreamState());
  }

  private static final class InstanceHolder {
    private static final EmbeddedPyLogModule_ProvidesStandardStreamStateFactory INSTANCE = new EmbeddedPyLogModule_ProvidesStandardStreamStateFactory();
  }
}
