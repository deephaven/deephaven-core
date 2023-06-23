package io.deephaven.python.server;

import dagger.internal.DaggerGenerated;
import dagger.internal.Factory;
import dagger.internal.Preconditions;
import dagger.internal.QualifierMetadata;
import dagger.internal.ScopeMetadata;
import io.deephaven.internal.log.InitSink;
import java.util.Set;
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
    "rawtypes"
})
public final class EmbeddedPyLogModule_ProvidesLoggerSinkSetupsFactory implements Factory<Set<InitSink>> {
  @Override
  public Set<InitSink> get() {
    return providesLoggerSinkSetups();
  }

  public static EmbeddedPyLogModule_ProvidesLoggerSinkSetupsFactory create() {
    return InstanceHolder.INSTANCE;
  }

  public static Set<InitSink> providesLoggerSinkSetups() {
    return Preconditions.checkNotNullFromProvides(EmbeddedPyLogModule.providesLoggerSinkSetups());
  }

  private static final class InstanceHolder {
    private static final EmbeddedPyLogModule_ProvidesLoggerSinkSetupsFactory INSTANCE = new EmbeddedPyLogModule_ProvidesLoggerSinkSetupsFactory();
  }
}
