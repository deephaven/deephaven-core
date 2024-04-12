package io.deephaven.python.server;

import dagger.internal.DaggerGenerated;
import dagger.internal.Factory;
import dagger.internal.Preconditions;
import dagger.internal.QualifierMetadata;
import dagger.internal.ScopeMetadata;
import io.deephaven.io.log.LogSink;
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
public final class EmbeddedPyLogModule_ProvidesLogSinkFactory implements Factory<LogSink> {
  @Override
  public LogSink get() {
    return providesLogSink();
  }

  public static EmbeddedPyLogModule_ProvidesLogSinkFactory create() {
    return InstanceHolder.INSTANCE;
  }

  public static LogSink providesLogSink() {
    return Preconditions.checkNotNullFromProvides(EmbeddedPyLogModule.providesLogSink());
  }

  private static final class InstanceHolder {
    private static final EmbeddedPyLogModule_ProvidesLogSinkFactory INSTANCE = new EmbeddedPyLogModule_ProvidesLogSinkFactory();
  }
}
