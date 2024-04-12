package io.deephaven.python.server;

import dagger.internal.DaggerGenerated;
import dagger.internal.Factory;
import dagger.internal.Preconditions;
import dagger.internal.QualifierMetadata;
import dagger.internal.ScopeMetadata;
import io.deephaven.io.logger.LogBuffer;
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
public final class EmbeddedPyLogModule_ProvidesLogBufferFactory implements Factory<LogBuffer> {
  @Override
  public LogBuffer get() {
    return providesLogBuffer();
  }

  public static EmbeddedPyLogModule_ProvidesLogBufferFactory create() {
    return InstanceHolder.INSTANCE;
  }

  public static LogBuffer providesLogBuffer() {
    return Preconditions.checkNotNullFromProvides(EmbeddedPyLogModule.providesLogBuffer());
  }

  private static final class InstanceHolder {
    private static final EmbeddedPyLogModule_ProvidesLogBufferFactory INSTANCE = new EmbeddedPyLogModule_ProvidesLogBufferFactory();
  }
}
