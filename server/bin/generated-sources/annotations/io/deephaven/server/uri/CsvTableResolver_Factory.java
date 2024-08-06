package io.deephaven.server.uri;

import dagger.internal.DaggerGenerated;
import dagger.internal.Factory;
import dagger.internal.QualifierMetadata;
import dagger.internal.ScopeMetadata;
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
    "rawtypes",
    "KotlinInternal",
    "KotlinInternalInJava",
    "cast"
})
public final class CsvTableResolver_Factory implements Factory<CsvTableResolver> {
  @Override
  public CsvTableResolver get() {
    return newInstance();
  }

  public static CsvTableResolver_Factory create() {
    return InstanceHolder.INSTANCE;
  }

  public static CsvTableResolver newInstance() {
    return new CsvTableResolver();
  }

  private static final class InstanceHolder {
    private static final CsvTableResolver_Factory INSTANCE = new CsvTableResolver_Factory();
  }
}
