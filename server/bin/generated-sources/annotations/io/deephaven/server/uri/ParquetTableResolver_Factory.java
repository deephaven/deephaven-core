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
public final class ParquetTableResolver_Factory implements Factory<ParquetTableResolver> {
  @Override
  public ParquetTableResolver get() {
    return newInstance();
  }

  public static ParquetTableResolver_Factory create() {
    return InstanceHolder.INSTANCE;
  }

  public static ParquetTableResolver newInstance() {
    return new ParquetTableResolver();
  }

  private static final class InstanceHolder {
    private static final ParquetTableResolver_Factory INSTANCE = new ParquetTableResolver_Factory();
  }
}
