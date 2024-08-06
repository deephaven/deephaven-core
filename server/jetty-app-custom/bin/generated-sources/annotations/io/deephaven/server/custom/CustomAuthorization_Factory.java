package io.deephaven.server.custom;

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
public final class CustomAuthorization_Factory implements Factory<CustomAuthorization> {
  @Override
  public CustomAuthorization get() {
    return newInstance();
  }

  public static CustomAuthorization_Factory create() {
    return InstanceHolder.INSTANCE;
  }

  public static CustomAuthorization newInstance() {
    return new CustomAuthorization();
  }

  private static final class InstanceHolder {
    private static final CustomAuthorization_Factory INSTANCE = new CustomAuthorization_Factory();
  }
}
