package io.deephaven.server.custom;

import dagger.internal.DaggerGenerated;
import dagger.internal.Factory;
import dagger.internal.Preconditions;
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
public final class CustomComponentFactory_CustomModule_ProvidesApp2Factory implements Factory<CustomApplication2> {
  @Override
  public CustomApplication2 get() {
    return providesApp2();
  }

  public static CustomComponentFactory_CustomModule_ProvidesApp2Factory create() {
    return InstanceHolder.INSTANCE;
  }

  public static CustomApplication2 providesApp2() {
    return Preconditions.checkNotNullFromProvides(CustomComponentFactory.CustomModule.providesApp2());
  }

  private static final class InstanceHolder {
    private static final CustomComponentFactory_CustomModule_ProvidesApp2Factory INSTANCE = new CustomComponentFactory_CustomModule_ProvidesApp2Factory();
  }
}
