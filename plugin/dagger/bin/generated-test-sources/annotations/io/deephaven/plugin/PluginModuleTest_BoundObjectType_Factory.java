package io.deephaven.plugin;

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
    "rawtypes"
})
public final class PluginModuleTest_BoundObjectType_Factory implements Factory<PluginModuleTest.BoundObjectType> {
  @Override
  public PluginModuleTest.BoundObjectType get() {
    return newInstance();
  }

  public static PluginModuleTest_BoundObjectType_Factory create() {
    return InstanceHolder.INSTANCE;
  }

  public static PluginModuleTest.BoundObjectType newInstance() {
    return new PluginModuleTest.BoundObjectType();
  }

  private static final class InstanceHolder {
    private static final PluginModuleTest_BoundObjectType_Factory INSTANCE = new PluginModuleTest_BoundObjectType_Factory();
  }
}
