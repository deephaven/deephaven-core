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
public final class PluginModuleTest_BadObjectType_Factory implements Factory<PluginModuleTest.BadObjectType> {
  @Override
  public PluginModuleTest.BadObjectType get() {
    return newInstance();
  }

  public static PluginModuleTest_BadObjectType_Factory create() {
    return InstanceHolder.INSTANCE;
  }

  public static PluginModuleTest.BadObjectType newInstance() {
    return new PluginModuleTest.BadObjectType();
  }

  private static final class InstanceHolder {
    private static final PluginModuleTest_BadObjectType_Factory INSTANCE = new PluginModuleTest_BadObjectType_Factory();
  }
}
