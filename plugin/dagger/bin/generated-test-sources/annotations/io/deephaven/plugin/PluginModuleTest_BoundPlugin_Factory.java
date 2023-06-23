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
public final class PluginModuleTest_BoundPlugin_Factory implements Factory<PluginModuleTest.BoundPlugin> {
  @Override
  public PluginModuleTest.BoundPlugin get() {
    return newInstance();
  }

  public static PluginModuleTest_BoundPlugin_Factory create() {
    return InstanceHolder.INSTANCE;
  }

  public static PluginModuleTest.BoundPlugin newInstance() {
    return new PluginModuleTest.BoundPlugin();
  }

  private static final class InstanceHolder {
    private static final PluginModuleTest_BoundPlugin_Factory INSTANCE = new PluginModuleTest_BoundPlugin_Factory();
  }
}
