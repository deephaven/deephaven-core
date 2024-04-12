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
public final class PluginModuleTest_BadPlugin_Factory implements Factory<PluginModuleTest.BadPlugin> {
  @Override
  public PluginModuleTest.BadPlugin get() {
    return newInstance();
  }

  public static PluginModuleTest_BadPlugin_Factory create() {
    return InstanceHolder.INSTANCE;
  }

  public static PluginModuleTest.BadPlugin newInstance() {
    return new PluginModuleTest.BadPlugin();
  }

  private static final class InstanceHolder {
    private static final PluginModuleTest_BadPlugin_Factory INSTANCE = new PluginModuleTest_BadPlugin_Factory();
  }
}
