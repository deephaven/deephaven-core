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
public final class PluginModuleTest_BoundRegistration_Factory implements Factory<PluginModuleTest.BoundRegistration> {
  @Override
  public PluginModuleTest.BoundRegistration get() {
    return newInstance();
  }

  public static PluginModuleTest_BoundRegistration_Factory create() {
    return InstanceHolder.INSTANCE;
  }

  public static PluginModuleTest.BoundRegistration newInstance() {
    return new PluginModuleTest.BoundRegistration();
  }

  private static final class InstanceHolder {
    private static final PluginModuleTest_BoundRegistration_Factory INSTANCE = new PluginModuleTest_BoundRegistration_Factory();
  }
}
