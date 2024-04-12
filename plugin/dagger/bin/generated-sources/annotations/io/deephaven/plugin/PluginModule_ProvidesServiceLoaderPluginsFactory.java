package io.deephaven.plugin;

import dagger.internal.DaggerGenerated;
import dagger.internal.Factory;
import dagger.internal.Preconditions;
import dagger.internal.QualifierMetadata;
import dagger.internal.ScopeMetadata;
import java.util.Set;
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
public final class PluginModule_ProvidesServiceLoaderPluginsFactory implements Factory<Set<Registration>> {
  @Override
  public Set<Registration> get() {
    return providesServiceLoaderPlugins();
  }

  public static PluginModule_ProvidesServiceLoaderPluginsFactory create() {
    return InstanceHolder.INSTANCE;
  }

  public static Set<Registration> providesServiceLoaderPlugins() {
    return Preconditions.checkNotNullFromProvides(PluginModule.providesServiceLoaderPlugins());
  }

  private static final class InstanceHolder {
    private static final PluginModule_ProvidesServiceLoaderPluginsFactory INSTANCE = new PluginModule_ProvidesServiceLoaderPluginsFactory();
  }
}
