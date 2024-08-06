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
    "rawtypes",
    "KotlinInternal",
    "KotlinInternalInJava",
    "cast"
})
public final class PluginModule_ProvidesServiceLoaderJsPluginFactory implements Factory<Set<Registration>> {
  @Override
  public Set<Registration> get() {
    return providesServiceLoaderJsPlugin();
  }

  public static PluginModule_ProvidesServiceLoaderJsPluginFactory create() {
    return InstanceHolder.INSTANCE;
  }

  public static Set<Registration> providesServiceLoaderJsPlugin() {
    return Preconditions.checkNotNullFromProvides(PluginModule.providesServiceLoaderJsPlugin());
  }

  private static final class InstanceHolder {
    private static final PluginModule_ProvidesServiceLoaderJsPluginFactory INSTANCE = new PluginModule_ProvidesServiceLoaderJsPluginFactory();
  }
}
