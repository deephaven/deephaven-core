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
public final class PluginModule_ProvidesServiceLoaderObjectTypesFactory implements Factory<Set<Registration>> {
  @Override
  public Set<Registration> get() {
    return providesServiceLoaderObjectTypes();
  }

  public static PluginModule_ProvidesServiceLoaderObjectTypesFactory create() {
    return InstanceHolder.INSTANCE;
  }

  public static Set<Registration> providesServiceLoaderObjectTypes() {
    return Preconditions.checkNotNullFromProvides(PluginModule.providesServiceLoaderObjectTypes());
  }

  private static final class InstanceHolder {
    private static final PluginModule_ProvidesServiceLoaderObjectTypesFactory INSTANCE = new PluginModule_ProvidesServiceLoaderObjectTypesFactory();
  }
}
