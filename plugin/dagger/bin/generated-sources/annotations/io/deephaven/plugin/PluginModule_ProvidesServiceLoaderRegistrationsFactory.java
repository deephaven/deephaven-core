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
public final class PluginModule_ProvidesServiceLoaderRegistrationsFactory implements Factory<Set<Registration>> {
  @Override
  public Set<Registration> get() {
    return providesServiceLoaderRegistrations();
  }

  public static PluginModule_ProvidesServiceLoaderRegistrationsFactory create() {
    return InstanceHolder.INSTANCE;
  }

  public static Set<Registration> providesServiceLoaderRegistrations() {
    return Preconditions.checkNotNullFromProvides(PluginModule.providesServiceLoaderRegistrations());
  }

  private static final class InstanceHolder {
    private static final PluginModule_ProvidesServiceLoaderRegistrationsFactory INSTANCE = new PluginModule_ProvidesServiceLoaderRegistrationsFactory();
  }
}
