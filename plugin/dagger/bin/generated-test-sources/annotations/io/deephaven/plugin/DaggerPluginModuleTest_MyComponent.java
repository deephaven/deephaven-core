package io.deephaven.plugin;

import dagger.internal.DaggerGenerated;
import dagger.internal.SetBuilder;
import io.deephaven.plugin.type.ObjectType;
import java.util.Collections;
import java.util.Set;
import javax.annotation.processing.Generated;

@DaggerGenerated
@Generated(
    value = "dagger.internal.codegen.ComponentProcessor",
    comments = "https://dagger.dev"
)
@SuppressWarnings({
    "unchecked",
    "rawtypes"
})
final class DaggerPluginModuleTest_MyComponent {
  private DaggerPluginModuleTest_MyComponent() {
  }

  public static Builder builder() {
    return new Builder();
  }

  public static PluginModuleTest.MyComponent create() {
    return new Builder().build();
  }

  static final class Builder {
    private Builder() {
    }

    public PluginModuleTest.MyComponent build() {
      return new MyComponentImpl();
    }
  }

  private static final class MyComponentImpl implements PluginModuleTest.MyComponent {
    private final MyComponentImpl myComponentImpl = this;

    private MyComponentImpl() {


    }

    @Override
    public Set<Registration> registrations() {
      return SetBuilder.<Registration>newSetBuilder(6).addAll(PluginModule_ProvidesServiceLoaderObjectTypesFactory.providesServiceLoaderObjectTypes()).addAll(PluginModule_ProvidesServiceLoaderPluginsFactory.providesServiceLoaderPlugins()).addAll(PluginModule_ProvidesServiceLoaderRegistrationsFactory.providesServiceLoaderRegistrations()).add(new PluginModuleTest.BoundRegistration()).add(new PluginModuleTest.BoundPlugin()).add(new PluginModuleTest.BoundObjectType()).build();
    }

    @Override
    public Set<Plugin> plugins() {
      return Collections.<Plugin>singleton(new PluginModuleTest.BadPlugin());
    }

    @Override
    public Set<ObjectType> objectTypes() {
      return Collections.<ObjectType>singleton(new PluginModuleTest.BadObjectType());
    }
  }
}
