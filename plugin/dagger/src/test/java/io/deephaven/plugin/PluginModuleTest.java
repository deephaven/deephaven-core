package io.deephaven.plugin;

import com.google.auto.service.AutoService;
import dagger.Binds;
import dagger.Component;
import dagger.Module;
import dagger.multibindings.IntoSet;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeBase;
import org.junit.jupiter.api.Test;

import javax.inject.Inject;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class PluginModuleTest {
    @Component(modules = {MyModule.class})
    interface MyComponent {

        Set<Registration> registrations();

        // testing only, real impl doesn't expect this to be bound
        Set<Plugin> plugins();

        // testing only, real impl doesn't expect this to be bound
        Set<ObjectType> objectTypes();
    }

    @Module(includes = {PluginModule.class})
    interface MyModule {

        @Binds
        @IntoSet
        Registration providesBoundRegistration(BoundRegistration boundRegistration);

        @Binds
        @IntoSet
        Registration providesBoundPlugin(BoundPlugin boundPlugin);

        @Binds
        @IntoSet
        Registration providesBoundObjectType(BoundObjectType boundObjectType);

        // This won't get provided as a Registration
        @Binds
        @IntoSet
        Plugin providesBadPlugin(BadPlugin badPlugin);

        // This won't get provided as a Registration
        @Binds
        @IntoSet
        ObjectType providesBadObjectType(BadObjectType badObjectType);
    }

    @AutoService(Registration.class)
    public static class AutoRegistration implements Registration {

        public AutoRegistration() {}

        @Override
        public void registerInto(Callback callback) {

        }
    }

    @AutoService(Plugin.class)
    public static class AutoPlugin extends PluginBase {

        public AutoPlugin() {}

        @Override
        public <T, V extends Visitor<T>> T walk(V visitor) {
            throw new UnsupportedOperationException("Not a real plugin");
        }
    }

    @AutoService(ObjectType.class)
    public static class AutoObjectType extends ObjectTypeBase {

        public AutoObjectType() {}

        @Override
        public String name() {
            return AutoObjectType.class.getSimpleName();
        }

        @Override
        public boolean isType(Object object) {
            return object instanceof AutoObjectType;
        }

        @Override
        public void writeCompatibleObjectTo(Exporter exporter, Object object, OutputStream out) throws IOException {
            throw new UnsupportedOperationException("Not a real plugin");
        }
    }

    public static class BoundRegistration implements Registration {
        @Inject
        public BoundRegistration() {}

        @Override
        public void registerInto(Callback callback) {

        }
    }

    public static class BoundPlugin extends PluginBase {
        @Inject
        public BoundPlugin() {}

        @Override
        public <T, V extends Visitor<T>> T walk(V visitor) {
            throw new UnsupportedOperationException("Not a real plugin");
        }
    }

    public static class BoundObjectType extends ObjectTypeBase {
        @Inject
        public BoundObjectType() {}

        @Override
        public String name() {
            return BoundObjectType.class.getSimpleName();
        }

        @Override
        public boolean isType(Object object) {
            return object instanceof BoundObjectType;
        }

        @Override
        public void writeCompatibleObjectTo(Exporter exporter, Object object, OutputStream out) throws IOException {
            throw new UnsupportedOperationException("Not a real plugin");
        }
    }

    public static class BadPlugin extends PluginBase {
        @Inject
        public BadPlugin() {}

        @Override
        public <T, V extends Visitor<T>> T walk(V visitor) {
            throw new UnsupportedOperationException("Not a real plugin");
        }
    }

    public static class BadObjectType extends ObjectTypeBase {
        @Inject
        public BadObjectType() {}

        @Override
        public String name() {
            return BadObjectType.class.getSimpleName();
        }

        @Override
        public boolean isType(Object object) {
            return object instanceof BadObjectType;
        }

        @Override
        public void writeCompatibleObjectTo(Exporter exporter, Object object, OutputStream out) throws IOException {
            throw new UnsupportedOperationException("Not a real plugin");
        }
    }


    @Test
    void registrations() {
        final Set<Class<? extends Registration>> registrationClasses = DaggerPluginModuleTest_MyComponent.create()
                .registrations()
                .stream()
                .map(Registration::getClass)
                .collect(Collectors.toSet());

        assertThat(registrationClasses).isEqualTo(Set.of(
                AutoRegistration.class,
                AutoPlugin.class,
                AutoObjectType.class,
                BoundRegistration.class,
                BoundPlugin.class,
                BoundObjectType.class));
    }

    @Test
    void plugins() {
        final Set<Class<? extends Plugin>> pluginClasses = DaggerPluginModuleTest_MyComponent.create()
                .plugins()
                .stream()
                .map(Plugin::getClass)
                .collect(Collectors.toSet());

        assertThat(pluginClasses).isEqualTo(Set.of(BadPlugin.class));
    }

    @Test
    void objectTypes() {
        final Set<Class<? extends ObjectType>> objectTypeClasses = DaggerPluginModuleTest_MyComponent.create()
                .objectTypes()
                .stream()
                .map(ObjectType::getClass)
                .collect(Collectors.toSet());

        assertThat(objectTypeClasses).isEqualTo(Set.of(BadObjectType.class));
    }
}
