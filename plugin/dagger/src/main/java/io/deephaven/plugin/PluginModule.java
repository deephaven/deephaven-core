package io.deephaven.plugin;

import dagger.Module;
import dagger.Provides;
import dagger.multibindings.ElementsIntoSet;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeModule;

import java.util.Collections;
import java.util.HashSet;
import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Provides the set of {@link Registration} from {@link ServiceLoader#load(Class)} and as adapted from {@link Plugin}
 * via {@link PluginAdapter}.
 *
 * <p>
 * Provides the set of {@link Plugin} from {@link ServiceLoader#load(Class)} and as casted directly from
 * {@link ObjectType}.
 *
 * @see ObjectTypeModule
 */
@Module(includes = {ObjectTypeModule.class})
public interface PluginModule {

    @Provides
    @ElementsIntoSet
    static Set<Registration> providesServiceLoaderRegistrations() {
        return ServiceLoader.load(Registration.class).stream().map(Provider::get).collect(Collectors.toSet());
    }

    @Provides
    @ElementsIntoSet
    static Set<Registration> adaptsPlugins(Set<Plugin> plugins) {
        return plugins.stream().map(PluginAdapter::new).collect(Collectors.toSet());
    }

    @Provides
    @ElementsIntoSet
    static Set<Plugin> providesServiceLoaderPlugins() {
        return ServiceLoader.load(Plugin.class).stream().map(Provider::get).collect(Collectors.toSet());
    }

    @Provides
    @ElementsIntoSet
    static Set<Plugin> adaptsObjectTypes(Set<ObjectType> apps) {
        return new HashSet<>(apps);
    }
}
