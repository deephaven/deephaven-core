package io.deephaven.plugin;

import dagger.Module;
import dagger.Provides;
import dagger.multibindings.ElementsIntoSet;
import io.deephaven.plugin.type.ObjectTypeServiceLoaderModule;

import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Provides the {@link ServiceLoader#load(Class)} set for {@link Plugin}.
 *
 * @see PluginModule
 * @see ObjectTypeServiceLoaderModule
 */
@Module(includes = {PluginModule.class, ObjectTypeServiceLoaderModule.class})
public interface PluginServiceLoaderModule {

    @Provides
    @ElementsIntoSet
    static Set<Plugin> providesServiceLoaderPlugins() {
        return ServiceLoader.load(Plugin.class).stream().map(Provider::get).collect(Collectors.toSet());
    }
}
