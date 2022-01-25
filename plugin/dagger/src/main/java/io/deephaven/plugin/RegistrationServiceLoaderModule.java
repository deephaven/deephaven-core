package io.deephaven.plugin;

import dagger.Module;
import dagger.Provides;
import dagger.multibindings.ElementsIntoSet;

import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Provides the {@link ServiceLoader#load(Class)} set for {@link Registration}.
 *
 * @see RegistrationModule
 * @see PluginServiceLoaderModule
 */
@Module(includes = {RegistrationModule.class, PluginServiceLoaderModule.class})
public interface RegistrationServiceLoaderModule {

    @Provides
    @ElementsIntoSet
    static Set<Registration> providesServiceLoaderRegistrations() {
        return ServiceLoader.load(Registration.class).stream().map(Provider::get).collect(Collectors.toSet());
    }
}
