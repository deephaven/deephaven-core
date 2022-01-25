package io.deephaven.plugin;

import dagger.Module;
import dagger.Provides;
import dagger.multibindings.ElementsIntoSet;
import dagger.multibindings.IntoSet;

import java.util.Collections;
import java.util.Set;

/**
 * Provides an empty set for {@link Registration} and adapters into {@link Registration} from {@link Plugin}.
 *
 * @see PluginModule
 */
@Module(includes = {PluginModule.class})
public interface RegistrationModule {

    @Provides
    @ElementsIntoSet
    static Set<Registration> primesRegistrations() {
        return Collections.emptySet();
    }

    @Provides
    @IntoSet
    static Registration adaptsPlugins(Set<Plugin> plugins) {
        return new PluginRegistrations(plugins);
    }
}
