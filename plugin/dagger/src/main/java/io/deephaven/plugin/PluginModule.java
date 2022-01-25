package io.deephaven.plugin;

import dagger.Module;
import dagger.Provides;
import dagger.multibindings.ElementsIntoSet;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeModule;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Provides an empty set for {@link Plugin} and adapters into {@link Plugin} from {@link ObjectType}.
 *
 * @see ObjectTypeModule
 */
@Module(includes = {ObjectTypeModule.class})
public interface PluginModule {

    @Provides
    @ElementsIntoSet
    static Set<Plugin> primesPlugins() {
        return Collections.emptySet();
    }

    @Provides
    @ElementsIntoSet
    static Set<Plugin> adaptsObjectTypes(Set<ObjectType> apps) {
        return new HashSet<>(apps);
    }
}
