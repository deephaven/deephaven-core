package io.deephaven.plugin.type;

import dagger.Module;
import dagger.Provides;
import dagger.multibindings.ElementsIntoSet;
import io.deephaven.plugin.Plugin;

import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Provides the set of {@link ObjectType} from {@link ServiceLoader#load(Class)}.
 */
@Module
public interface ObjectTypeModule {

    @Provides
    @ElementsIntoSet
    static Set<ObjectType> providesServiceLoaderObjectTypes() {
        return ServiceLoader.load(ObjectType.class).stream().map(Provider::get).collect(Collectors.toSet());
    }
}
