package io.deephaven.plugin.type;

import dagger.Module;
import dagger.Provides;
import dagger.multibindings.ElementsIntoSet;

import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Provides the {@link ServiceLoader#load(Class)} set for {@link ObjectType}.
 *
 * @see ObjectTypeModule
 */
@Module(includes = {ObjectTypeModule.class})
public interface ObjectTypeServiceLoaderModule {

    @Provides
    @ElementsIntoSet
    static Set<ObjectType> providesServiceLoaderObjectTypes() {
        return ServiceLoader.load(ObjectType.class).stream().map(Provider::get).collect(Collectors.toSet());
    }
}
