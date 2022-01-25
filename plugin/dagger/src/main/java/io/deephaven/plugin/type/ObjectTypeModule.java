package io.deephaven.plugin.type;

import dagger.Module;
import dagger.Provides;
import dagger.multibindings.ElementsIntoSet;

import java.util.Collections;
import java.util.Set;

/**
 * Provides an empty set for {@link ObjectType}.
 */
@Module
public interface ObjectTypeModule {

    @Provides
    @ElementsIntoSet
    static Set<ObjectType> primesObjectTypes() {
        return Collections.emptySet();
    }
}
