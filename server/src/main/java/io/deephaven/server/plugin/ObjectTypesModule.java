package io.deephaven.server.plugin;

import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import io.deephaven.plugin.type.ObjectTypeLookup;

import javax.inject.Singleton;

@Module
public interface ObjectTypesModule {

    @Provides
    @Singleton
    static ObjectTypes providesObjectTypes() {
        return new ObjectTypes();
    }

    @Binds
    ObjectTypeLookup providesLookup(ObjectTypes types);
}
