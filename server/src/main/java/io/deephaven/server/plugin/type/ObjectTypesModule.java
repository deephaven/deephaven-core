package io.deephaven.server.plugin.type;

import dagger.Binds;
import dagger.Module;
import io.deephaven.plugin.type.ObjectTypeCallback;
import io.deephaven.plugin.type.ObjectTypeLookup;

/**
 * Binds {@link ObjectTypes} as {@link ObjectTypeLookup} and {@link ObjectTypeCallback}.
 */
@Module
public interface ObjectTypesModule {

    @Binds
    ObjectTypeLookup bindsLookup(ObjectTypes types);

    @Binds
    ObjectTypeCallback bindsCallback(ObjectTypes types);
}
