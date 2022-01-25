package io.deephaven.server.plugin.type;

import dagger.Binds;
import dagger.Module;
import io.deephaven.plugin.type.ObjectTypeRegistration;
import io.deephaven.plugin.type.ObjectTypeLookup;

/**
 * Binds {@link ObjectTypes} as {@link ObjectTypeLookup} and {@link ObjectTypeRegistration}.
 */
@Module
public interface ObjectTypesModule {

    @Binds
    ObjectTypeLookup bindsLookup(ObjectTypes types);

    @Binds
    ObjectTypeRegistration bindsCallback(ObjectTypes types);
}
