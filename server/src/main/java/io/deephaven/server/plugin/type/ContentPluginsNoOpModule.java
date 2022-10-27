/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.plugin.type;

import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import io.deephaven.plugin.type.ContentPluginRegistration;

/**
 * Binds {@link ContentPluginRegistrationNoOp} as {@link ContentPluginRegistration}.
 */
@Module
public interface ContentPluginsNoOpModule {

    @Provides
    static ContentPluginRegistrationNoOp providesContentPluginRegistrationNoOp() {
        return ContentPluginRegistrationNoOp.INSTANCE;
    }

    @Binds
    ContentPluginRegistration bindsRegistration(ContentPluginRegistrationNoOp noop);
}
