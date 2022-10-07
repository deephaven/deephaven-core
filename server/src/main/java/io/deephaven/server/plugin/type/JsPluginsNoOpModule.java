/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.plugin.type;

import dagger.Binds;
import dagger.Module;
import io.deephaven.plugin.type.JsPluginRegistration;

/**
 * Binds {@link JsPluginRegistrationNoOp} as {@link JsPluginRegistration}.
 */
@Module
public interface JsPluginsNoOpModule {
    @Binds
    JsPluginRegistration bindsRegistration(JsPluginRegistrationNoOp noop);
}
