/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.jetty.jsplugin;

import dagger.Binds;
import dagger.Module;
import io.deephaven.plugin.type.JsTypeRegistration;

/**
 * Binds {@link JsPlugins} as {@link JsTypeRegistration}.
 */
@Module
public interface JsPluginsModule {

    @Binds
    JsTypeRegistration bindsJsPlugins(JsPlugins plugins);
}
