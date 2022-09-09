/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.jetty.jsplugin;

import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import io.deephaven.plugin.type.JsTypeRegistration;

import javax.inject.Singleton;
import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Binds {@link JsPlugins} as {@link JsTypeRegistration}.
 */
@Module
public interface JsPluginsModule {
    @Binds
    JsTypeRegistration bindsJsPlugins(JsPlugins plugins);

    @Provides
    @Singleton
    static JsPlugins providesJsPlugins() {
        try {
            return JsPlugins.create();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
