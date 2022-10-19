/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.jetty.jsplugin;

import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import io.deephaven.plugin.type.JsPluginRegistration;
import io.deephaven.server.jetty.JettyConfig;
import io.deephaven.server.plugin.type.JsPluginRegistrationNoOp;

import javax.inject.Singleton;
import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Provides {@link JsPluginsZipFilesystem} as a {@link Singleton}.
 *
 * <p>
 * If {@link JettyConfig#jsPluginsOrDefault()} is {@code true}, then {@link JsPluginsZipFilesystem} is provided as the
 * {@link JsPluginRegistration}; otherwise, a {@link JsPluginRegistrationNoOp} is provided.
 *
 * <p>
 * Binds {@link JsPluginsZipFilesystem} as {@link JsPlugins}.
 */
@Module
public interface JsPluginsModule {

    @Provides
    @Singleton
    static JsPluginsZipFilesystem providesJsPluginsZipFilesystem() {
        try {
            return JsPluginsZipFilesystem.create();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Provides
    static JsPluginRegistration providesRegistration(JettyConfig config, JsPluginsZipFilesystem plugins) {
        return config.jsPluginsOrDefault() ? plugins : JsPluginRegistrationNoOp.INSTANCE;
    }

    @Binds
    JsPlugins bindsJsPluginsZipFilesystem(JsPluginsZipFilesystem plugins);
}
