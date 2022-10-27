/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.jetty.jsplugin;

import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import io.deephaven.plugin.type.ContentPluginRegistration;
import io.deephaven.server.jetty.JettyConfig;
import io.deephaven.server.plugin.type.ContentPluginRegistrationNoOp;

import javax.inject.Singleton;
import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Provides {@link ContentPluginsZipFilesystem} as a {@link Singleton}.
 *
 * <p>
 * If {@link JettyConfig#jsPluginsOrDefault()} is {@code true}, then {@link ContentPluginsZipFilesystem} is provided as
 * the {@link ContentPluginRegistration}; otherwise, a {@link ContentPluginRegistrationNoOp} is provided.
 *
 * <p>
 * Binds {@link ContentPluginsZipFilesystem} as {@link ContentPlugins}.
 */
@Module
public interface ContentPluginsModule {

    @Provides
    @Singleton
    static ContentPluginsZipFilesystem providesContentPluginsZipFilesystem() {
        try {
            return ContentPluginsZipFilesystem.create();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Provides
    static ContentPluginRegistration providesRegistration(JettyConfig config, ContentPluginsZipFilesystem plugins) {
        return config.jsPluginsOrDefault() ? plugins : ContentPluginRegistrationNoOp.INSTANCE;
    }

    @Binds
    ContentPlugins bindsContentPluginsZipFilesystem(ContentPluginsZipFilesystem plugins);
}
