/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.jetty.jsplugin;

import dagger.Module;
import dagger.Provides;
import io.deephaven.plugin.type.JsTypeRegistration;
import io.deephaven.server.jetty.JettyConfig;
import io.deephaven.server.plugin.type.JsTypeRegistrationNoOp;

import javax.inject.Singleton;
import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Provides {@link JsPlugins} as a {@link Singleton} via {@link JsPlugins#create()}. If
 * {@link JettyConfig#jsPluginsOrDefault()} is {@code true}, then the plugin is provided as the
 * {@link JsTypeRegistration}; otherwise, a {@link JsTypeRegistrationNoOp} is provided.
 */
@Module
public interface JsPluginsModule {

    @Provides
    static JsTypeRegistration providesRegistration(JettyConfig config, JsPlugins plugins) {
        return config.jsPluginsOrDefault() ? plugins : new JsTypeRegistrationNoOp();
    }

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
