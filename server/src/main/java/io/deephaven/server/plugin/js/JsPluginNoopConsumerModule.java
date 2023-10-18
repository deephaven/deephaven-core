/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.plugin.js;

import dagger.Module;
import dagger.Provides;
import io.deephaven.plugin.js.JsPlugin;
import io.deephaven.server.plugin.PluginsModule;

import javax.inject.Named;
import java.util.function.Consumer;

/**
 * Provides a no-op {@link JsPlugin} {@link Consumer} {@link Named named}
 * {@value PluginsModule#JS_PLUGIN_CONSUMER_NAME}.
 */
@Module
public interface JsPluginNoopConsumerModule {

    @Provides
    @Named(PluginsModule.JS_PLUGIN_CONSUMER_NAME)
    static Consumer<JsPlugin> providesNoopJsPluginConsumer() {
        return jsPlugin -> {
        };
    }
}
