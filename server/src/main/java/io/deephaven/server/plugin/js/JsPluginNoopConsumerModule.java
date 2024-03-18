//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.plugin.js;

import dagger.Module;
import dagger.Provides;
import io.deephaven.plugin.js.JsPluginRegistration;

/**
 * Provides a no-op {@link JsPluginRegistration} for servers that don't host JS content.
 */
@Module
public interface JsPluginNoopConsumerModule {

    @Provides
    static JsPluginRegistration providesNoopJsPluginConsumer() {
        return jsPlugin -> {
        };
    }
}
