/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.plugin.type;

import java.io.IOException;

/**
 * The {@link JsPlugin} specific registration.
 */
public interface JsPluginRegistration {

    /**
     * Register {@code jsPlugin}.
     *
     * @param jsPlugin the js plugin
     */
    void register(JsPlugin jsPlugin) throws IOException;
}
