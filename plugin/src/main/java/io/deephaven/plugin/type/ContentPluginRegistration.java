/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.plugin.type;

import java.io.IOException;

/**
 * The {@link ContentPlugin} specific registration.
 */
public interface ContentPluginRegistration {

    /**
     * Register {@code jsPlugin}.
     *
     * @param contentPlugin the js plugin
     */
    void register(ContentPlugin contentPlugin) throws IOException;
}
