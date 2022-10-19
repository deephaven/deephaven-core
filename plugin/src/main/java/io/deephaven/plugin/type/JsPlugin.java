/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.plugin.type;

import io.deephaven.plugin.Plugin;

import java.io.IOException;
import java.nio.file.Path;

/**
 * A js plugin. Useful for adding custom JS code to the server that can be passed to the web client.
 *
 * @see <a href="https://github.com/deephaven/js-plugin-template">js-plugin-template</a>
 */
public interface JsPlugin extends Plugin {

    /**
     * The js plugin info.
     */
    JsPluginInfo info();

    /**
     * Copy all distribution files into the directory {@code destination}.
     *
     * <p>
     * Note: this should only be called during the {@link JsPluginRegistration#register(JsPlugin)} phase.
     *
     * @param destination the destination directory
     * @throws IOException if an I/O exception occurs
     */
    void copyTo(Path destination) throws IOException;
}
