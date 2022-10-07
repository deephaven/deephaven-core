/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.plugin.type;

import io.deephaven.plugin.Plugin;

import java.io.IOException;
import java.nio.file.Path;

/**
 * A js plugin. Useful for adding custom JS code to the server that can be passed to the web client.
 */
public interface JsPlugin extends Plugin {

    /**
     * The name of the plugin.
     *
     * @return the name
     */
    String name();

    /**
     * The version of the plugin.
     *
     * @return the version
     */
    String version();

    /**
     * The main js file; the relative path with respect to {@link #copyTo(Path)} destination.
     *
     * @return the main entrypoint.
     */
    String main();

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
