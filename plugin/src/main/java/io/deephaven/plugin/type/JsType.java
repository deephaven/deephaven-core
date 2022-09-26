/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.plugin.type;

import io.deephaven.plugin.Plugin;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;

/**
 * A "js type" plugin. Useful for adding custom javascript code to the server.
 */
public interface JsType extends Plugin {

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
     * Note: this is only be called during the {@link JsTypeRegistration#register(JsType)} phase.
     *
     * @param destination the destination directory
     * @throws IOException if an I/O exception occurs
     */
    void copyTo(Path destination) throws IOException;
}
