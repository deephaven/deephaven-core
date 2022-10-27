/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.plugin.type;

import io.deephaven.plugin.Plugin;

import java.io.IOException;
import java.nio.file.Path;

/**
 * A content plugin. Useful for adding content to the server that can be passed to the client.
 *
 * @see JsPlugin
 */
public interface ContentPlugin extends Plugin {

    /**
     * Copy all distribution files into the directory {@code destination}.
     *
     * <p>
     * Note: this should only be called during the {@link ContentPluginRegistration#register(ContentPlugin)} phase.
     *
     * @param destination the destination directory
     * @throws IOException if an I/O exception occurs
     */
    void copyTo(Path destination) throws IOException;

    <T> T walk(Visitor<T> visitor);

    interface Visitor<T> {

        T visit(JsPlugin jsPlugin);
    }
}
