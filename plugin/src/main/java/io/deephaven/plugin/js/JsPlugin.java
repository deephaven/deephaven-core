/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.plugin.js;

import io.deephaven.plugin.Plugin;

/**
 * A js plugin.
 *
 * @see JsPluginPackagePath
 * @see JsPluginManifestPath
 */
public interface JsPlugin extends Plugin {

    <T> T walk(Visitor<T> visitor);

    interface Visitor<T> {
        T visit(JsPluginPackagePath packageRoot);

        T visit(JsPluginManifestPath manifestRoot);
    }
}
