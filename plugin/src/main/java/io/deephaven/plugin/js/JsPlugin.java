/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.plugin.js;

import io.deephaven.plugin.Plugin;

/**
 * A js plugin is a {@link Plugin} that allows adding javascript code under the server's URL path "js-plugins/". See
 * <a href="https://github.com/deephaven/deephaven-plugins#js-plugins">deephaven-plugins#js-plugins</a> for more details
 * about the underlying construction for js plugins.
 *
 * @see JsPluginPackagePath
 * @see JsPluginManifestPath
 */
public interface JsPlugin extends Plugin {

}
