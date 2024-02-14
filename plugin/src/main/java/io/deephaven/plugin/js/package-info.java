/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */

/**
 * The Deephaven server supports {@link io.deephaven.plugin.js.JsPlugin JS plugins} which allow custom javascript (and
 * related content) to be served under the HTTP path "js-plugins/".
 *
 * <p>
 * A "js-plugins/manifest.json" is served that allows clients to discover what JS plugins are installed. This will be a
 * JSON object, and will have a "plugins" array, with object elements that have a "name", "version", and "main". All
 * files served via a specific plugin will be accessed under "js-plugins/{name}/". The main entry file for a plugin will
 * be accessed at "js-plugins/{name}/{main}". The "version" is currently for informational purposes only.
 *
 * @see <a href="https://github.com/deephaven/deephaven-plugins">deephaven-plugins</a> for Deephaven-maintained JS
 *      plugins
 */
package io.deephaven.plugin.js;
