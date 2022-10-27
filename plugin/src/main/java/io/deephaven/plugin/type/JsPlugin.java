package io.deephaven.plugin.type;

/**
 * A js plugin. Useful for adding custom JS code to the server that can be passed to the web client.
 *
 * @see <a href="https://github.com/deephaven/js-plugin-template">js-plugin-template</a>
 */
public interface JsPlugin extends ContentPlugin {

    JsPluginInfo info();
}
