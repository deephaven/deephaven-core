/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.plugin.js;

import io.deephaven.plugin.Plugin;
import io.deephaven.plugin.PluginBase;

public abstract class JsPluginBase extends PluginBase implements JsPlugin {

    @Override
    public final <T, V extends Plugin.Visitor<T>> T walk(V visitor) {
        return visitor.visit(this);
    }
}
