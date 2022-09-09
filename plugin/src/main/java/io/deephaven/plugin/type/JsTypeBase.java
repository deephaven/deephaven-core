/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.plugin.type;

import io.deephaven.plugin.Plugin;
import io.deephaven.plugin.PluginBase;

public abstract class JsTypeBase extends PluginBase implements JsType {

    @Override
    public final <T, V extends Plugin.Visitor<T>> T walk(V visitor) {
        return visitor.visit(this);
    }
}
