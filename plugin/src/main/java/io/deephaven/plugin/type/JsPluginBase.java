package io.deephaven.plugin.type;

public abstract class JsPluginBase extends ContentPluginBase implements JsPlugin {

    @Override
    public final <T> T walk(ContentPlugin.Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
