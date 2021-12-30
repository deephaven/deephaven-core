package io.deephaven.figure;

import io.deephaven.plugin.Plugin;
import io.deephaven.plugin.PluginCallback;

/**
 * Registers the object type {@link FigureWidgetType}.
 */
public final class FigureWidgetPlugin implements Plugin {

    @Override
    public void registerInto(PluginCallback callback) {
        callback.registerObjectType(FigureWidgetType.instance());
    }
}
