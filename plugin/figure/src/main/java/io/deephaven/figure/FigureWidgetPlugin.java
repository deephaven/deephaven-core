package io.deephaven.figure;

import io.deephaven.plugin.PluginBase;
import io.deephaven.plugin.type.ObjectTypeCallback;

/**
 * Registers the object type {@link FigureWidgetType}.
 */
public final class FigureWidgetPlugin extends PluginBase {

    @Override
    public void registerInto(ObjectTypeCallback callback) {
        callback.registerObjectType(FigureWidgetType.instance());
    }
}
