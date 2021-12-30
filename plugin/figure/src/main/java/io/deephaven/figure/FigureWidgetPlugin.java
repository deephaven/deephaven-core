package io.deephaven.figure;

import io.deephaven.plugin.Plugin;
import io.deephaven.plugin.PluginCallback;
import io.deephaven.plugin.type.Exporter;
import io.deephaven.plugin.type.ObjectType;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Registers the object type {@link FigureWidgetType}.
 */
public final class FigureWidgetPlugin implements Plugin {

    @Override
    public void registerInto(PluginCallback callback) {
        callback.registerCustomType(FigureWidgetType.instance());
    }
}
