package io.deephaven.figure;

import io.deephaven.plugin.Registration;

/**
 * Registers the object type plugin {@link FigureWidgetTypePlugin}.
 */
public final class FigureWidgetPluginRegistration implements Registration {

    @Override
    public void registerInto(Callback callback) {
        callback.register(FigureWidgetTypePlugin.instance());
    }
}
