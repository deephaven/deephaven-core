package io.deephaven.figure;

import com.google.auto.service.AutoService;
import io.deephaven.plugin.Registration;

/**
 * Registers the object type plugin {@link FigureWidgetTypePlugin}.
 */
@AutoService(Registration.class)
public final class FigureWidgetPluginRegistration implements Registration {

    @Override
    public void registerInto(Callback callback) {
        callback.register(FigureWidgetTypePlugin.instance());
    }
}
