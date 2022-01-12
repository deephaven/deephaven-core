package io.deephaven.figure;

import io.deephaven.plugin.RegistrationBase;

/**
 * Registers the object type plugin {@link FigureWidgetTypePlugin}.
 */
public final class FigureWidgetPluginRegistration extends RegistrationBase {

    @Override
    protected void initImpl() {

    }

    @Override
    protected void registerIntoImpl(Callback callback) {
        callback.register(FigureWidgetTypePlugin.instance());
    }
}
