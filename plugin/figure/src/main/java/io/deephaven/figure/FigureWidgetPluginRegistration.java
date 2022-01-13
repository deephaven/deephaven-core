package io.deephaven.figure;

import com.google.auto.service.AutoService;
import io.deephaven.plugin.Registration;
import io.deephaven.plugin.RegistrationBase;

/**
 * Registers the object type plugin {@link FigureWidgetTypePlugin}.
 */
@AutoService(Registration.class)
public final class FigureWidgetPluginRegistration extends RegistrationBase {

    @Override
    protected void initImpl() {

    }

    @Override
    protected void registerIntoImpl(Callback callback) {
        callback.register(FigureWidgetTypePlugin.instance());
    }
}
