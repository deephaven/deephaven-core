/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.plugin;

import dagger.Module;
import dagger.Provides;
import io.deephaven.plugin.PluginModule;
import io.deephaven.plugin.Registration;
import io.deephaven.plugin.Registration.Callback;
import io.deephaven.plugin.js.JsPlugin;
import io.deephaven.plugin.type.ObjectTypeRegistration;
import io.deephaven.server.plugin.js.JsPluginModule;
import io.deephaven.server.plugin.type.ObjectTypesModule;

import javax.inject.Named;
import java.util.function.Consumer;

/**
 * Includes the {@link Module modules} necessary to provide {@link PluginRegistration}.
 *
 * <p>
 * Downstream servers will need to provide an appropriate {@link JsPlugin} {@link Consumer} {@link Named named}
 * {@value JS_PLUGIN_CONSUMER_NAME}, or include {@link io.deephaven.server.plugin.js.JsPluginNoopConsumerModule}.
 *
 * <p>
 * Note: runtime plugin registration is not currently supported - ie, no {@link Callback} is provided. See
 * <a href="https://github.com/deephaven/deephaven-core/issues/1809">deephaven-core#1809</a> for the feature request.
 *
 * @see ObjectTypesModule
 * @see JsPluginModule
 * @see PluginModule
 */
@Module(includes = {ObjectTypesModule.class, JsPluginModule.class, PluginModule.class})
public interface PluginsModule {
    String JS_PLUGIN_CONSUMER_NAME = "JsPlugin";

    @Provides
    static Registration.Callback providesPluginRegistrationCallback(
            ObjectTypeRegistration objectTypeRegistration,
            @Named(JS_PLUGIN_CONSUMER_NAME) Consumer<JsPlugin> jsPluginConsumer) {
        return new PluginRegistrationVisitor(objectTypeRegistration, jsPluginConsumer);
    }
}
