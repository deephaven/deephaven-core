package io.deephaven.server.plugin;

import dagger.Binds;
import dagger.Module;
import io.deephaven.plugin.Registration;
import io.deephaven.plugin.Registration.Callback;
import io.deephaven.plugin.RegistrationServiceLoaderModule;
import io.deephaven.server.plugin.type.ObjectTypesModule;

/**
 * Includes the {@link Module modules} necessary to provide {@link PluginRegistration}.
 *
 * <p>
 * Note: runtime plugin registration is not currently supported - ie, no {@link Callback} is provided. See
 * <a href="https://github.com/deephaven/deephaven-core/issues/1809">deephaven-core#1809</a> for the feature request.
 *
 * @see ObjectTypesModule
 * @see RegistrationServiceLoaderModule
 */
@Module(includes = {ObjectTypesModule.class, RegistrationServiceLoaderModule.class})
public interface PluginsModule {

    @Binds
    Registration.Callback bindPluginRegistrationCallback(PluginRegistrationVisitor visitor);
}
