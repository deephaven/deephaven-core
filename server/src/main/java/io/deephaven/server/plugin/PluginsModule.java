package io.deephaven.server.plugin;

import dagger.Module;
import io.deephaven.server.plugin.type.ObjectTypesModule;

/**
 * Includes the {@link Module modules} necessary to provide {@link PluginsAutoDiscovery}.
 *
 * <p>
 * Note: runtime plugin registration is not currently supported - ie, no {@link io.deephaven.plugin.PluginCallback} is
 * provided. See <a href="https://github.com/deephaven/deephaven-core/issues/1809">deephaven-core#1809</a> for the
 * feature request.
 *
 * @see ObjectTypesModule
 */
@Module(includes = {ObjectTypesModule.class})
public interface PluginsModule {

}
