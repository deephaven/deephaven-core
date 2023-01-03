/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.runner;

import dagger.Module;
import io.deephaven.server.console.SessionToExecutionStateModule;
import io.deephaven.server.console.groovy.GroovyConsoleSessionModule;
import io.deephaven.server.console.python.PythonConsoleSessionModule;
import io.deephaven.server.console.python.PythonGlobalScopeCopyModule;
import io.deephaven.server.healthcheck.HealthCheckModule;
import io.deephaven.server.log.LogModule;
import io.deephaven.server.plugin.python.PythonPluginsRegistration;

/**
 * Includes some of the common modules necessary for creating a {@link DeephavenApiServerComponent} /
 * {@link DeephavenApiServer}.
 *
 * <p>
 * As use-cases arise, modules may be removed from these defaults to better support common cases where integrators want
 * to provide their own implementation of some feature.
 *
 * <p>
 * Advanced integrators may choose to declare all of their own modules instead of depending on the defaults here.
 *
 * @see DeephavenApiServerModule
 * @see LogModule
 * @see DeephavenApiConfigModule
 * @see PythonGlobalScopeCopyModule
 * @see HealthCheckModule
 * @see PythonPluginsRegistration.Module
 * @see PythonConsoleSessionModule
 * @see GroovyConsoleSessionModule
 * @see SessionToExecutionStateModule
 */
@Module(includes = {
        DeephavenApiServerModule.class,
        LogModule.class,
        DeephavenApiConfigModule.class,
        PythonGlobalScopeCopyModule.class,
        HealthCheckModule.class,
        PythonPluginsRegistration.Module.class,
        PythonConsoleSessionModule.class,
        GroovyConsoleSessionModule.class,
        SessionToExecutionStateModule.class,
})
public interface CommunityDefaultsModule {
}
