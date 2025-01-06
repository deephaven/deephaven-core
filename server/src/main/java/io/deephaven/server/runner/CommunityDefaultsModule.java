//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.runner;

import dagger.Module;
import io.deephaven.time.calendar.CalendarsFromConfigurationModule;
import io.deephaven.server.console.ExecutionContextModule;
import io.deephaven.server.console.groovy.GroovyConsoleSessionModule;
import io.deephaven.server.console.python.PythonConsoleSessionModule;
import io.deephaven.server.console.python.PythonGlobalScopeCopyModule;
import io.deephaven.server.healthcheck.HealthCheckModule;
import io.deephaven.server.log.LogModule;
import io.deephaven.server.plugin.python.PythonPluginsRegistration;
import io.deephaven.server.session.ObfuscatingErrorTransformerModule;

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
 * @see ExecutionContextModule
 * @see ObfuscatingErrorTransformerModule
 * @see CalendarsFromConfigurationModule
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
        ExecutionContextModule.class,
        ObfuscatingErrorTransformerModule.class,
        CalendarsFromConfigurationModule.class,
})
public interface CommunityDefaultsModule {
}
