package io.deephaven.server.plugin;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.plugin.PluginCallback;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeCallback;
import io.deephaven.server.console.ConsoleServiceGrpcImpl;
import io.deephaven.server.plugin.java.JavaServiceLoader;
import io.deephaven.server.plugin.python.PythonModuleLoader;

import javax.inject.Inject;
import java.util.Objects;

/**
 * Provides a {@link #registerAll()} entrypoint for {@link io.deephaven.plugin.Plugin} auto-discovery. Logs
 * auto-discovered details.
 */
public final class PluginsAutoDiscovery {
    private static final Logger log = LoggerFactory.getLogger(PluginsAutoDiscovery.class);

    private final ObjectTypeCallback types;

    @Inject
    public PluginsAutoDiscovery(ObjectTypeCallback types) {
        this.types = Objects.requireNonNull(types);
    }

    /**
     * Registers {@link io.deephaven.plugin.Plugin plugins} via
     * {@link JavaServiceLoader#allRegisterInto(PluginCallback)} and
     * {@link PythonModuleLoader#allRegisterInto(PluginCallback)} (if python is enabled).
     */
    public void registerAll() {
        log.info().append("Registering plugins...").endl();
        // TODO(deephaven-core#1810): Use service loader to abstract the different plugin auto-discovery methods
        final Counting serviceLoaderCount = new Counting();
        JavaServiceLoader.allRegisterInto(serviceLoaderCount);
        final Counting pythonModuleCount = new Counting();
        if (ConsoleServiceGrpcImpl.isPythonSession()) {
            PythonModuleLoader.allRegisterInto(pythonModuleCount);
        }
        log.info().append("Registered via service loader: ").append(serviceLoaderCount).endl();
        if (ConsoleServiceGrpcImpl.isPythonSession()) {
            log.info().append("Registered via python modules: ").append(pythonModuleCount).endl();
        }
    }

    private class Counting implements PluginCallback, LogOutputAppendable {

        private int objectTypeCount = 0;

        @Override
        public void registerObjectType(ObjectType objectType) {
            log.info().append("Registering object type: ")
                    .append(objectType.name()).append(" / ")
                    .append(objectType.toString())
                    .endl();
            types.registerObjectType(objectType);
            ++objectTypeCount;
        }

        @Override
        public LogOutput append(LogOutput logOutput) {
            return logOutput.append("objectType=").append(objectTypeCount);
        }
    }
}
