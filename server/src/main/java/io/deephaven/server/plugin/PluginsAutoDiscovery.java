package io.deephaven.server.plugin;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.plugin.Plugin;
import io.deephaven.plugin.Registration;
import io.deephaven.plugin.Registration.Callback;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.server.console.ConsoleServiceGrpcImpl;
import io.deephaven.server.plugin.java.JavaServiceLoader;
import io.deephaven.server.plugin.python.PythonModuleLoader;
import io.deephaven.util.annotations.VisibleForTesting;

import javax.inject.Inject;
import java.util.Objects;

/**
 * Provides a {@link #registerAll()} entrypoint for {@link Registration} auto-discovery. Logs auto-discovered details.
 */
public final class PluginsAutoDiscovery {
    private static final Logger log = LoggerFactory.getLogger(PluginsAutoDiscovery.class);

    private final Registration.Callback callback;

    @Inject
    public PluginsAutoDiscovery(Registration.Callback callback) {
        this.callback = Objects.requireNonNull(callback);
    }

    /**
     * Registers {@link Registration plugins} via {@link JavaServiceLoader#initializeAllAndRegisterInto(Callback)} and
     * {@link PythonModuleLoader#allRegisterInto(Callback)} (if python is enabled).
     */
    public void registerAll() {
        registerAll(ConsoleServiceGrpcImpl.isPythonSession());
    }

    @VisibleForTesting
    public void registerAll(boolean includePython) {
        log.info().append("Registering plugins...").endl();
        // TODO(deephaven-core#1810): Use service loader to abstract the different plugin auto-discovery methods
        final Counting serviceLoaderCount = new Counting();
        JavaServiceLoader.initializeAllAndRegisterInto(serviceLoaderCount);
        final Counting pythonModuleCount = new Counting();
        if (includePython) {
            PythonModuleLoader.allRegisterInto(pythonModuleCount);
        }
        log.info().append("Registered via service loader: ").append(serviceLoaderCount).endl();
        if (includePython) {
            log.info().append("Registered via python modules: ").append(pythonModuleCount).endl();
        }
    }

    private class Counting implements Registration.Callback, LogOutputAppendable, Plugin.Visitor<Counting> {

        private int objectTypeCount = 0;

        @Override
        public void register(Plugin plugin) {
            plugin.walk(this);
        }

        @Override
        public Counting visit(ObjectType objectType) {
            log.info().append("Registering object type: ")
                    .append(objectType.name()).append(" / ")
                    .append(objectType.toString())
                    .endl();
            callback.register(objectType);
            ++objectTypeCount;
            return this;
        }

        @Override
        public LogOutput append(LogOutput logOutput) {
            return logOutput.append("objectType=").append(objectTypeCount);
        }
    }
}
