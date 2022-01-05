package io.deephaven.server.plugin;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.plugin.PluginCallback;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.server.console.ConsoleServiceGrpcImpl;
import io.deephaven.server.plugin.java.JavaServiceLoader;
import io.deephaven.server.plugin.python.PythonModuleLoader;

import javax.inject.Inject;
import java.util.Objects;

public final class PluginsLoader implements PluginCallback {
    private static final Logger log = LoggerFactory.getLogger(PluginsLoader.class);

    private final ObjectTypes types;

    @Inject
    public PluginsLoader(ObjectTypes types) {
        this.types = Objects.requireNonNull(types);
    }

    public void registerAll() {
        log.info().append("Registering plugins...").endl();
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

    @Override
    public void registerObjectType(ObjectType objectType) {
        log.info().append("Registering object type: ")
                .append(objectType.name()).append(" / ")
                .append(objectType.toString())
                .endl();
        types.register(objectType);
    }

    private class Counting implements PluginCallback, LogOutputAppendable {

        private int objectTypeCount = 0;

        @Override
        public void registerObjectType(ObjectType objectType) {
            PluginsLoader.this.registerObjectType(objectType);
            ++objectTypeCount;
        }

        @Override
        public LogOutput append(LogOutput logOutput) {
            return logOutput.append("objectType=").append(objectTypeCount);
        }
    }
}
