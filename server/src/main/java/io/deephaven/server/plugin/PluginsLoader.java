package io.deephaven.server.plugin;

import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.plugin.Plugin;
import io.deephaven.plugin.PluginCallback;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeBase;
import io.deephaven.plugin.type.Exporter;
import io.deephaven.server.console.ConsoleServiceGrpcImpl;
import org.jpy.PyLib.CallableKind;
import org.jpy.PyModule;
import org.jpy.PyObject;

import javax.inject.Inject;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;
import java.util.ServiceLoader;

public final class PluginsLoader implements PluginCallback {
    private static final Logger log = LoggerFactory.getLogger(PluginsLoader.class);

    private static void allServiceLoaderRegisterInto(PluginCallback callback) {
        for (Plugin provider : ServiceLoader.load(Plugin.class)) {
            provider.registerInto(callback);
        }
    }

    private static void allPythonRegisterInto(PluginCallback callback) {
        try (final PythonPluginModule module = PythonPluginModule.of()) {
            module.all_plugins_register_into(new CallbackAdapter(callback));
        }
    }

    public final ObjectTypes types;

    @Inject
    public PluginsLoader(ObjectTypes types) {
        this.types = Objects.requireNonNull(types);
    }

    public void registerAll() {
        log.info().append("Registering plugins...").endl();
        final Counting serviceLoaderCount = new Counting();
        allServiceLoaderRegisterInto(serviceLoaderCount);
        final Counting pythonModuleCount = new Counting();
        if (ConsoleServiceGrpcImpl.isPythonSession()) {
            allPythonRegisterInto(pythonModuleCount);
        }
        log.info().append("Registered via service loader: ").append(serviceLoaderCount.count).endl();
        if (ConsoleServiceGrpcImpl.isPythonSession()) {
            log.info().append("Registered via python module: ").append(pythonModuleCount.count).endl();
        }
    }

    @Override
    public void registerCustomType(ObjectType objectType) {
        log.info().append("Registering plugin object type: ")
                .append(objectType.name()).append(" / ")
                .append(objectType.toString())
                .endl();
        types.register(objectType);
    }

    private class Counting implements PluginCallback {

        private int count = 0;

        @Override
        public void registerCustomType(ObjectType objectType) {
            PluginsLoader.this.registerCustomType(objectType);
            ++count;
        }
    }

    interface PythonCustomType {

        static PythonCustomType of(PyObject object) {
            return (PythonCustomType) object.createProxy(CallableKind.FUNCTION, PythonCustomType.class);
        }

        String name();

        boolean is_type(PyObject object);

        // TODO(deephaven-core#1785): Use more pythonic wrapper for io.deephaven.plugin.type.Exporter
        byte[] to_bytes(Exporter exporter, PyObject object);
    }

    private static final class Adapter extends ObjectTypeBase {

        public static Adapter of(PythonCustomType module) {
            return new Adapter(module.name(), module);
        }

        private final String name;
        private final PythonCustomType module;

        private Adapter(String name, PythonCustomType module) {
            this.name = Objects.requireNonNull(name);
            this.module = Objects.requireNonNull(module);
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public boolean isType(Object o) {
            if (!(o instanceof PyObject)) {
                return false;
            }
            return module.is_type((PyObject) o);
        }

        @Override
        public void writeToTypeChecked(Exporter exporter, Object object, OutputStream out) throws IOException {
            out.write(module.to_bytes(exporter, (PyObject) object));
        }

        @Override
        public String toString() {
            return name + ":" + module;
        }
    }

    private static class CallbackAdapter {
        private final PluginCallback callback;

        public CallbackAdapter(PluginCallback callback) {
            this.callback = Objects.requireNonNull(callback);
        }

        @SuppressWarnings("unused")
        public void register_custom_type(PyObject module) {
            final PythonCustomType pythonCustomType = PythonCustomType.of(module);
            final Adapter adapter = Adapter.of(pythonCustomType);
            callback.registerCustomType(adapter);
        }
    }

    interface PythonPluginModule extends AutoCloseable {

        static PythonPluginModule of() {
            return (PythonPluginModule) PyModule.importModule("deephaven.plugin")
                    .createProxy(CallableKind.FUNCTION, PythonPluginModule.class);
        }

        void all_plugins_register_into(CallbackAdapter callback);

        @Override
        void close();
    }
}
