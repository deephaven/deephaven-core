//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.integrations.python;

import io.deephaven.base.FileUtils;
import io.deephaven.base.verify.Assert;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.exceptions.CancellationException;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.updategraph.OperationInitializer;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.util.AbstractScriptSession;
import io.deephaven.engine.util.PythonEvaluator;
import io.deephaven.engine.util.PythonEvaluatorJpy;
import io.deephaven.engine.util.PythonScope;
import io.deephaven.engine.util.ScriptFinder;
import io.deephaven.engine.util.ScriptSession;
import io.deephaven.integrations.python.PythonDeephavenSession.PythonSnapshot;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.plugin.type.ObjectTypeLookup;
import io.deephaven.plugin.type.ObjectTypeLookup.NoOp;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.ScriptApi;
import io.deephaven.util.thread.NamingThreadFactory;
import io.deephaven.util.thread.ThreadInitializationFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jpy.KeyError;
import org.jpy.PyDictWrapper;
import org.jpy.PyInputMode;
import org.jpy.PyLib;
import org.jpy.PyLib.CallableKind;
import org.jpy.PyModule;
import org.jpy.PyObject;

import java.io.Closeable;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A ScriptSession that uses a JPy cpython interpreter internally.
 */
public class PythonDeephavenSession extends AbstractScriptSession<PythonSnapshot> {
    private static final Logger log = LoggerFactory.getLogger(PythonDeephavenSession.class);

    private static final String DEFAULT_SCRIPT_PATH = Configuration.getInstance()
            .getStringWithDefault("PythonDeephavenSession.defaultScriptPath", ".");

    /**
     * This is following the convention set by io.deephaven.server.config.ConfigServiceGrpcImpl / dh-defaults.prop of
     * relaying version information to the client via Configuration properties.
     */
    private static final String PYTHON_VERSION_PROPERTY = "python.version";

    public static String SCRIPT_TYPE = "Python";

    private static void setPythonVersion(Configuration configuration) {
        final String pythonVersion;
        try (final PyModule platformModule = PyModule.importModule("platform")) {
            pythonVersion = platformModule.call(String.class, "python_version", new Class[0], new Object[0]);
        } catch (RuntimeException e) {
            log.warn(e).append("Unable to retrieve python version").endl();
            return;
        }
        configuration.setProperty(PYTHON_VERSION_PROPERTY, pythonVersion);
    }

    private final PythonEvaluator evaluator;
    private final PythonScope<PyObject> scope;
    private final PythonScriptSessionModule module;
    private final ScriptFinder scriptFinder;

    /**
     * Create a Python ScriptSession.
     *
     * <p>
     * Sets the configuration property {@value PYTHON_VERSION_PROPERTY} to the value returned from the python code
     * {@code platform.python_version()}.
     *
     * @param updateGraph the default update graph to install for the repl
     * @param operationInitializer the default operation initializer to install for the repl
     * @param objectTypeLookup the object type lookup
     * @param listener an optional listener that will be notified whenever the query scope changes
     * @param runInitScripts if init scripts should be executed
     * @param pythonEvaluator
     * @throws IOException if an IO error occurs running initialization scripts
     */
    public PythonDeephavenSession(
            final UpdateGraph updateGraph,
            final OperationInitializer operationInitializer,
            final ThreadInitializationFactory threadInitializationFactory,
            final ObjectTypeLookup objectTypeLookup,
            @Nullable final Listener listener,
            final boolean runInitScripts,
            final PythonEvaluatorJpy pythonEvaluator) throws IOException {
        super(updateGraph, operationInitializer, objectTypeLookup, listener);

        evaluator = pythonEvaluator;
        scope = pythonEvaluator.getScope();
        executionContext.getQueryLibrary().importClass(org.jpy.PyObject.class);
        try (final SafeCloseable ignored = executionContext.open()) {
            module = (PythonScriptSessionModule) PyModule.importModule("deephaven_internal.script_session")
                    .createProxy(CallableKind.FUNCTION, PythonScriptSessionModule.class);
        }
        scriptFinder = new ScriptFinder(DEFAULT_SCRIPT_PATH);

        registerJavaExecutor(threadInitializationFactory);
        publishInitial();

        final Configuration configuration = Configuration.getInstance();
        /*
         * And now the user-defined initialization scripts, if any.
         */
        if (runInitScripts) {
            String[] scripts = configuration.getProperty("PythonDeephavenSession.initScripts").split(",");

            for (String script : scripts) {
                runScript(script);
            }
        }
        setPythonVersion(configuration);
    }

    /**
     * Creates a Python "{@link ScriptSession}", for use where we should only be reading from the scope, such as an
     * IPython kernel session.
     */
    public PythonDeephavenSession(
            final UpdateGraph updateGraph,
            final OperationInitializer operationInitializer,
            final ThreadInitializationFactory threadInitializationFactory,
            final PythonScope<?> scope) {
        super(updateGraph, operationInitializer, NoOp.INSTANCE, null);

        evaluator = null;
        this.scope = (PythonScope<PyObject>) scope;
        try (final SafeCloseable ignored = executionContext.open()) {
            module = (PythonScriptSessionModule) PyModule.importModule("deephaven_internal.script_session")
                    .createProxy(CallableKind.FUNCTION, PythonScriptSessionModule.class);
        }
        scriptFinder = null;

        registerJavaExecutor(threadInitializationFactory);
        publishInitial();
    }

    private void registerJavaExecutor(ThreadInitializationFactory threadInitializationFactory) {
        // TODO (deephaven-core#4040) Temporary exec service until we have cleaner startup wiring
        try (PyModule pyModule = PyModule.importModule("deephaven.server.executors");
                final PythonDeephavenThreadsModule module = pyModule.createProxy(PythonDeephavenThreadsModule.class)) {
            NamingThreadFactory threadFactory = new NamingThreadFactory(PythonDeephavenSession.class, "serverThread") {
                @Override
                public Thread newThread(@NotNull Runnable r) {
                    return super.newThread(threadInitializationFactory.createInitializer(r));
                }
            };
            ExecutorService executorService = Executors.newFixedThreadPool(1, threadFactory);
            module._register_named_java_executor("serial", executorService::submit);
            module._register_named_java_executor("concurrent", executorService::submit);
        }
    }

    /**
     * Finds the specified script; and runs it as a file, or if it is a stream writes it to a temporary file in order to
     * run it.
     *
     * @param script the script's name
     * @throws IOException if an error occurs reading or writing the script
     */
    private void runScript(String script) throws IOException {
        final ScriptFinder.FileOrStream file = scriptFinder.findScriptEx(script);
        if (file.getFile().isPresent()) {
            evaluator.runScript(file.getFile().get().getAbsolutePath());
        } else {
            Assert.assertion(file.getStream().isPresent(), "file.getStream().isPresent()");
            final String scriptText = FileUtils.readTextFile(file.getStream().get());
            Path f = Files.createTempFile("PythonDeephavenSession", ".py");
            try (FileWriter fileWriter = new FileWriter(f.toFile())) {
                fileWriter.write(scriptText);
                fileWriter.close();

                evaluator.runScript(f.toFile().getAbsolutePath());
            } finally {
                Files.delete(f);
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <T> T getVariable(String name) throws QueryScope.MissingVariableException {
        return (T) scope
                .getValue(name)
                .orElseThrow(() -> new QueryScope.MissingVariableException("Missing variable " + name));
    }


    @SuppressWarnings("unused")
    @ScriptApi
    public void pushScope(PyObject pydict) {
        if (!pydict.isDict()) {
            throw new IllegalArgumentException("Expect a Python dict but got a" + pydict.repr());
        }
        scope.pushScope(pydict);
    }

    @SuppressWarnings("unused")
    @ScriptApi
    public void popScope() {
        scope.popScope();
    }

    @Override
    protected void evaluate(String command, String scriptName) {
        log.info().append("Evaluating command: " + command).endl();
        try {
            ExecutionContext.getContext().getUpdateGraph().exclusiveLock()
                    .doLockedInterruptibly(() -> evaluator.evalScript(command));
        } catch (InterruptedException e) {
            throw new CancellationException(e.getMessage() != null ? e.getMessage() : "Query interrupted", e);
        }
    }

    protected static class PythonSnapshot implements Snapshot, SafeCloseable {

        private final PyDictWrapper dict;

        public PythonSnapshot(PyDictWrapper dict) {
            this.dict = Objects.requireNonNull(dict);
        }

        @Override
        public void close() {
            dict.close();
        }
    }

    @Override
    protected PythonSnapshot emptySnapshot() {
        return new PythonSnapshot(PyObject.executeCode("dict()", PyInputMode.EXPRESSION).asDict());
    }

    @Override
    protected PythonSnapshot takeSnapshot() {
        return new PythonSnapshot(scope.mainGlobals().copy());
    }

    @Override
    protected Changes createDiff(PythonSnapshot from, PythonSnapshot to, RuntimeException e) {
        // TODO(deephaven-core#1775): multivariate jpy (unwrapped) return type into java
        // It would be great if we could push down the maybeUnwrap logic into create_change_list (it could handle the
        // unwrapping), but we are unable to tell jpy that we want to unwrap JType objects, but pass back python objects
        // as PyObject.
        try (PyObject changes = module.create_change_list(from.dict.unwrap(), to.dict.unwrap())) {
            final Changes diff = new Changes();
            diff.error = e;
            for (PyObject change : changes.asList()) {
                // unpack the tuple
                // (name, existing_value, new_value)
                final String name = change.call(String.class, "__getitem__", int.class, 0);
                final PyObject fromValue = change.call(PyObject.class, "__getitem__", int.class, 1);
                final PyObject toValue = change.call(PyObject.class, "__getitem__", int.class, 2);
                applyVariableChangeToDiff(diff, name, unwrapObject(fromValue), unwrapObject(toValue));
            }
            return diff;
        }
    }

    @Override
    protected Set<String> getVariableNames() {
        try (final PyDictWrapper currScope = scope.currentScope().copy()) {
            return currScope.keySet().stream()
                    .map(scope::convertStringKey)
                    .collect(Collectors.toSet());
        }
    }

    @Override
    protected boolean hasVariable(String name) {
        return scope.containsKey(name);
    }

    @Override
    protected synchronized Object setVariable(String name, @Nullable Object newValue) {
        Object old = PyLib.ensureGil(() -> {
            final PyDictWrapper globals = scope.mainGlobals();

            if (newValue == null) {
                try {
                    return globals.unwrap().callMethod("pop", name);
                } catch (KeyError key) {
                    return null;
                }
            } else {
                Object wrapped;
                if (newValue instanceof PyObject) {
                    wrapped = newValue;
                } else {
                    wrapped = PythonObjectWrapper.wrap(newValue);
                }
                // This isn't thread safe, we're relying on the GIL being kind to us (as we have historically done).
                // There is no built-in for "replace a variable and return the old one".
                Object prev = globals.get(name);
                globals.setItem(name, wrapped);
                return prev;
            }
        });

        // Observe changes from this "setVariable" (potentially capturing previous or concurrent external changes from
        // other threads)
        observeScopeChanges();

        // This doesn't return the same Java instance of PyObject, so we won't decref it properly, but
        // again, that is consistent with how we've historically treated these references.
        return old;
    }

    @Override
    protected <T> Map<String, T> getAllValues(
            @Nullable final Function<Object, T> valueMapper,
            @NotNull final QueryScope.ParamFilter<T> filter) {
        final Map<String, T> result = new HashMap<>();

        try (final PyDictWrapper currScope = scope.currentScope().copy()) {
            for (final Map.Entry<PyObject, PyObject> entry : currScope.entrySet()) {
                final String name = scope.convertStringKey(entry.getKey());
                Object value = scope.convertValue(entry.getValue());
                if (valueMapper != null) {
                    value = valueMapper.apply(value);
                }

                // noinspection unchecked
                if (filter.accept(name, (T) value)) {
                    // noinspection unchecked
                    result.put(name, (T) value);
                }
            }
        }

        return result;
    }

    @Override
    public String scriptType() {
        return SCRIPT_TYPE;
    }

    // TODO core#41 move this logic into the python console instance or scope like this - can go further and move
    // isWidget too
    @Override
    public Object unwrapObject(@Nullable Object object) {
        if (object instanceof PyObject) {
            final PyObject pyObject = (PyObject) object;
            final Object unwrapped = module.javaify(pyObject);
            if (unwrapped != null) {
                return unwrapped;
            }
        }

        return object;
    }

    interface PythonScriptSessionModule extends Closeable {
        PyObject create_change_list(PyObject from, PyObject to);

        Object javaify(PyObject object);

        void close();
    }

    interface PythonDeephavenThreadsModule extends Closeable {
        void close();

        void _register_named_java_executor(String executorName, Consumer<Runnable> execute);
    }
}
