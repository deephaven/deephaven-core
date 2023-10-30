/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.integrations.python;

import io.deephaven.base.FileUtils;
import io.deephaven.base.verify.Assert;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.exceptions.CancellationException;
import io.deephaven.engine.context.QueryScope;
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
import io.deephaven.util.annotations.VisibleForTesting;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jpy.KeyError;
import org.jpy.PyDictWrapper;
import org.jpy.PyInputMode;
import org.jpy.PyLib.CallableKind;
import org.jpy.PyModule;
import org.jpy.PyObject;

import java.io.Closeable;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A ScriptSession that uses a JPy cpython interpreter internally.
 * <p>
 * This is used for applications or the console; Python code running remotely uses WorkerPythonEnvironment for it's
 * supporting structures.
 */
public class PythonDeephavenSession extends AbstractScriptSession<PythonSnapshot> {
    private static final Logger log = LoggerFactory.getLogger(PythonDeephavenSession.class);

    private static final String DEFAULT_SCRIPT_PATH = Configuration.getInstance()
            .getStringWithDefault("PythonDeephavenSession.defaultScriptPath", ".");

    public static String SCRIPT_TYPE = "Python";

    private final PythonEvaluator evaluator;
    private final PythonScope<PyObject> scope;
    private final PythonScriptSessionModule module;
    private final ScriptFinder scriptFinder;

    /**
     * Create a Python ScriptSession.
     *
     * @param updateGraph the default update graph to install for the repl
     * @param objectTypeLookup the object type lookup
     * @param listener an optional listener that will be notified whenever the query scope changes
     * @param runInitScripts if init scripts should be executed
     * @param pythonEvaluator
     * @throws IOException if an IO error occurs running initialization scripts
     */
    public PythonDeephavenSession(
            final UpdateGraph updateGraph,
            final ObjectTypeLookup objectTypeLookup,
            @Nullable final Listener listener,
            final boolean runInitScripts,
            final PythonEvaluatorJpy pythonEvaluator) throws IOException {
        super(updateGraph, objectTypeLookup, listener);

        evaluator = pythonEvaluator;
        scope = pythonEvaluator.getScope();
        executionContext.getQueryLibrary().importClass(org.jpy.PyObject.class);
        try (final SafeCloseable ignored = executionContext.open()) {
            module = (PythonScriptSessionModule) PyModule.importModule("deephaven_internal.script_session")
                    .createProxy(CallableKind.FUNCTION, PythonScriptSessionModule.class);
        }
        scriptFinder = new ScriptFinder(DEFAULT_SCRIPT_PATH);

        publishInitial();
        /*
         * And now the user-defined initialization scripts, if any.
         */
        if (runInitScripts) {
            String[] scripts = Configuration.getInstance().getProperty("PythonDeephavenSession.initScripts").split(",");

            for (String script : scripts) {
                runScript(script);
            }
        }
    }

    /**
     * Creates a Python "{@link ScriptSession}", for use where we should only be reading from the scope, such as an
     * IPython kernel session.
     */
    public PythonDeephavenSession(
            final UpdateGraph updateGraph, final PythonScope<?> scope) {
        super(updateGraph, NoOp.INSTANCE, null);

        evaluator = null;
        this.scope = (PythonScope<PyObject>) scope;
        try (final SafeCloseable ignored = executionContext.open()) {
            module = (PythonScriptSessionModule) PyModule.importModule("deephaven_internal.script_session")
                    .createProxy(CallableKind.FUNCTION, PythonScriptSessionModule.class);
        }
        scriptFinder = null;

        publishInitial();
    }

    @Override
    @VisibleForTesting
    public QueryScope newQueryScope() {
        // depend on the GIL instead of local synchronization
        return new UnsynchronizedScriptSessionQueryScope(this);
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

    @NotNull
    @Override
    public Object getVariable(String name) throws QueryScope.MissingVariableException {
        return scope
                .getValue(name)
                .orElseThrow(() -> new QueryScope.MissingVariableException("No variable for: " + name));
    }

    @Override
    public <T> T getVariable(String name, T defaultValue) {
        return scope
                .<T>getValueUnchecked(name)
                .orElse(defaultValue);
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

    @Override
    public Map<String, Object> getVariables() {
        final Map<String, Object> outMap = new LinkedHashMap<>();
        scope.getEntriesMap().forEach((key, value) -> outMap.put(key, maybeUnwrap(value)));
        return outMap;
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
                applyVariableChangeToDiff(diff, name, maybeUnwrap(fromValue), maybeUnwrap(toValue));
            }
            return diff;
        }
    }

    private Object maybeUnwrap(Object o) {
        if (o instanceof PyObject) {
            return maybeUnwrap((PyObject) o);
        }
        return o;
    }

    private Object maybeUnwrap(PyObject o) {
        if (o == null) {
            return null;
        }
        final Object javaObject = module.javaify(o);
        if (javaObject != null) {
            return javaObject;
        }
        return o;
    }

    @Override
    public Set<String> getVariableNames() {
        return Collections.unmodifiableSet(scope.getKeys().collect(Collectors.toSet()));
    }

    @Override
    public boolean hasVariableName(String name) {
        return scope.containsKey(name);
    }

    @Override
    public synchronized void setVariable(String name, @Nullable Object newValue) {
        final PyDictWrapper globals = scope.mainGlobals();
        if (newValue == null) {
            try {
                globals.delItem(name);
            } catch (KeyError key) {
                // ignore
            }
        } else {
            if (!(newValue instanceof PyObject)) {
                newValue = PythonObjectWrapper.wrap(newValue);
            }
            globals.setItem(name, newValue);
        }

        // Observe changes from this "setVariable" (potentially capturing previous or concurrent external changes from
        // other threads)
        observeScopeChanges();
    }

    @Override
    public String scriptType() {
        return SCRIPT_TYPE;
    }

    // TODO core#41 move this logic into the python console instance or scope like this - can go further and move
    // isWidget too
    @Override
    public Object unwrapObject(Object object) {
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
}
