/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.util;

import io.deephaven.base.FileUtils;
import io.deephaven.base.verify.Assert;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.exceptions.CancellationException;
import io.deephaven.engine.table.lang.QueryLibrary;
import io.deephaven.engine.table.lang.QueryScope;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.PythonDeephavenSession.PythonSnapshot;
import io.deephaven.engine.util.scripts.ScriptPathLoader;
import io.deephaven.engine.util.scripts.ScriptPathLoaderState;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.plugin.type.ObjectTypeLookup;
import io.deephaven.plugin.type.ObjectTypeLookup.NoOp;
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
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A ScriptSession that uses a JPy cpython interpreter internally.
 *
 * This is used for applications or the console; Python code running remotely uses WorkerPythonEnvironment for it's
 * supporting structures.
 */
public class PythonDeephavenSession extends AbstractScriptSession<PythonSnapshot> implements ScriptSession {
    private static final Logger log = LoggerFactory.getLogger(PythonDeephavenSession.class);

    private static final String DEFAULT_SCRIPT_PATH = Configuration.getInstance()
            .getProperty("PythonDeephavenSession.defaultScriptPath")
            .replace("<devroot>", Configuration.getInstance().getDevRootPath())
            .replace("<workspace>", Configuration.getInstance().getWorkspacePath());

    public static String SCRIPT_TYPE = "Python";

    private final PythonScriptSessionModule module;

    private final ScriptFinder scriptFinder;
    private final PythonEvaluator evaluator;
    private final PythonScope<PyObject> scope;

    /**
     * Create a Python ScriptSession.
     *
     * @param objectTypeLookup the object type lookup
     * @param runInitScripts if init scripts should be executed
     * @throws IOException if an IO error occurs running initialization scripts
     */
    public PythonDeephavenSession(ObjectTypeLookup objectTypeLookup, boolean runInitScripts)
            throws IOException {
        this(objectTypeLookup, null, runInitScripts, false);
    }

    /**
     * Create a Python ScriptSession.
     *
     * @param objectTypeLookup the object type lookup
     * @param listener an optional listener that will be notified whenever the query scope changes
     * @param runInitScripts if init scripts should be executed
     * @param isDefaultScriptSession true if this is in the default context of a worker jvm
     * @throws IOException if an IO error occurs running initialization scripts
     */
    public PythonDeephavenSession(
            ObjectTypeLookup objectTypeLookup, @Nullable final Listener listener, boolean runInitScripts,
            boolean isDefaultScriptSession)
            throws IOException {
        super(objectTypeLookup, listener, isDefaultScriptSession);
        PythonEvaluatorJpy jpy = PythonEvaluatorJpy.withGlobalCopy();
        evaluator = jpy;
        scope = jpy.getScope();
        this.module = (PythonScriptSessionModule) PyModule.importModule("deephaven2.server.script_session")
                .createProxy(CallableKind.FUNCTION, PythonScriptSessionModule.class);
        this.scriptFinder = new ScriptFinder(DEFAULT_SCRIPT_PATH);

        /*
         * We redirect the standard Python sys.stdout and sys.stderr streams to our log object.
         */
        PythonLogAdapter.interceptOutputStreams(evaluator);

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

        final QueryLibrary currLibrary = QueryLibrary.getLibrary();
        try {
            QueryLibrary.setLibrary(queryLibrary);
            QueryLibrary.importClass(org.jpy.PyObject.class);
        } finally {
            QueryLibrary.setLibrary(currLibrary);
        }
    }

    /**
     * Creates a Python "{@link ScriptSession}", for use where we should only be reading from the scope, such as an
     * IPython kernel session.
     */
    public PythonDeephavenSession(PythonScope<?> scope) {
        super(NoOp.INSTANCE, null, false);
        this.scope = (PythonScope<PyObject>) scope;
        this.module = null;
        this.evaluator = null;
        this.scriptFinder = null;
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
                .orElseThrow(() -> new QueryScope.MissingVariableException("No global variable for: " + name));
    }

    @Override
    public <T> T getVariable(String name, T defaultValue) {
        return scope
                .<T>getValueUnchecked(name)
                .orElse(defaultValue);
    }

    @Override
    protected void evaluate(String command, String scriptName) {
        log.info().append("Evaluating command: " + command).endl();
        try {
            UpdateGraphProcessor.DEFAULT.exclusiveLock().doLockedInterruptibly(() -> {
                evaluator.evalScript(command);
            });
        } catch (InterruptedException e) {
            throw new CancellationException(e.getMessage() != null ? e.getMessage() : "Query interrupted", e);
        }
    }

    @Override
    public Map<String, Object> getVariables() {
        return Collections.unmodifiableMap(scope.getEntriesMap());
    }

    protected static class PythonSnapshot implements Snapshot, Closeable {

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
        return new PythonSnapshot(scope.globals().copy());
    }

    @Override
    protected Changes createDiff(PythonSnapshot from, PythonSnapshot to, RuntimeException e) {
        // TODO(deephaven-core#1775): multivariate jpy (unwrapped) return type into java
        // It would be great if we could push down the maybeUnwrap logic into create_change_list (it could handle the
        // unwrapping), but we are unable to tell jpy that we want to unwrap JType objects, but pass back python objects
        // as PyObject.
        try (
                PythonSnapshot fromSnapshot = from;
                PythonSnapshot toSnapshot = to;
                PyObject changes = module.create_change_list(fromSnapshot.dict.unwrap(), toSnapshot.dict.unwrap())) {
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

    private Object maybeUnwrap(PyObject o) {
        if (o == null) {
            return null;
        }
        final Object javaObject = module.unwrap_to_java_type(o);
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
        final PythonSnapshot fromSnapshot = takeSnapshot();
        final PyDictWrapper globals = scope.globals();
        if (newValue == null) {
            try {
                globals.delItem(name);
            } catch (KeyError key) {
                // ignore
            }
        } else {
            globals.setItem(name, newValue);
        }
        final PythonSnapshot toSnapshot = takeSnapshot();
        applyDiff(fromSnapshot, toSnapshot, null);
    }

    @Override
    public String scriptType() {
        return SCRIPT_TYPE;
    }

    @Override
    public void onApplicationInitializationBegin(Supplier<ScriptPathLoader> pathLoader,
            ScriptPathLoaderState scriptLoaderState) {}

    @Override
    public void onApplicationInitializationEnd() {}

    @Override
    public void setScriptPathLoader(Supplier<ScriptPathLoader> scriptPathLoader, boolean caching) {}

    @Override
    public void clearScriptPathLoader() {}

    @Override
    public boolean setUseOriginalScriptLoaderState(boolean useOriginal) {
        return true;
    }

    // TODO core#41 move this logic into the python console instance or scope like this - can go further and move
    // isWidget too
    @Override
    public Object unwrapObject(Object object) {
        if (object instanceof PyObject) {
            final PyObject pyObject = (PyObject) object;
            final Object unwrapped = module.unwrap_to_java_type(pyObject);
            if (unwrapped != null) {
                return unwrapped;
            }
        }

        return object;
    }

    interface PythonScriptSessionModule extends Closeable {
        PyObject create_change_list(PyObject from, PyObject to);

        Object unwrap_to_java_type(PyObject object);

        void close();
    }
}
