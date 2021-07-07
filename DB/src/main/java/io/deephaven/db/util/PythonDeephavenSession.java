package io.deephaven.db.util;

import io.deephaven.base.FileUtils;
import io.deephaven.base.verify.Assert;
import io.deephaven.configuration.Configuration;
import io.deephaven.db.exceptions.OperationException;
import io.deephaven.db.exceptions.QueryCancellationException;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.libs.QueryLibrary;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.select.QueryScope;
import io.deephaven.db.tables.utils.LiveWidget;
import io.deephaven.db.util.jpy.JpyInit;
import io.deephaven.db.util.scripts.ScriptPathLoader;
import io.deephaven.db.util.scripts.ScriptPathLoaderState;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.lang.parse.api.CompletionParseService;
import io.deephaven.lang.parse.api.Languages;
import io.deephaven.util.annotations.VisibleForTesting;
import org.jpy.KeyError;
import org.jpy.PyDictWrapper;
import org.jpy.PyObject;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A ScriptSession that uses a JPy cpython interpreter internally.
 *
 * This is used for persistent queries or the DB console; Python code running remotely uses WorkerPythonEnvironment
 * for it's supporting structures.
 */
public class PythonDeephavenSession extends AbstractScriptSession implements ScriptSession {
    private static final Logger log = LoggerFactory.getLogger(PythonDeephavenSession.class);

    private static final String DEFAULT_SCRIPT_PATH = Configuration.getInstance()
            .getProperty("PythonDeephavenSession.defaultScriptPath")
            .replace("<devroot>", Configuration.getInstance().getDevRootPath())
            .replace("<workspace>", Configuration.getInstance().getWorkspacePath());

    private final ScriptFinder scriptFinder;
    private final PythonEvaluator evaluator;
    private final PythonScope<?> scope;

    /**
     * Create a Python ScriptSession.
     *
     * @param runInitScripts if init scripts should be executed
     * @throws IOException if an IO error occurs running initialization scripts
     */
    public PythonDeephavenSession(boolean runInitScripts) throws IOException {
        this(runInitScripts, false);
    }

    /**
     * Create a Python ScriptSession.
     *
     * @param runInitScripts if init scripts should be executed
     * @param isDefaultScriptSession true if this is in the default context of a worker jvm
     * @throws IOException if an IO error occurs running initialization scripts
     */
    public PythonDeephavenSession(boolean runInitScripts, boolean isDefaultScriptSession) throws IOException {
        super(isDefaultScriptSession);

        JpyInit.init(log);
        PythonEvaluatorJpy jpy = PythonEvaluatorJpy.withGlobalCopy();
        evaluator = jpy;
        scope = jpy.getScope();
        this.scriptFinder = new ScriptFinder(DEFAULT_SCRIPT_PATH);

        /*
         * We redirect the standard Python sys.stdout and sys.stderr streams to our log object.
         */
        PythonLogAdapter.interceptOutputStreams(evaluator);

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
     * Creates a Python "{@link ScriptSession}", for use where we should only be reading from the
     * scope, such as an IPython kernel session.
     */
    public PythonDeephavenSession(PythonScope<?> scope) {
        super(false);

        this.scope = scope;
        this.evaluator = null;
        this.scriptFinder = null;
    }

    @Override
    @VisibleForTesting
    public QueryScope newQueryScope() {
        // depend on the GIL instead of local synchronization
        return new QueryScope.UnsynchronizedScriptSessionImpl(this);
    }

    /**
     * Finds the specified script; and runs it as a file, or if it is a stream writes it to a temporary file in order
     * to run it.
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
            LiveTableMonitor.DEFAULT.exclusiveLock().doLockedInterruptibly(() -> {
                evaluator.evalScript(command);
            });
        } catch (InterruptedException e) {
            throw new QueryCancellationException(e.getMessage() != null ? e.getMessage() : "Query interrupted" , e);
        }
    }

    @Override
    public Map<String, Object> getVariables() {
        return Collections.unmodifiableMap(scope.getEntriesMap());
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
    public void setVariable(String name, Object value) {
        PyDictWrapper globals = scope.globals();
        if (value == null) {
            try {
                globals.delItem(name);
            } catch (KeyError key) {
                // ignore
            }
        } else {
            globals.setItem(name, value);
        }
    }

    @Override
    public String scriptType() { return Languages.LANGUAGE_PYTHON; }

    @Override
    public void onApplicationInitializationBegin(Supplier<ScriptPathLoader> pathLoader, ScriptPathLoaderState scriptLoaderState) {
    }

    @Override
    public void onApplicationInitializationEnd() {
    }

    @Override
    public void setScriptPathLoader(Supplier<ScriptPathLoader> scriptPathLoader, boolean caching) {
    }

    @Override
    public void clearScriptPathLoader() {
    }

    @Override
    public boolean setUseOriginalScriptLoaderState(boolean useOriginal) {
        return true;
    }

    //TODO core#41 move this logic into the python console instance or scope like this - can go further and move isWidget too
    @Override
    public Object unwrapObject(Object object) {
        if (object instanceof PyObject) {
            final PyObject pyObject = (PyObject) object;
            if (isWidget(pyObject)) {
                return getWidget(pyObject);
            } else if (isTable(pyObject)) {
                return getTable(pyObject);
            }
        }

        return object;
    }

    private static final String GET_WIDGET_ATTRIBUTE = "getWidget";
    private static boolean isWidget(PyObject value) {
        if ((value != null && value.hasAttribute(GET_WIDGET_ATTRIBUTE))) {
            try (final PyObject widget = value.callMethod(GET_WIDGET_ATTRIBUTE)) {
                return !widget.isNone();
            }
        }

        return false;
    }
    private static LiveWidget getWidget(PyObject pyObject) {
        boolean isWidget = pyObject.hasAttribute(GET_WIDGET_ATTRIBUTE);
        if (isWidget) {
            try (final PyObject widget = pyObject.callMethod(GET_WIDGET_ATTRIBUTE)) {
                if (!widget.isNone()) {
                    return (LiveWidget) widget.getObjectValue();
                }
            }
        }

        throw new OperationException("Can not convert pyOjbect=" + pyObject + " to a LiveWidget.");
    }

    private static final String GET_TABLE_ATTRIBUTE = "get_dh_table";
    private static boolean isTable(PyObject value) {
        if ((value != null && value.hasAttribute(GET_TABLE_ATTRIBUTE))) {
            try (final PyObject widget = value.callMethod(GET_TABLE_ATTRIBUTE)) {
                return !widget.isNone();
            }
        }

        return false;
    }
    private static Table getTable(PyObject pyObject) {
        if (pyObject.hasAttribute(GET_TABLE_ATTRIBUTE)) {
            try (final PyObject widget = pyObject.callMethod(GET_TABLE_ATTRIBUTE)) {
                if (!widget.isNone()) {
                    return (Table) widget.getObjectValue();
                }
            }
        }

        throw new OperationException("Can not convert pyObject=" + pyObject + " to a Table.");
    }

}
