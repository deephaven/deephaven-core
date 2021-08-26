package io.deephaven.db.util;

import io.deephaven.base.FileUtils;
import io.deephaven.base.verify.Assert;
import io.deephaven.configuration.Configuration;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.process.ProcessEnvironment;
import io.deephaven.db.tables.select.QueryScope;
import io.deephaven.db.util.jpy.JpyInit;
import org.jpy.PyObject;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * This class is the support infrastructure for running Python remote queries.
 *
 * It is a singleton that contains an instance of a PythonHolder. All of the specially handled db operations from a
 * remote Python session should execute queries which interact wtih this class. The script sessions that run for
 * PersistentQueries or consoles are handled separately by the {@link PythonDeephavenSession}.
 */
public enum WorkerPythonEnvironment {
    DEFAULT;

    private final PythonEvaluator evaluator;
    private final PythonScope<?> scope;
    private final String pythonVersion;
    private final String[] pythonVersionParts;

    private final Logger log;

    /**
     * Create the environment using our global log and a JpyHolder.
     *
     * Runs the init script specified by WorkerPythonEnvironment.initScript.
     */
    WorkerPythonEnvironment() {
        log = ProcessEnvironment.getGlobalLog();

        JpyInit.init(log);
        PythonEvaluatorJpy jpy = PythonEvaluatorJpy.withGlobalCopy();
        evaluator = jpy;
        scope = jpy.getScope();
        pythonVersion = evaluator.getPythonVersion();
        pythonVersionParts = pythonVersion.split("\\.", 0); // NB: you have to escape "." ...
        if (pythonVersionParts.length > 2) {
            log.info().append("Worker python version ").append(pythonVersion).endl();
        } else {
            log.warn().append("Worker python version set as ").append(pythonVersion)
                    .append(" which has unexpected format (not `<major>.<minor>...` which may " +
                            "lead to unexpected errors")
                    .endl();
        }

        PythonLogAdapter.interceptOutputStreams(evaluator);

        final String defaultScriptPath = Configuration.getInstance()
                .getProperty("WorkerPythonEnvironment.defaultScriptPath")
                .replace("<devroot>", Configuration.getInstance().getDevRootPath())
                .replace("<workspace>", Configuration.getInstance().getWorkspacePath());

        final ScriptFinder scriptFinder = new ScriptFinder(defaultScriptPath);
        final String initScript = Configuration.getInstance().getStringWithDefault("WorkerPythonEnvironment.initScript",
                "core/deephaven_jpy_init.py");

        final ScriptFinder.FileOrStream file;
        try {
            file = scriptFinder.findScriptEx(initScript);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        if (file.getFile().isPresent()) {
            try {
                evaluator.runScript(file.getFile().get().getAbsolutePath());
            } catch (FileNotFoundException fnfe) {
                throw new UncheckedIOException(fnfe);
            }
        } else {
            Assert.assertion(file.getStream().isPresent(), "file.getStream().isPresent()");

            final String scriptText;
            try {
                scriptText = FileUtils.readTextFile(file.getStream().get());
            } catch (IOException ioe) {
                throw new UncheckedIOException(ioe);
            }
            evaluator.evalScript(scriptText);
        }
    }

    /**
     * Retrieves a value from our Python holder's globals.
     *
     * When the object is a convertible PyObject; we return the PyObject. Otherwise, we'll return a
     * PythonRemoteQuery.PickledResult, which is suitable for unpickling by the remote side.
     *
     * The caller should never serialize an unconverted PyObject; it contains a raw pointer and will result in a Hotspot
     * or memory corruption on the remote side.
     *
     * @param name the variable to retrieve
     * @return the variable as a Java object; or pickled
     */
    public Object fetch(String name) {
        Object x;
        boolean tryPickle = false;

        x = getValue(name);
        if (x instanceof PyObject) {
            if (!((PyObject) x).isConvertible()) {
                tryPickle = true;
            }
        }

        if (tryPickle) {
            log.info().append("Variable ").append(name).append(" requires pickling.").endl();
            evaluator.evalStatement("__resultDill__ = dill.dumps(__result__)");
            if (pythonVersionParts[0].equals("2")) {
                // In python2, we have that str is 8 bits, and base64.b64encode produces a str
                evaluator.evalStatement("__resultPickled__ = base64.b64encode(__resultDill__)");
            } else {
                // In python3, we have that str is 32 bit unicode, and base64.b64encode produces a bytes (array
                // basically)
                // our next step is to cast this output as a java string, which does not work for a bytes.
                // We must make it a str, via calling .decode() on it.
                evaluator.evalStatement("__resultPickled__ = base64.b64encode(__resultDill__).decode()");
            }
            // this is the only place a base64 encoded item is cast to a string, so only fix needed
            String pickled = (String) getValue("__resultPickled__");
            x = new PickledResult(pickled, pythonVersion);
        } else {
            log.info().append("Variable ").append(name).append(" is of type ").append(x.getClass().getCanonicalName())
                    .endl();
        }

        return x;
    }

    /**
     * Evaluates the given string as a statement.
     *
     * @param evalString the statement to evaluate
     */
    public void eval(String evalString) {
        evaluator.evalStatement(evalString);
    }

    /**
     * Retrieves a value from the Python holder, with default conversion, but no (un)pickling.
     *
     * Used for unit testing.
     *
     * @param variable the variable name to retrieve
     * @return the value from the Python holder's environment.
     */
    Object getValue(String variable) {
        return scope
                .getValue(variable)
                .orElseThrow(() -> new QueryScope.MissingVariableException("No variable: " + variable));
    }
}

