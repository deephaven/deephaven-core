package io.deephaven.engine.util;

import java.io.FileNotFoundException;

/**
 * The {@link PythonEvaluator} encapsulates the evaluation around a Python library instance.
 */
public interface PythonEvaluator {
    /**
     * Evaluate the provided statement.
     *
     * Not suitable for use with scripts; because multiple statements will be ignored.
     *
     * @param s the string to evaluate
     */
    void evalStatement(String s);

    /**
     * Evaluate the provided statements.
     *
     * If you've got a multi-line script; you must use this version.
     *
     * @param s the string to evaluate
     */
    void evalScript(String s);

    /**
     * Runs a Python script.
     *
     * @param scriptFile the file to execute
     * @throws FileNotFoundException if scriptFile was not found
     */
    void runScript(String scriptFile) throws FileNotFoundException;

    /**
     * Sets the variable within our session.
     *
     * @param name the name of the variable to set.
     * @param value the value of the variable.
     */
    void set(String name, Object value);

    /**
     * Gets the Python interpreter version string.
     *
     * @return The Python interpreter version string.
     */
    String getPythonVersion();
}
