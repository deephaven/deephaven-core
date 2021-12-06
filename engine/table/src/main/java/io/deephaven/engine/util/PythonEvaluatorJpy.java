package io.deephaven.engine.util;

import java.io.FileNotFoundException;
import org.jpy.KeyError;
import org.jpy.PyDictWrapper;
import org.jpy.PyInputMode;
import org.jpy.PyLib;
import org.jpy.PyModule;
import org.jpy.PyObject;

/**
 * The sole implementation of the {@link PythonEvaluator}, using Jpy to create a cpython interpreter instance inside of
 * our JVM.
 *
 * Each evaluator has their own copy of the globals.
 */
public class PythonEvaluatorJpy implements PythonEvaluator {

    public static PythonEvaluatorJpy withGlobalCopy() {
        // TODO: We still have to reach into the __main__ dictionary to push classes and import the Deephaven
        // quasi-module
        // because after we dill the item, the undilled item has a reference to the __main__ globals() and not our
        // globals.

        // we want to create a copy of globals, which is then used to execute code for this session
        return new PythonEvaluatorJpy(PyLib.getMainGlobals().asDict().copy());
    }

    private final PyDictWrapper globals;

    private PythonEvaluatorJpy(PyDictWrapper globals) {
        this.globals = globals;
    }

    public PythonScopeJpyImpl getScope() {
        return new PythonScopeJpyImpl(globals);
    }

    @Override
    public void evalStatement(String s) {
        if (s == null) {
            return;
        }
        // noinspection EmptyTryBlock
        try (final PyObject pyObject = PyModule.executeCode(s, PyInputMode.STATEMENT, globals, null)) {

        }
    }

    @Override
    public void evalScript(String s) {
        if (s == null) {
            return;
        }
        // noinspection EmptyTryBlock
        try (final PyObject pyObject = PyModule.executeCode(s, PyInputMode.SCRIPT, globals, null)) {

        }
    }

    @Override
    public void runScript(String scriptFile) throws FileNotFoundException {
        // noinspection EmptyTryBlock
        try (final PyObject pyObject = PyModule.executeScript(scriptFile, PyInputMode.SCRIPT, globals, null)) {

        }
    }

    @Override
    public String getPythonVersion() {
        return PyLib.getPythonVersion();
    }

    /**
     * Print out or globals for debugging.
     */
    @SuppressWarnings("unused")
    private void dumpGlobals() {
        System.out.println("Globals size: " + globals.size());
        globals.forEach((k, v) -> System.out.println(k + " -> " + v));
        System.out.println("Globals done.");
    }

    @Override
    public void set(String name, Object value) {
        if (value == null) {
            try {
                globals.delItem(name);
            } catch (KeyError keyError) {
                // ignore
            }
        } else {
            globals.setItem(name, value);
        }
    }

}
