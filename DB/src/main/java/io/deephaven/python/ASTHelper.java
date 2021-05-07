package io.deephaven.python;

import java.util.List;
import java.util.stream.Collectors;
import org.jpy.PyObject;

/**
 * See ast_helper.py
 */
public interface ASTHelper extends AutoCloseable {

    // IDS-6072
    /*
     * java.lang.RuntimeException: Error in Python interpreter:
     * Type: <type 'exceptions.ValueError'>
     * Value: cannot convert a Python 'list' to a Java 'java.util.List'
     * Line: <not available>
     * Namespace: <not available>
     * File: <not available>
     *  at org.jpy.PyLib.callAndReturnValue(Native Method)
     *  at org.jpy.PyProxyHandler.invoke(PyProxyHandler.java:80)
     *  at io.deephaven.python.$Proxy7.extract_expression_names(Unknown Source)
     *  at io.deephaven.python.ExtractFormulaNamesTest.extract_expression_names(ExtractFormulaNamesTest.java:26)
     */
    //List<String> extract_expression_names(String pythonExpression);

    /**
     * Extract names from an expression string
     *
     * @param pythonExpression the python expression, of the form <expression>
     * @return a sorted list of the named elements from <expression>
     */
    PyObject extract_expression_names(String pythonExpression);

    @Override
    void close();

    /**
     * A helper to convert the returned results.
     *
     * <p>A workaround until <a href="IDS-6072">IDS-6072</a> is addressed.
     *
     * @param object the python object, as returned from {@link #extract_expression_names(String)}
     * @return the list
     */
    static List<String> convertToList(PyObject object) {
        final List<PyObject> list = object.asList();
        for (PyObject pyObject : list) {
            if (!pyObject.isString()) {
                throw new ClassCastException("Expecting all items in the list to be python strings");
            }
        }
        return list.stream().map(PyObject::str).collect(Collectors.toList());
    }
}
