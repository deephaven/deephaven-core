package io.deephaven.jpy.integration;

import org.jpy.PyObject;

public class PassPyObjectToJava {
    public static void from_python_with_love(PyObject object) {
        System.out.println(
            String.format("Received PyObject, pointer=%d, str=%s", object.getPointer(), object));
    }

    public static void from_python_with_love_var(PyObject... varArgs) {
        for (PyObject arg : varArgs) {
            System.out.println(
                String.format("Received PyObject(s), pointer=%d, str=%s", arg.getPointer(), arg));
        }
    }

    interface TheContext {
        int plus(int other);
    }

    public static int invoke_the_context_plus(PyObject object, int other) {
        // The easiest way to invoke functions on a python object is to have the python object be a
        // class, and proxy it to a java interface!
        TheContext theContext = object.createProxy(TheContext.class);
        return theContext.plus(other);
    }

    public static String overload_test_1(PyObject pyObject) {
        return "PyObject";
    }

    public static String overload_test_1(String string) {
        return "String";
    }
}
