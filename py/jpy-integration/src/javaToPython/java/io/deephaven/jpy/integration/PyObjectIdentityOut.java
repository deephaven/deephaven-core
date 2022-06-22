/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.jpy.integration;

import io.deephaven.jpy.integration.SomeJavaClassOutTest.SomeJavaClass;
import org.jpy.PyObject;

interface PyObjectIdentityOut extends IdentityOut {
    PyObject identity(int object);

    PyObject identity(Integer object);

    PyObject identity(String object);

    PyObject identity(SomeJavaClass object);

    PyObject identity(int[] object);

    PyObject identity(Integer[] object);

    PyObject identity(String[] object);

    PyObject identity(SomeJavaClass[] object);

    PyObject identity(PyObject object);

    PyObject identity(Object object);
}
