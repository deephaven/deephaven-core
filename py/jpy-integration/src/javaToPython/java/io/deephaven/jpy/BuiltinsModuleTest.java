/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.jpy;

import org.jpy.PyObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BuiltinsModuleTest extends PythonTest {
    private BuiltinsModule builtins;

    @Before
    public void setUp() {
        builtins = BuiltinsModule.create();
    }

    @After
    public void tearDown() {
        builtins.close();
    }

    @Test
    public void dict() {
        try (final PyObject dict = builtins.dict()) {
            Assert.assertTrue(dict.isDict());
        }
    }

    @Test
    public void len() {
        try (final PyObject dict = builtins.dict()) {
            Assert.assertEquals(0, builtins.len(dict));
        }
    }

    @Test
    public void dir() {
        try (final PyObject dict = builtins.dict()) {
            try (final PyObject dir = builtins.dir(dict)) {
                Assert.assertTrue(dir.isList());
            }
        }
    }
}
