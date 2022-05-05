/*
 * Copyright 2015 Brockmann Consult GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.jpy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PyLibTest {

    @Before
    public void setUp() throws Exception {
        //PyLib.Diag.setFlags(PyLib.Diag.F_ERR);
        PyLib.startPython();
        assertEquals(true, PyLib.isPythonRunning());
    }

    @After
    public void tearDown() throws Exception {
        PyLib.stopPython();
    }

    @Test
    public void testGettingSysArgv() throws Exception {
        // Since sys.argv is really part of the C-based sys module, there
        // are special hooks embedded python systems need to call to set it up.
        PyModule sys = PyModule.importModule("sys");
        String[] argv = sys.getAttribute("argv", String[].class);
        assertNotNull(argv);
        assertEquals(1, argv.length);
        assertTrue(argv[0].isEmpty());
    }

    @Test
    public void testGetPythonVersion() throws Exception {
        String pythonVersion = PyLib.getPythonVersion();
        System.out.println("pythonVersion = " + pythonVersion);
        assertNotNull(pythonVersion);
    }

    @Test
    public void testExecScript() throws Exception {
        int exitCode = PyLib.execScript(String.format("print('%s says: \"Hello Python!\"')", PyLibTest.class.getName()));
        assertEquals(0, exitCode);
    }

    @Test
    public void testExecScriptInError() throws Exception {
        int exitCode;
        exitCode = PyLib.execScript("0 / 1");
        assertEquals(0, exitCode);
        exitCode = PyLib.execScript("1 / 0");
        assertEquals(-1, exitCode);
    }

    @Test
    public void testImportModule() throws Exception {
        long pyModule;

        pyModule = PyLib.importModule("os");
        assertTrue(pyModule != 0);
        PyLib.decRef(pyModule);

        pyModule = PyLib.importModule("sys");
        assertTrue(pyModule != 0);
        PyLib.decRef(pyModule);
    }

    @Test
    public void testGetSetAttributeValue() throws Exception {
        long pyModule;
        long pyObject;

        pyModule = PyLib.importModule("jpy");
        try {
            assertTrue(pyModule != 0);

            long pyObj = PyLib.getAttributeObject(pyModule, "JType");
            assertTrue(pyObj != 0);
            PyLib.decRef(pyObj);

            PyLib.setAttributeValue(pyModule, "_hello", "Hello Python!", String.class);
            assertEquals("Hello Python!",
                PyLib.getAttributeValue(pyModule, "_hello", String.class));

            pyObject = PyLib.getAttributeObject(pyModule, "_hello");
            try {
                assertTrue(pyObject != 0);
            } finally {
                PyLib.decRef(pyObject);
            }
        } finally {
            PyLib.decRef(pyModule);
        }
    }

    @Test
    public void testCallAndReturnValue() throws Exception {
        long builtins;
        try {
            //Python 3.3
            builtins = PyLib.importModule("builtins");
        } catch (Exception e) {
            //Python 2.7
            builtins = PyLib.importModule("__builtin__");
        }
        try {
            assertTrue(builtins != 0);

            long max = PyLib.getAttributeObject(builtins, "max");
            try {
                assertTrue(max != 0);
            } finally {
                PyLib.decRef(max);
            }

            //PyLib.Diag.setFlags(PyLib.Diag.F_ALL);
            String result = PyLib.callAndReturnValue(builtins, false, "max", 2, new Object[]{"A", "Z"}, new Class[]{String.class, String.class}, String.class);
            assertEquals("Z", result);
        } finally {
            PyLib.decRef(builtins);
        }
    }

    @Test
    public void testCallAndReturnObject() throws Exception {
        long builtins;
        try {
            //Python 3.3
            builtins = PyLib.importModule("builtins");
        } catch (Exception e) {
            //Python 2.7
            builtins = PyLib.importModule("__builtin__");
        }
        try {
            assertTrue(builtins != 0);

            long max = PyLib.getAttributeObject(builtins, "max");
            assertTrue(max != 0);
            PyLib.decRef(max);

            long pointer = PyLib.callAndReturnObject(builtins, false, "max", 2, new Object[]{"A", "Z"}, null);
            try {
                assertTrue(pointer != 0);

                //PyLib.Diag.setFlags(PyLib.Diag.F_ALL);
                assertEquals("Z", new PyObject(pointer).getStringValue());
            } finally {
                PyLib.decRef(pointer);
            }
        } finally {
            PyLib.decRef(builtins);
        }
    }

    @Test
    public void testGetMainGlobals() throws Exception {
        PyObject globals = PyLib.getMainGlobals();
        Map<PyObject, PyObject> dict = globals.asDict();
        assertFalse(dict.isEmpty());

        boolean foundName = false;

        for (Map.Entry<PyObject, PyObject> entry : dict.entrySet()) {
            if (entry.getKey().isString()) {
                if (entry.getKey().getObjectValue().equals("__name__")) {
                    foundName = true;
                    break;
                }
            }
        }

        assertTrue(foundName);
    }

    @Test
    public void testNewDict() throws Exception {
        PyObject dict = PyLib.newDict();
        assertTrue(dict.asDict().isEmpty());

        PyObject globals = PyLib.getMainGlobals();

        PyObject.executeCode("x = 42", PyInputMode.STATEMENT, globals, dict);

        assertFalse(dict.asDict().isEmpty());
    }

    @Test
    public void testDictKeys() {
        PyObject dict = PyLib.newDict();
        PyDictWrapper wrapper = dict.asDict();
        assertTrue(wrapper.keySet().isEmpty());

        PyObject.executeCode("my_key = 42", PyInputMode.STATEMENT, PyLib.getMainGlobals(), dict);
        PyObject pyKey = PyObject.executeCode("'my_key'", PyInputMode.EXPRESSION);

        assertEquals(wrapper.keySet(), Collections.singleton(pyKey));
        assertTrue(wrapper.containsKey(pyKey));
        assertTrue(wrapper.containsKey("my_key"));
        assertTrue(wrapper.containsKey((Object)pyKey));

        assertFalse(wrapper.containsKey(1));
        assertFalse(wrapper.containsKey(this));
    }

    @Test
    public void testDictValues() {
        PyObject dict = PyLib.newDict();
        PyDictWrapper wrapper = dict.asDict();
        assertTrue(wrapper.values().isEmpty());

        PyObject.executeCode("my_key = 42", PyInputMode.STATEMENT, PyLib.getMainGlobals(), dict);
        PyObject pyValue = PyObject.executeCode("42", PyInputMode.EXPRESSION);

        // PyListWrapper doesn't implement the full List interface... can't check direct List equality
        Collection<PyObject> values = wrapper.values();
        assertFalse(values.isEmpty());
        assertEquals(values.size(), 1);
        assertEquals(values.iterator().next(), pyValue);
        assertTrue(wrapper.containsValue(pyValue));
    }

    @Test
    public void invalidPyDictKeys() {
        PyObject pyValue = PyObject.executeCode("42", PyInputMode.EXPRESSION);
        try {
            PyLib.pyDictKeys(pyValue.getPointer());
            fail("Expected exception");
        } catch (UnsupportedOperationException e) {
            // expected
        }
    }

    @Test
    public void invalidPyDictValues() {
        PyObject pyValue = PyObject.executeCode("42", PyInputMode.EXPRESSION);
        try {
            PyLib.pyDictValues(pyValue.getPointer());
            fail("Expected exception");
        } catch (UnsupportedOperationException e) {
            // expected
        }
    }

    @Test
    public void invalidPyDictContains() {
        PyObject pyValue = PyObject.executeCode("42", PyInputMode.EXPRESSION);
        try {
            PyLib.pyDictContains(pyValue.getPointer(), pyValue, null);
            fail("Expected exception");
        } catch (UnsupportedOperationException e) {
            // expected
        }
    }

    @Test
    public void decRefs() {
        final long pyObject1 = PyLib.executeCode("4321", PyInputMode.EXPRESSION.value(), null, null);
        final long pyObject2 = PyLib.executeCode("4322", PyInputMode.EXPRESSION.value(), null, null);

        PyLib.decRefs(new long[] { pyObject1, pyObject2, 0, 0 }, 2);
    }

    @Test
    public void testEnsureGIL() {
        assertFalse(PyLib.hasGil());
        boolean[] lambdaSuccessfullyRan = {false};
        Integer intResult = PyLib.ensureGil(() -> {
            assertTrue(PyLib.hasGil());
            lambdaSuccessfullyRan[0] = true;
            return 123;
        });
        assertEquals((Integer) 123, intResult);
        assertTrue(lambdaSuccessfullyRan[0]);

        try {
            Object result = PyLib.ensureGil(() -> {
                throw new IllegalStateException("Error from inside GIL block");
            });
            fail("Exception expected");
        } catch (IllegalStateException expectedException) {
            assertEquals("Error from inside GIL block", expectedException.getMessage());
        }//let anything else rethrow as a failure
    }
}
