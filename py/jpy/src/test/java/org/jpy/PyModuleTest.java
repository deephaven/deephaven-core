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

import org.junit.*;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Norman Fomferra
 */
public class PyModuleTest {

    @Before
    public void setUp() throws Exception {
        //System.out.println("PyModuleTest: Current thread: " + Thread.currentThread());

        String importPath = new File("src/test/python/fixtures").getCanonicalPath();
        PyLib.startPython(importPath);
        assertEquals(true, PyLib.isPythonRunning());

        //PyLib.Diag.setFlags(PyLib.Diag.F_METH);
    }

    @After
    public void tearDown() throws Exception {
        PyLib.Diag.setFlags(PyLib.Diag.F_OFF);
        PyLib.stopPython();
    }

    @Test
    public void testCreateAndCallProxySingleThreaded() throws Exception {
        //PyObjectTest.addTestDirToPythonSysPath();
        PyModule procModule = PyModule.importModule("proc_module");
        PyObjectTest.testCallProxySingleThreaded(procModule);
    }

    // see https://github.com/bcdev/jpy/issues/26
    @Test
    public void testCreateAndCallProxyMultiThreaded() throws Exception {
        //PyObjectTest.addTestDirToPythonSysPath();
        PyModule procModule = PyModule.importModule("proc_module");
        PyObjectTest.testCallProxyMultiThreaded(procModule);
    }

    // see: https://github.com/bcdev/jpy/issues/39: Improve Java exception messages on Python errors #39
    @Test
    public void testPythonErrorMessages() throws Exception {
        //PyObjectTest.addTestDirToPythonSysPath();
        PyModule raiserModule = PyModule.importModule("raise_errors");
        for (int i=0;i < 10;i++) {
            try {
                raiserModule.call("raise_if_zero", 0);
                Assert.fail();
            } catch (RuntimeException e) {
                //e.printStackTrace();
                String message = e.getMessage();
                //System.out.println("message = " + message);
                assertNotNull(message);
                assertTrue(message.startsWith("Error in Python interpreter"));
                assertTrue(message.contains("Type: <"));
                assertTrue(message.contains("IndexError'>\n"));
                assertTrue(message.contains("Value: arg wasn't there\n"));
                assertTrue(message.contains("Line: 3\n"));
                assertTrue(message.contains("Namespace: raise_if_zero\n"));
                assertTrue(message.contains("File: "));
            }
            // ok
            raiserModule.call("raise_if_zero", 1);
        }
    }
}
