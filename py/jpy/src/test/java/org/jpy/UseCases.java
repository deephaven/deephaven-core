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

import org.junit.After;
import org.junit.Before;
import org.junit.Assert;
import org.junit.Test;

import java.util.Locale;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Some (more complex) tests that represent possible API use cases.
 *
 * @author Norman Fomferra
 */
public class UseCases {

    @Before
    public void setUp() {
      PyLib.startPython();
    }

    @After
    public void tearDown() {
      PyLib.stopPython();
    }

    @Test
    public void modifyPythonSysPath() {

        try (
            final PyModule builtinsMod = PyModule.getBuiltins();
            final PyModule sysMod = PyModule.importModule("sys");
            final PyObject pathObj = sysMod.getAttribute("path")) {

            final PyObject lenObj1 = builtinsMod.call("len", pathObj);
            pathObj.call("append", "/usr/home/norman/");
            final PyObject lenObj2 = builtinsMod.call("len", pathObj);

            int lenVal1 = lenObj1.getIntValue();
            int lenVal2 = lenObj2.getIntValue();
            String[] pathEntries = pathObj.getObjectArrayValue(String.class);

            lenObj2.close();
            lenObj1.close();

            /////////////////////////////////////////////////

            assertEquals(lenVal1 + 1, lenVal2);
            assertEquals(pathEntries.length, lenVal2);
            //for (int i = 0; i < pathEntries.length; i++) {
            //    System.out.printf("pathEntries[%d] = %s%n", i, pathEntries[i]);
            //}

            /////////////////////////////////////////////////
        }
    }

    @Test
    public void setAndGetGlobalPythonVariables() throws Exception {

        PyLib.startPython();
        PyLib.execScript("paramInt = 123");
        PyLib.execScript("paramStr = 'abc'");
        try (
            final PyModule mainModule = PyModule.getMain();
            final PyObject paramIntObj = mainModule.getAttribute("paramInt");
            final PyObject paramStrObj = mainModule.getAttribute("paramStr")) {
            int paramIntValue = paramIntObj.getIntValue();
            String paramStrValue = paramStrObj.getStringValue();

            /////////////////////////////////////////////////

            assertEquals(123, paramIntValue);
            assertEquals("abc", paramStrValue);

            /////////////////////////////////////////////////
        }
    }

    @Test
    public void defAndUseGlobalPythonFunction() throws Exception {

        PyLib.startPython();
        PyLib.execScript("def incByOne(x): return x + 1");
        PyModule mainModule = PyModule.getMain();
        PyObject eleven = mainModule.call("incByOne", 10);

        /////////////////////////////////////////////////

        assertEquals(11, eleven.getIntValue());

        /////////////////////////////////////////////////
        // Performance test for TheMegaTB:

        long t0 = System.nanoTime();
        long numCalls = 100000;
        PyObject num = eleven;
        for (long i = 0; i < numCalls; i++) {
            num = mainModule.call("incByOne", num);
        }
        long t1 = System.nanoTime();

        assertEquals(11 + numCalls, num.getIntValue());

        double millis = (t1 - t0) / 1000. / 1000.;
        double callsPerMilli = numCalls / millis;
        double millisPerCall = millis / numCalls;

        System.out.printf("Performance: %10.1f Python-calls/ms, %2.10f ms/Python-call%n", callsPerMilli, millisPerCall);
        assertTrue(callsPerMilli > 1.0);

        /////////////////////////////////////////////////
    }

    static {
        Locale.setDefault(Locale.ENGLISH);
    }
}
