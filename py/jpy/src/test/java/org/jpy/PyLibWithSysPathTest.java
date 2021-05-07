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
import java.net.URI;
import java.security.CodeSource;

import static org.junit.Assert.*;


public class PyLibWithSysPathTest {

    @Before
    public void setUp() throws Exception {

        CodeSource codeSource = PyLibWithSysPathTest.class.getProtectionDomain().getCodeSource();
        if (codeSource == null) {
            System.out.println(PyLibWithSysPathTest.class + " not run: no code source found");
            return;
        }
        URI codeSourceLocation = codeSource.getLocation().toURI();
        System.out.println(PyLibWithSysPathTest.class + ": code source: " + codeSourceLocation);
        File codeSourceDir = new File(codeSourceLocation);
        if (!codeSourceDir.isDirectory()) {
            System.out.println(PyLibWithSysPathTest.class + " not run: code source is not a directory: " + codeSourceLocation);
            return;
        }

        File pymodulesDir = new File(codeSourceDir, "pymodules");
        //assertFalse(PyLib.isPythonRunning());
        System.out.println("PyLibWithSysPathTest: starting Python with 'sys.path' extension: " + pymodulesDir);
        PyLib.startPython(pymodulesDir.getPath());
        //PyLib.startPython("x");
        assertTrue(PyLib.isPythonRunning());
    }

    @After
    public void tearDown() throws Exception {
        PyLib.stopPython();
    }

    @Test
    public void testLoadModule() throws Exception {
        try (final PyModule pyModule = PyModule.importModule("mod_1")) {
            assertNotNull(pyModule);
            try (final PyObject pyAnswer = pyModule.getAttribute("answer")) {
                assertNotNull(pyAnswer);
                assertEquals(42, pyAnswer.getIntValue());
            }
        }
    }
}
