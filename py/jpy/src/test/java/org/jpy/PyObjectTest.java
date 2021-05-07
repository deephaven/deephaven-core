/*
 * Copyright 2015 Brockmann Consult GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * This file was modified by Deephaven Data Labs.
 *
 */

package org.jpy;

import java.util.regex.Pattern;
import org.junit.*;
import org.jpy.fixtures.Processor;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.Assert.*;

/**
 * @author Norman Fomferra
 */
public class PyObjectTest {

    private PyModule SPECIAL_METHODS;

    @Before
    public void setUp() throws Exception {
        // System.out.println("PyModuleTest: Current thread: " +
        // Thread.currentThread());
        String importPath = new File("src/test/python/fixtures").getCanonicalPath();
        
        PyLib.startPython(importPath);
        assertEquals(true, PyLib.isPythonRunning());
        
        PyLib.Diag.setFlags(PyLib.Diag.F_ALL);

        SPECIAL_METHODS = PyModule.importModule("special_methods");
    }
    
    @After
    public void tearDown() throws Exception {
        PyLib.Diag.setFlags(PyLib.Diag.F_OFF);
        PyLib.stopPython();
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testNullPointer() throws Exception {
        new PyObject(0);
    }
    
    @Test
    public void testPointer() throws Exception {
        long pointer = PyLib.importModule("sys");
        PyObject pyObject = new PyObject(pointer);
        assertEquals(pointer, pyObject.getPointer());
    }
    
    @Test
    public void testToString() throws Exception {
        long pointer = PyLib.importModule("sys");
        PyObject pyObject = new PyObject(pointer);
        assertEquals("<module 'sys' (built-in)>", pyObject.toString());
    }
    
    @Test
    public void testEqualsAndHashCode() throws Exception {
        long pointer1 = PyLib.importModule("sys");
        long pointer2 = PyLib.importModule("os");
        PyObject pyObject1 = new PyObject(pointer1);
        PyObject pyObject2 = new PyObject(pointer2);
        assertEquals(true, pyObject1.equals(pyObject1));
        assertEquals(true, pyObject1.equals(new PyObject(pointer1)));
        assertEquals(false, pyObject1.equals(pyObject2));
        assertEquals(false, pyObject1.equals(new PyObject(pointer2)));
        assertEquals(false, pyObject1.equals((Object) pointer1));
        assertTrue(0 != pyObject1.hashCode());
        assertTrue(0 != pyObject2.hashCode());
        assertEquals(pyObject1.hashCode(), pyObject1.hashCode());
        assertEquals(pyObject1.hashCode(), new PyObject(pointer1).hashCode());
        assertTrue(pyObject1.hashCode() != pyObject2.hashCode());
    }
    
    @Test
    public void testExecuteCode_Stmt() throws Exception {
        PyObject pyObject = PyObject.executeCode("pass", PyInputMode.STATEMENT);
        assertNotNull(pyObject);
        assertNull(pyObject.getObjectValue());
    }
    
    @Test
    public void testExecuteCode_IntExpr() throws Exception {
        PyObject pyObject = PyObject.executeCode("7465", PyInputMode.EXPRESSION);
        assertNotNull(pyObject);
        assertEquals(7465, pyObject.getIntValue());
    }
    
    @Test
    public void testExecuteCode_DoubleExpr() throws Exception {
        PyObject pyObject = PyObject.executeCode("3.14", PyInputMode.EXPRESSION);
        assertNotNull(pyObject);
        assertEquals(3.14, pyObject.getDoubleValue(), 1e-10);
    }
    
    @Test
    public void testExecuteCode_StringExpr() throws Exception {
        PyObject pyObject = PyObject.executeCode("'Hello from Python'", PyInputMode.EXPRESSION);
        assertNotNull(pyObject);
        assertEquals("Hello from Python", pyObject.getStringValue());
    }
    
    @Test
    public void testExecuteCode_Script() throws Exception {
        HashMap<String, Object> localMap = new HashMap<>();
        PyObject pyVoid = PyObject.executeCode(
                "" + "import jpy\n" + "File = jpy.get_type('java.io.File')\n" + "f = File('test.txt')",
                PyInputMode.SCRIPT, null, localMap);
        assertNotNull(pyVoid);
        assertEquals(null, pyVoid.getObjectValue());
        
        assertNotNull(localMap.get("jpy"));
        assertNotNull(localMap.get("File"));
        assertNotNull(localMap.get("f"));
        assertEquals(PyObject.class, localMap.get("jpy").getClass());
        assertEquals(Class.class, localMap.get("File").getClass());
        assertEquals(File.class, localMap.get("f").getClass());
        
        assertEquals(new File("test.txt"), localMap.get("f"));
    }
    
    @Test
    public void testLocals() throws Exception {
        HashMap<String, Object> localMap = new HashMap<>();
        localMap.put("x", 7);
        localMap.put("y", 6);
        PyObject pyVoid = PyObject.executeCode("z = x + y", PyInputMode.STATEMENT, null, localMap);
        assertEquals(null, pyVoid.getObjectValue());
        
        System.out.println("LocalMap size = " + localMap.size());
        for (Map.Entry<String, Object> entry : localMap.entrySet()) {
            System.out.println("LocalMap[" + entry.getKey() + "]: " + entry.getValue());
        }
        
        assertNotNull(localMap.get("x"));
        assertNotNull(localMap.get("y"));
        assertNotNull(localMap.get("z"));
        
        assertEquals(7, localMap.get("x"));
        assertEquals(6, localMap.get("y"));
        assertEquals(13, localMap.get("z"));
    }
    
    @Test
    public void testExecuteScript_ErrorExpr() throws Exception {
        try {
            PyObject.executeCode("[1, 2, 3", PyInputMode.EXPRESSION);
        } catch (RuntimeException e) {
            assertNotNull(e.getMessage());
            assertTrue(e.getMessage().contains("SyntaxError"));
        }
    }
    
    @Test
    public void testCall() throws Exception {
        // Python equivalent:
        //
        // >>> import builtins
        // >>> builtins.max('A', 'Z')
        // 'Z'
        //
        PyModule builtins;
        try {
            // Python 3.3
            builtins = PyModule.importModule("builtins");
        } catch (Exception e) {
            // Python 2.7
            builtins = PyModule.importModule("__builtin__");
        }
        PyObject value = builtins.call("max", "A", "Z");
        Assert.assertEquals("Z", value.getStringValue());
    }
    
    @Test
    public void testGetSetAttributes() throws Exception {
        // Python equivalent:
        //
        // >>> import imp
        // >>> myobj = imp.new_module('myobj')
        // >>> myobj.a = 'Tut tut!'
        // >>> myobj.a
        // 'Tut tut!'
        //
        try (
            final PyModule imp = PyModule.importModule("imp");
            final PyObject myobj = imp.call("new_module", "myobj")) {
            // Call imp.new_module('') module
            myobj.setAttribute("a", "Tut tut!");
            Assert.assertEquals("Tut tut!", myobj.getAttribute("a", String.class));
            try (final PyObject a = myobj.getAttribute("a")) {
                Assert.assertEquals("Tut tut!", a.getStringValue());
            }
        }
    }
    
    private boolean hasKey(Map<PyObject, PyObject> dict, String key) {
        for (Map.Entry<PyObject, PyObject> entry : dict.entrySet()) {
            if (entry.getKey().isString()) {
                if (entry.getKey().getObjectValue().equals(key)) {
                    return true;
                }
            }
        }
        return false;
    }
    
    @Test
    public void testDictCopy() throws Exception {
        PyObject globals = PyLib.getMainGlobals();
        PyDictWrapper dict = globals.asDict();
        PyDictWrapper dictCopy = dict.copy();
        
        PyObject.executeCode("x = 42", PyInputMode.STATEMENT, globals, dictCopy.unwrap());
        
        boolean copyHasX = hasKey(dictCopy, "x");
        boolean origHasX = hasKey(dict, "x");
        
        assertTrue(copyHasX);
        assertFalse(origHasX);
    }
    
    @Test
    public void testCreateProxyAndCallSingleThreaded() throws Exception {
        // addTestDirToPythonSysPath();
        PyModule procModule = PyModule.importModule("proc_class");
        PyObject procObj = procModule.call("Processor");
        testCallProxySingleThreaded(procObj);
    }
    
    // see https://github.com/bcdev/jpy/issues/26
    @Test
    public void testCreateProxyAndCallMultiThreaded() throws Exception {
        // addTestDirToPythonSysPath();
        // PyLib.Diag.setFlags(PyLib.Diag.F_ALL);
        PyModule procModule = PyModule.importModule("proc_class");
        PyObject procObj = procModule.call("Processor");
        PyLib.Diag.setFlags(PyLib.Diag.F_ALL);
        testCallProxyMultiThreaded(procObj);
        // PyLib.Diag.setFlags(PyLib.Diag.F_OFF);
    }

    @Test
    public void testUnwrapProxy() {
        PyModule procModule = PyModule.importModule("proc_class");
        PyObject procObj = procModule.call("Processor");
        Processor proxy = procObj.createProxy(Processor.class);
        PyObject unwrapped = PyObject.unwrapProxy(proxy);
        assertSame(procObj, unwrapped);
    }

    @Test
    public void testUnwrapProxyNotAProxy() {
        assertNull(PyObject.unwrapProxy(this));
    }

    static void testCallProxySingleThreaded(PyObject procObject) {
        // Cast the Python object to a Java object of type 'Processor'
        Processor processor = procObject.createProxy(Processor.class);
        assertNotNull(processor);
        
        String result;
        result = processor.initialize();
        assertEquals("initialize", result);
        result = processor.computeTile(100, 100, new float[100 * 100]);
        assertEquals("computeTile-100,100", result);
        result = processor.computeTile(200, 100, new float[100 * 100]);
        assertEquals("computeTile-200,100", result);
        result = processor.computeTile(100, 200, new float[100 * 100]);
        assertEquals("computeTile-100,200", result);
        result = processor.computeTile(200, 200, new float[100 * 100]);
        assertEquals("computeTile-200,200", result);
        processor.setVal(1234);
        int val = processor.getVal();
        assertEquals(val, 1234);
        assertEquals(true, processor.check1234());
        result = processor.dispose();
        assertEquals("dispose", result);
    }
    
    static void testCallProxyMultiThreaded(PyObject procObject) {
        testCallProxyMultiThreaded(procObject, Executors.newFixedThreadPool(4));
    }
    
    private static void testCallProxyMultiThreaded(PyObject procObject, ExecutorService executorService) {
        // Cast the Python object to a Java object of type 'Processor'
        final Processor processor = procObject.createProxy(Processor.class);
        assertNotNull(processor);
        
        String result;
        result = processor.initialize();
        assertEquals("initialize", result);
        
        List<Future<String>> futures;
        try {
            futures = executorService.invokeAll(
                    Arrays.asList(new ProcessorTask(processor, 100, 100), new ProcessorTask(processor, 200, 100),
                            new ProcessorTask(processor, 100, 200), new ProcessorTask(processor, 200, 200)),
                    10, TimeUnit.SECONDS);
            
            result = processor.dispose();
            assertEquals("dispose", result);
            
            String[] results = new String[] { futures.get(0).get(), futures.get(1).get(), futures.get(2).get(),
                            futures.get(3).get(), };
            
            Arrays.sort(results);
            
            result = results[0];
            assertEquals("computeTile-100,100", result);
            result = results[1];
            assertEquals("computeTile-100,200", result);
            result = results[2];
            assertEquals("computeTile-200,100", result);
            result = results[3];
            assertEquals("computeTile-200,200", result);
            
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
    
    static void addTestDirToPythonSysPath() throws IOException {
        // Add module dir to sys.path in order to import file 'proc_class.py'
        String importPath = new File("src/test/python/fixtures").getCanonicalPath();
        // System.out.println("importPath = " + importPath);
        PyLib.execScript(String.format("import sys; sys.path.append('%s')", importPath.replace("\\", "\\\\")));
    }
    
    private static class ProcessorTask implements Callable<String> {
        final Processor processor;
        int x;
        int y;
        
        public ProcessorTask(Processor processor, int x, int y) {
            this.processor = processor;
            this.x = x;
            this.y = y;
        }
        
        @Override
        public String call() throws Exception {
            return processor.computeTile(x, y, new float[100 * 100]);
        }
    }
    
    private static interface ISimple {
        public int getValue();
    }
    
    private static ISimple newTestObj(PyModule pyModule, String pythonClass, int value) {
        PyObject procObj = pyModule.call(pythonClass, value);
        ISimple simple = procObj.createProxy(ISimple.class);
        return simple;
    }
    
    @Test
    public void testHashEqStr() {
        PyModule pyModule = PyModule.importModule("hasheqstr");
        testOneClass(pyModule, "Simple", false);
        testOneClass(pyModule, "HashSimple", true);
    }
    
    private static void testOneClass(PyModule pyModule, String pythonClass, boolean eqResExpected) {
        ISimple simple = newTestObj(pyModule, pythonClass, 1234);
        int value = simple.getValue();
        assertEquals(value, 1234);
        String rep = simple.toString();
        assertEquals(rep, pythonClass + ": 1234");
        ISimple simple2 = newTestObj(pyModule, pythonClass, 1234);
        boolean eqRes = simple.equals(simple2);
        assertEquals(eqRes, eqResExpected);
        assertEquals(simple.hashCode() == simple2.hashCode(), eqResExpected);
        assertEquals(simple.equals(simple), true);
    }

    @Test
    public void strSimple() {
        PyObject obj = SPECIAL_METHODS.call("Simple", 1);
        assertEquals("1", obj.str());
    }

    @Test
    public void hashSimple() {
        PyObject obj = SPECIAL_METHODS.call("Simple", 1);
        assertEquals(1L, obj.hash());
    }

    @Test
    public void eqSimple() {
        PyObject obj1 = SPECIAL_METHODS.call("Simple", 1);
        PyObject obj2 = SPECIAL_METHODS.call("Simple", 1);
        PyObject none = PyObject.executeCode("None", PyInputMode.EXPRESSION);

        assertTrue(obj1.eq(obj1));
        assertTrue(obj2.eq(obj2));

        assertTrue(obj1.eq(obj2));
        assertTrue(obj2.eq(obj1));

        assertFalse(obj1.eq(null));
        assertFalse(obj2.eq(null));

        assertFalse(obj1.eq(none));
        assertFalse(obj2.eq(none));
        assertFalse(none.eq(obj1));
        assertFalse(none.eq(obj2));
    }

    @Test
    public void eqSimpleDiff() {
        PyObject obj1 = SPECIAL_METHODS.call("Simple", 1);
        PyObject obj2 = SPECIAL_METHODS.call("Simple", 2);
        PyObject none = PyObject.executeCode("None", PyInputMode.EXPRESSION);

        assertTrue(obj1.eq(obj1));
        assertTrue(obj2.eq(obj2));

        assertFalse(obj1.eq(obj2));
        assertFalse(obj2.eq(obj1));

        assertFalse(obj1.eq(null));
        assertFalse(obj2.eq(null));

        assertFalse(obj1.eq(none));
        assertFalse(obj2.eq(none));
        assertFalse(none.eq(obj1));
        assertFalse(none.eq(obj2));
    }

    @Test
    public void noneEqNone() {
        PyObject obj1 = PyObject.executeCode("None", PyInputMode.EXPRESSION);
        PyObject obj2 = PyObject.executeCode("None", PyInputMode.EXPRESSION);

        assertTrue(obj1.eq(obj1));
        assertTrue(obj2.eq(obj2));

        assertTrue(obj1.eq(obj2));
        assertTrue(obj2.eq(obj1));

        assertTrue(obj1.eq(null));
        assertTrue(obj2.eq(null));
    }

    @Test
    public void strException() {
        PyObject obj = SPECIAL_METHODS.call("ThrowsException");
        try {
            obj.str();
            fail("Expected exception");
        } catch (RuntimeException e) {
            // expected
        }
    }

    @Test
    public void hashException() {
        PyObject obj = SPECIAL_METHODS.call("ThrowsException");
        try {
            obj.hash();
            fail("Expected exception");
        } catch (RuntimeException e) {
            // expected
        }
    }

    @Test
    public void eqException() {
        PyObject obj = SPECIAL_METHODS.call("ThrowsException");
        try {
            obj.eq(obj);
            fail("Expected exception");
        } catch (RuntimeException e) {
            // expected
        }
    }

    @Test
    public void nonBooleanEq() {
        PyObject obj1 = SPECIAL_METHODS.call("NonBooleanEq");
        PyObject obj2 = SPECIAL_METHODS.call("NonBooleanEq");
        PyObject none = PyObject.executeCode("None", PyInputMode.EXPRESSION);

        assertTrue(obj1.eq(obj1));
        assertTrue(obj2.eq(obj2));

        assertFalse(obj1.eq(obj2));
        assertFalse(obj2.eq(obj1));

        assertFalse(obj1.eq(null));
        assertFalse(obj2.eq(null));

        assertFalse(obj1.eq(none));
        assertFalse(obj2.eq(none));
        assertFalse(none.eq(obj1));
        assertFalse(none.eq(obj2));
    }

    @Test
    public void nonStringStr() {
        PyObject obj = SPECIAL_METHODS.call("NonStringStr");
        try {
            obj.str();
            fail("Expected exception");
        } catch (RuntimeException e) {
            //
        }
    }

    @Test
    public void strNotDefined() {
        PyObject obj = SPECIAL_METHODS.call("NoMethodsDefined");
        // python2 uses instance, python3 uses object
        Pattern pattern = Pattern
            .compile("^<special_methods.NoMethodsDefined (instance|object) at 0x[0-9a-f]+>$");
        assertTrue(pattern.matcher(obj.str()).matches());
    }

    @Test
    public void hashNotDefined() {
        PyObject obj = SPECIAL_METHODS.call("NoMethodsDefined");
        obj.hash();
    }

    @Test
    public void eqNotDefined() {
        PyObject obj1 = SPECIAL_METHODS.call("NoMethodsDefined");
        PyObject obj2 = SPECIAL_METHODS.call("NoMethodsDefined");
        PyObject none = PyObject.executeCode("None", PyInputMode.EXPRESSION);

        assertTrue(obj1.eq(obj1));
        assertTrue(obj2.eq(obj2));

        assertFalse(obj1.eq(obj2));
        assertFalse(obj2.eq(obj1));

        assertFalse(obj1.eq(null));
        assertFalse(obj2.eq(null));

        assertFalse(obj1.eq(none));
        assertFalse(obj2.eq(none));
        assertFalse(none.eq(obj1));
        assertFalse(none.eq(obj2));
    }

    @Test
    public void eqAlwaysFalse() {
        PyObject obj1 = SPECIAL_METHODS.call("EqAlwaysFalse");
        PyObject obj2 = SPECIAL_METHODS.call("EqAlwaysFalse");
        PyObject none = PyObject.executeCode("None", PyInputMode.EXPRESSION);

        assertFalse(obj1.eq(obj1));
        assertFalse(obj2.eq(obj2));
        assertFalse(obj1.eq(obj2));
        assertFalse(obj2.eq(obj1));
        assertFalse(obj1.eq(null));
        assertFalse(obj2.eq(null));

        assertFalse(obj1.eq(none));
        assertFalse(obj2.eq(none));
        assertFalse(none.eq(obj1));
        assertFalse(none.eq(obj2));
    }

    @Test
    public void eqAlwaysTrue() {
        PyObject obj1 = SPECIAL_METHODS.call("EqAlwaysTrue");
        PyObject obj2 = SPECIAL_METHODS.call("EqAlwaysTrue");
        PyObject none = PyObject.executeCode("None", PyInputMode.EXPRESSION);

        assertTrue(obj1.eq(obj1));
        assertTrue(obj2.eq(obj2));
        assertTrue(obj1.eq(obj2));
        assertTrue(obj2.eq(obj1));
        assertTrue(obj1.eq(null));
        assertTrue(obj2.eq(null));

        assertTrue(obj1.eq(none));
        assertTrue(obj2.eq(none));

        // this was a bit surprising to me, but None == obj must proxy to obj == None
        assertTrue(none.eq(obj1));
        assertTrue(none.eq(obj2));
    }

    @Test
    public void eqMixed() {
        PyObject alwaysFalse = SPECIAL_METHODS.call("EqAlwaysFalse");
        PyObject alwaysTrue = SPECIAL_METHODS.call("EqAlwaysTrue");

        assertFalse(alwaysFalse.eq(alwaysTrue));
        assertTrue(alwaysTrue.eq(alwaysFalse));
    }

    @Test
    public void hashNegativeOne() {
        // note: the hash() function treats -1 as a special return value, thus if the underlying
        // __hash__ returns -1, hash() will change that to -2.

        PyObject obj = SPECIAL_METHODS.call("HashNegativeOne");
        assertEquals(-2L, obj.hash());

        PyObject result = obj.callMethod("__hash__");
        assertTrue(result.isInt());
        assertEquals(-1, obj.getIntValue());
    }

    @Test
    public void closeIsIdempotent() {
        PyObject obj = SPECIAL_METHODS.call("Simple", 1);
        obj.close();
        obj.close();
    }

    @Test(expected = IllegalStateException.class)
    public void errorIfUsageAfterClose() {
        PyObject obj = SPECIAL_METHODS.call("Simple", 1);
        obj.close();
        obj.getPointer();
    }
}
