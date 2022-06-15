/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.jpy.integration;

import io.deephaven.jpy.PythonTest;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.jpy.PyInputMode;
import org.jpy.PyLib;
import org.jpy.PyObject;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PyLibTest extends PythonTest {

    @Test
    public void isPythonRunning() {
        Assert.assertTrue(PyLib.isPythonRunning());
    }

    @Test
    public void hasGil() {
        Assert.assertFalse(PyLib.hasGil());
    }

    @Test
    public void printHi() {
        PyObject.executeCode("print('hi')", PyInputMode.STATEMENT);
    }

    @Test
    public void plus42() {
        PyObject.executeCode(readResource("plus42.py"), PyInputMode.SCRIPT);
        PyObject result = PyObject.executeCode("plus42(3)", PyInputMode.EXPRESSION);
        Assert.assertEquals(45, result.getIntValue());
    }

    @Test
    public void myClass() {
        PyObject.executeCode(readResource("my_class.py"), PyInputMode.SCRIPT);
        PyObject myClass = PyObject.executeCode("MyClass()", PyInputMode.EXPRESSION);
        MyClass proxy = myClass.createProxy(MyClass.class);

        // Assert.assertEquals(46, proxy.plus43(3)); // todo, why is this breaking?
        Assert.assertEquals("hi", proxy.echo("hi"));
        Assert.assertEquals(13, proxy.echo(13));
    }

    @Test
    public void myNumbersPrimitive() {
        PyObject.executeCode(readResource("my_numbers.py"), PyInputMode.SCRIPT);
        PyObject myClass = PyObject.executeCode("MyNumbers()", PyInputMode.EXPRESSION);
        MyNumbersPrimitive proxy = myClass.createProxy(MyNumbersPrimitive.class);

        Assert.assertEquals(Byte.MAX_VALUE, proxy.get_byte());
        Assert.assertEquals(Short.MAX_VALUE, proxy.get_short());
        Assert.assertEquals(Integer.MAX_VALUE, proxy.get_int());
        Assert.assertEquals(Long.MAX_VALUE, proxy.get_long());
    }

    @Test
    public void myNumbersBoxed() {
        PyObject.executeCode(readResource("my_numbers.py"), PyInputMode.SCRIPT);
        PyObject myClass = PyObject.executeCode("MyNumbers()", PyInputMode.EXPRESSION);
        MyNumbersBoxed proxy = myClass.createProxy(MyNumbersBoxed.class);

        Assert.assertEquals(Byte.valueOf(Byte.MAX_VALUE), proxy.get_byte());
        Assert.assertEquals(Short.valueOf(Short.MAX_VALUE), proxy.get_short());
        Assert.assertEquals(Integer.valueOf(Integer.MAX_VALUE), proxy.get_int());
        Assert.assertEquals(Long.valueOf(Long.MAX_VALUE), proxy.get_long());
    }

    @Test
    public void myNumbersObject() {
        PyObject.executeCode(readResource("my_numbers.py"), PyInputMode.SCRIPT);
        PyObject myClass = PyObject.executeCode("MyNumbers()", PyInputMode.EXPRESSION);
        MyNumbersObject proxy = myClass.createProxy(MyNumbersObject.class);

        Assert.assertEquals(Byte.valueOf(Byte.MAX_VALUE), proxy.get_byte());
        Assert.assertEquals(Short.valueOf(Short.MAX_VALUE), proxy.get_short());
        Assert.assertEquals(Integer.valueOf(Integer.MAX_VALUE), proxy.get_int());
        Assert.assertEquals(Long.valueOf(Long.MAX_VALUE), proxy.get_long());
    }

    @Test
    public void myNumbersNumber() {
        PyObject.executeCode(readResource("my_numbers.py"), PyInputMode.SCRIPT);
        PyObject myClass = PyObject.executeCode("MyNumbers()", PyInputMode.EXPRESSION);
        MyNumbersNumber proxy = myClass.createProxy(MyNumbersNumber.class);

        Assert.assertEquals(Byte.valueOf(Byte.MAX_VALUE), proxy.get_byte());
        Assert.assertEquals(Short.valueOf(Short.MAX_VALUE), proxy.get_short());
        Assert.assertEquals(Integer.valueOf(Integer.MAX_VALUE), proxy.get_int());
        Assert.assertEquals(Long.valueOf(Long.MAX_VALUE), proxy.get_long());
    }

    @Test
    public void pingPong5() {
        Assert.assertEquals("PyLibTest(java,5)(python,4)(java,3)(python,2)(java,1)",
                PingPongStack.pingPongPython("PyLibTest", 5));
    }

    @Test
    public void pingPong4() {
        Assert.assertEquals("PyLibTest(java,4)(python,3)(java,2)(python,1)",
                PingPongStack.pingPongPython("PyLibTest", 4));
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
        } // let anything else rethrow as a failure
    }

    private static String readResource(String name) {
        try {
            return new String(
                    Files.readAllBytes(Paths.get(PyLibTest.class.getResource(name).toURI())),
                    StandardCharsets.UTF_8);
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
