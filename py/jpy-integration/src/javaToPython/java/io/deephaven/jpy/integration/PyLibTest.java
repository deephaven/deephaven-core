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
    public void pingPong5() {
        Assert.assertEquals("PyLibTest(java,5)(python,4)(java,3)(python,2)(java,1)",
                PingPongStack.pingPongPython("PyLibTest", 5));
    }

    @Test
    public void pingPong4() {
        Assert.assertEquals("PyLibTest(java,4)(python,3)(java,2)(python,1)",
                PingPongStack.pingPongPython("PyLibTest", 4));
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
