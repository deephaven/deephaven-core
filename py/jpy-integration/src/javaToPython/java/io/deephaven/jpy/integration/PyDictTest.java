package io.deephaven.jpy.integration;

import io.deephaven.jpy.PythonTest;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.jpy.PyDictWrapper;
import org.jpy.PyInputMode;
import org.jpy.PyLib;
import org.jpy.PyObject;
import org.junit.Assert;
import org.junit.Test;

public class PyDictTest extends PythonTest {
    private static int count(Iterator<?> it) {
        int i = 0;
        for (; it.hasNext(); ++i, it.next()) {
            // empty
        }
        return i;
    }

    @Test
    public void emptyDict() {
        PyDictWrapper dict = PyObject.executeCode("dict()", PyInputMode.EXPRESSION).asDict();

        Assert.assertTrue(dict.isEmpty());
        Assert.assertTrue(dict.keySet().isEmpty());
        Assert.assertTrue(dict.entrySet().isEmpty());

        Assert.assertEquals(0, dict.size());
        Assert.assertEquals(0, dict.keySet().size());
        Assert.assertEquals(0, dict.entrySet().size());

        Assert.assertEquals(0, count(dict.keySet().iterator()));
        Assert.assertEquals(0, count(dict.entrySet().iterator()));
        Assert.assertEquals(0, count(dict.keySet().stream().iterator()));
        Assert.assertEquals(0, count(dict.entrySet().stream().iterator()));
    }

    @Test
    public void simpleDict() {
        PyDictWrapper dict = PyObject
            .executeCode("{'mock':'yeah', 'ing':'yeah', 'bird':'yeah'}", PyInputMode.EXPRESSION)
            .asDict();

        Assert.assertTrue(!dict.isEmpty());
        Assert.assertTrue(!dict.keySet().isEmpty());
        Assert.assertTrue(!dict.entrySet().isEmpty());

        Assert.assertEquals(3, dict.size());
        Assert.assertEquals(3, dict.keySet().size());
        Assert.assertEquals(3, dict.entrySet().size());

        Assert.assertEquals(3, count(dict.keySet().iterator()));
        Assert.assertEquals(3, count(dict.entrySet().iterator()));
        Assert.assertEquals(3, count(dict.keySet().stream().iterator()));
        Assert.assertEquals(3, count(dict.entrySet().stream().iterator()));
    }

    @Test
    public void noneKeyDict() {
        PyObject none = PyObject.executeCode("None", PyInputMode.EXPRESSION);
        PyObject yeah = PyObject.executeCode("'yeah'", PyInputMode.EXPRESSION);
        PyDictWrapper dict = PyObject.executeCode("{None:'yeah'}", PyInputMode.EXPRESSION).asDict();

        Assert.assertTrue(dict.keySet().iterator().next().isNone());
        Assert.assertTrue(dict.keySet().stream().iterator().next().isNone());

        Assert.assertTrue(dict.entrySet().iterator().next().getKey().isNone());
        Assert.assertTrue(dict.entrySet().stream().iterator().next().getKey().isNone());

        Assert.assertEquals(yeah, dict.entrySet().iterator().next().getValue());
        Assert.assertEquals(yeah, dict.entrySet().stream().iterator().next().getValue());

        Assert.assertEquals(yeah, dict.get(none));
    }

    @Test
    public void globals() {
        PyLib.getMainGlobals()
            .asDict()
            .copy()
            .entrySet()
            .stream()
            .map(e -> new SimpleImmutableEntry<>(e.getKey().toString(), convert(e.getValue())))
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }

    static Object convert(PyObject pyObject) {
        if (pyObject.isNone()) {
            return pyObject;
        } else if (pyObject.isList()) {
            return pyObject.asList();
        } else if (pyObject.isDict()) {
            return pyObject.asDict();
        } else if (pyObject.isConvertible()) {
            return pyObject.getObjectValue();
        } else {
            return pyObject;
        }
    }
}
