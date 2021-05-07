package io.deephaven.jpy;

import io.deephaven.jpy.integration.ReferenceCounting;
import org.jpy.PyObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DictTest extends PythonTest {
    private BuiltinsModule builtins;
    private ReferenceCounting ref;

    @Before
    public void setUp() {
        builtins = BuiltinsModule.create();
        ref = ReferenceCounting.create();
    }

    @After
    public void tearDown() {
        ref.close();
        builtins.close();
    }

    @Test
    public void dictRef() {
        try (final PyObject dict = builtins.dict()) {
            dict.asDict().setItem("mykey", 42.0f);
            try (final PyObject myValue = dict.asDict().get("mykey")) {
                ref.check(2, myValue);
                dict.asDict().clear();
                ref.check(1, myValue);
            }
        }
    }

    private void check(Object key, Object value) {
        try (final PyObject dict = builtins.dict()) {
            dict.asDict().setItem(key, value);
            try (final PyObject myValue = dict.asDict().get(key)) {
                ref.check(2, myValue);
                dict.asDict().clear();
                ref.check(1, myValue);
            }
        }
    }
}
