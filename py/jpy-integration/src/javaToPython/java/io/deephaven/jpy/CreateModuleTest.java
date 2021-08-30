package io.deephaven.jpy;

import org.jpy.IdentityModule;
import org.jpy.PyDictWrapper;
import org.jpy.PyLib;
import org.jpy.PyObject;
import org.junit.Assert;
import org.junit.Test;

public class CreateModuleTest extends PythonTest {

    private static final String PLUS_42_CODE = "def plus_42(x):\n  return x + 42";

    private static final String GET_STATE =
        "import sys\n"
            + "def get_globals(x):\n"
            + "  return globals()\n"
            + "def get_locals(x):\n"
            + "  return locals()\n";

    interface Plus42 extends AutoCloseable {
        int plus_42(int x);

        @Override
        void close();
    }

    interface GetState extends AutoCloseable {

        PyObject get_globals(int x);

        PyObject get_locals(int x);

        @Override
        void close();
    }

    @Test
    public void plus_42() {
        try (final Plus42 plus42 = getCreateModule()
            .callAsFunctionModule("plus_42", PLUS_42_CODE, Plus42.class)) {
            Assert.assertEquals(42, plus42.plus_42(0));
            Assert.assertEquals(84, plus42.plus_42(42));
        }
    }

    @Test
    public void getGlobals() {
        try (final GetState getState = getCreateModule()
            .callAsFunctionModule("some_module_name", GET_STATE, GetState.class);
            final PyDictWrapper globals = getState.get_globals(42).asDict()) {
            Assert.assertTrue(globals.containsKey("sys"));
            Assert.assertTrue(globals.containsKey("get_globals"));
            Assert.assertTrue(globals.containsKey("get_locals"));
            Assert.assertFalse(globals.containsKey("x"));
            try (final PyObject p = globals.getItem("sys")) {
                Assert.assertTrue(p.isModule());
            }
            try (final PyObject p = globals.getItem("get_globals")) {
                Assert.assertTrue(p.isFunction());
            }
            try (final PyObject p = globals.getItem("get_locals")) {
                Assert.assertTrue(p.isFunction());
            }
        }
    }

    @Test
    public void getLocals() {
        try (final GetState getState = getCreateModule()
            .callAsFunctionModule("some_module_name", GET_STATE, GetState.class);
            final PyDictWrapper locals = getState.get_locals(42).asDict()) {
            Assert.assertEquals(1, locals.size());
            try (final PyObject p = locals.getItem("x")) {
                Assert.assertTrue(p.isLong());
                Assert.assertEquals(42, p.getIntValue());
            }
        }
    }

    interface HasGil extends AutoCloseable {
        boolean hasGil();

        @Override
        void close();
    }

    class HasGilObject {
        public boolean hasGil() {
            return PyLib.hasGil();
        }
    }

    @Test
    public void hasGil() {
        final HasGilObject hasGilObject = new HasGilObject();
        try (final IdentityModule identityModule = IdentityModule.create(getCreateModule());
            final HasGil proxy = identityModule
                .identity(hasGilObject)
                .createProxy(HasGil.class)) {
            Assert.assertFalse(hasGilObject.hasGil());
            Assert.assertTrue(proxy.hasGil());
        }
    }
}
