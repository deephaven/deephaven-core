package io.deephaven.jpy;

import io.deephaven.jpy.integration.ReferenceCounting;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.jpy.IdentityModule;
import org.jpy.PyObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PyProxyTest extends PythonTest {

    interface SomeClassBase {
        boolean get_closed();
    }

    interface WithJavaCloseable extends SomeClassBase, AutoCloseable {
        // close() will dec-ref the object (and *not* call the python close() method)
        @Override
        void close();
    }

    interface WithoutJavaCloseable extends SomeClassBase {
        // close() will call the python close() method
        void close();
    }

    interface AfterTheFactJavaCloseable extends WithoutJavaCloseable, AutoCloseable {
        // now, it acts like WithJavaCloseable (will dec-ref)
        void close();
    }

    private static String readResource(String name) {
        try {
            return new String(
                    Files.readAllBytes(Paths.get(PyProxyTest.class.getResource(name).toURI())),
                    StandardCharsets.UTF_8);
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private IdentityModule identityModule;
    private ReferenceCounting refCount;

    @Before
    public void setUp() throws Exception {
        identityModule = IdentityModule.create(getCreateModule());
        refCount = ReferenceCounting.create();
    }

    @After
    public void tearDown() throws Exception {
        refCount.close();
        identityModule.close();
    }

    @Test
    public void withJavaCloseable() {
        final WithJavaCloseable anotherReference;
        try (final WithJavaCloseable pythonObj = getCreateModule()
                .call("py_proxy_test", readResource("proxy_close_test.py"))
                .call("SomeClass")
                .createProxy(WithJavaCloseable.class)) {

            anotherReference = identityModule
                    .identity(PyObject.unwrapProxy(pythonObj))
                    .createProxy(WithJavaCloseable.class);

            Assert.assertFalse(anotherReference.get_closed());
            Assert.assertEquals(pythonObj, anotherReference);

            refCount.check(2, anotherReference);
            // a proxy dec-ref close
        }
        refCount.check(1, anotherReference);
        Assert.assertFalse(anotherReference.get_closed());
        anotherReference.close(); // a proxy dec-ref close
    }

    @Test
    public void withoutJavaCloseable() {
        final WithoutJavaCloseable anotherReference;
        final WithoutJavaCloseable pythonObj = getCreateModule()
                .call("py_proxy_test", readResource("proxy_close_test.py"))
                .call("SomeClass")
                .createProxy(WithoutJavaCloseable.class);

        anotherReference = identityModule
                .identity(PyObject.unwrapProxy(pythonObj))
                .createProxy(WithoutJavaCloseable.class);

        Assert.assertFalse(anotherReference.get_closed());
        Assert.assertEquals(pythonObj, anotherReference);

        refCount.check(2, anotherReference);
        pythonObj.close(); // a python close

        refCount.check(2, anotherReference);
        Assert.assertTrue(anotherReference.get_closed());

        PyObject.unwrapProxy(pythonObj).close(); // a decref close
        refCount.check(1, anotherReference);
        PyObject.unwrapProxy(anotherReference).close(); // a decref close
    }

    @Test
    public void afterTheFactJavaCloseable() {
        final AfterTheFactJavaCloseable anotherReference;
        final AfterTheFactJavaCloseable pythonObj = getCreateModule()
                .call("py_proxy_test", readResource("proxy_close_test.py"))
                .call("SomeClass")
                .createProxy(AfterTheFactJavaCloseable.class);

        anotherReference = identityModule
                .identity(PyObject.unwrapProxy(pythonObj))
                .createProxy(AfterTheFactJavaCloseable.class);

        Assert.assertFalse(anotherReference.get_closed());
        Assert.assertEquals(pythonObj, anotherReference);

        refCount.check(2, anotherReference);
        pythonObj.close(); // a proxy dec-ref close

        refCount.check(1, anotherReference);
        Assert.assertFalse(anotherReference.get_closed());

        anotherReference.close(); // a proxy dec-ref close
    }
}
