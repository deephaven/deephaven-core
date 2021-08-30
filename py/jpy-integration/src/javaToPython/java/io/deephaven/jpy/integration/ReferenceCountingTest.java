package io.deephaven.jpy.integration;

import io.deephaven.jpy.BuiltinsModule;
import io.deephaven.jpy.JpyModule;
import io.deephaven.jpy.PythonTest;
import io.deephaven.jpy.integration.DestructorModuleParent.OnDelete;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.jpy.PyInputMode;
import org.jpy.PyLib;
import org.jpy.PyModule;
import org.jpy.PyObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests that when passing array from java to python, it is correctly recognized by jpy.
 */
public class ReferenceCountingTest extends PythonTest {
    private NoopModule noop;
    private PyObjectIdentityOut pyOut;
    private BuiltinsModule builtins;
    private DestructorModuleParent destructor;
    private ReferenceCounting ref;
    private JpyModule jpy;

    @Before
    public void setUp() {
        ref = ReferenceCounting.create();
        noop = NoopModule.create(getCreateModule());
        pyOut = IdentityOut.create(getCreateModule(), PyObjectIdentityOut.class);
        builtins = BuiltinsModule.create();
        destructor = DestructorModuleParent.create(getCreateModule());
        jpy = JpyModule.create();
        // jpy.setFlags(EnumSet.of(Flag.ALL));
    }

    @After
    public void tearDown() {
        // jpy.setFlags(EnumSet.of(Flag.OFF));
        destructor.close();
        builtins.close();
        pyOut.close();
        noop.close();
        ref.close();
    }

    /*
     * @Test public void javaOnlyObjectDoesntHavePythonReferences() { final Object obj = new Object();
     * checkReferenceCount(0, obj); blackhole(obj); }
     */

    @Test
    public void pythonObjectViaExecuteHasOneReference() {
        final PyObject pyObject = PyObject.executeCode("dict()", PyInputMode.EXPRESSION);
        ref.check(1, pyObject);
        ReferenceCounting.blackhole(pyObject);
    }

    @Test
    public void pythonObjectViaCallHasOneReference() {
        final PyObject pyObject = PyModule.getBuiltins().call("dict");
        ref.check(1, pyObject);
        ReferenceCounting.blackhole(pyObject);
    }

    @Test
    public void pythonObjectViaProxyHasOneReference() {
        final PyObject pyObject = builtins.dict();
        ref.check(1, pyObject);
        ReferenceCounting.blackhole(pyObject);
    }

    @Test
    public void viaPython() throws InterruptedException {
        PyObject.executeCode("import sys", PyInputMode.STATEMENT);
        PyObject.executeCode("x = dict()", PyInputMode.STATEMENT);
        // the extra ref counts here are due to the reference that getrefcount itself is imposing
        PyObject.executeCode("assert sys.getrefcount(x) == 2", PyInputMode.STATEMENT);
        {
            PyObject javaRef = PyObject.executeCode("x", PyInputMode.EXPRESSION);
            PyObject.executeCode("assert sys.getrefcount(x) == 3", PyInputMode.STATEMENT);
            ReferenceCounting.blackhole(javaRef);
            javaRef = null;
        }
        // let's hope GC will kick in...
        ref.gc();
        PyObject.executeCode("assert sys.getrefcount(x) == 2", PyInputMode.STATEMENT);
    }

    @Test
    public void passingObjectBackToPythonForStorageIncreasesReference() {
        final PyObject scope = builtins.dict();

        final PyObject pyObject = builtins.dict();
        ref.check(1, pyObject);

        // adding an additional reference via direct method
        scope.asDict().setItem("copy1", pyObject);
        ref.check(2, pyObject);

        // adding an additional reference via code assignment
        PyObject.executeCode("copy2=copy1", PyInputMode.STATEMENT, scope, null).close();
        ref.check(3, pyObject);

        // removing a reference via code deletion
        PyObject.executeCode("del copy1", PyInputMode.STATEMENT, scope, null).close();
        ref.check(2, pyObject);

        // removing a reference via direct method
        scope.asDict().delItem("copy2");
        ref.check(1, pyObject);

        ReferenceCounting.blackhole(scope, pyObject);
    }

    @Test
    public void bringingCopiesIntoJavaIncreasesCount() {
        final PyObject scope = builtins.dict();

        final PyObject pyObject = builtins.dict();
        ref.check(1, pyObject);

        scope.asDict().setItem("copy1", pyObject);
        ref.check(2, pyObject);

        final PyObject javaCopy1 = scope.asDict().get("copy1");
        ref.check(3, pyObject);

        final PyObject javaCopy2 = scope.asDict().get("copy1");
        ref.check(4, pyObject);

        ReferenceCounting.blackhole(scope, pyObject, javaCopy1, javaCopy2);
    }


    @Test
    public void passingObjectBackToPythonTemporarilyDoesntIncreaseReference() {
        final PyObject pyObject = builtins.dict();
        ref.check(1, pyObject);

        noop.noop(pyObject);
        ref.check(1, pyObject);

        // Ensure that if our explicit type is less specific, reference counting is still correct
        noop.noop((Object) pyObject);
        ref.check(1, pyObject);

        ReferenceCounting.blackhole(pyObject);
    }

    @Test
    public void identityFunctionOnPyObjectIncreasesCount() {
        final PyObject pyObject = builtins.dict();
        ref.check(1, pyObject);

        final PyObject copy1 = pyOut.identity(pyObject);
        ref.check(2, pyObject);

        final PyObject copy2 = pyOut.identity((Object) pyObject);
        ref.check(3, pyObject);

        ReferenceCounting.blackhole(pyObject, copy1, copy2);
    }

    @Test
    public void nativePythonObjectsCanLiveInJava() {
        // A slightly different construction, showing raw executeCode as statements instead of expressions
        PyObject.executeCode("devin = {'was': 'here'}", PyInputMode.STATEMENT);
        final PyObject devin = PyObject.executeCode("devin", PyInputMode.EXPRESSION);
        ref.check(2, devin);
        PyObject.executeCode("del devin", PyInputMode.STATEMENT);
        ref.check(1, devin);
        ReferenceCounting.blackhole(devin);
    }

    @Test
    public void pythonObjectInJavaWillDestructAfterGC() throws InterruptedException {
        // this tests fails in python 2, but I haven't spent time debugging
        assumePython3();

        final CountDownLatch latch = new CountDownLatch(1);
        {
            PyObject child = destructor.create_child(new OnDelete(latch));
            ref.check(1, child);
            ReferenceCounting.blackhole(child);
            child = null;
        }
        // let's hope GC will kick in...
        ref.gc();
        Assert.assertTrue(latch.await(1, TimeUnit.SECONDS));
    }

    @Test
    public void pythonObjectInJavaWillDestructAfterClosure() throws InterruptedException {
        // this tests fails in python 2, but I haven't spent time debugging
        assumePython3();

        final CountDownLatch latch = new CountDownLatch(1);
        try (final PyObject child = destructor.create_child(new OnDelete(latch))) {
            ref.check(1, child);
        }
        Assert.assertTrue(latch.await(1, TimeUnit.SECONDS));
    }

    @Test
    public void setAttributeDelAttributeCounted() {
        try (
                final PyObject simpleObject = SimpleObject.create(getCreateModule());
                final PyObject someValue = builtins.dict()) {
            ref.check(1, simpleObject);
            ref.check(1, someValue);

            simpleObject.setAttribute("x1", someValue);
            ref.check(1, simpleObject);
            ref.check(2, someValue);

            simpleObject.setAttribute("x2", someValue, PyObject.class);
            ref.check(1, simpleObject);
            ref.check(3, someValue);

            simpleObject.delAttribute("x1");
            ref.check(1, simpleObject);
            ref.check(2, someValue);

            simpleObject.delAttribute("x2");
            ref.check(1, simpleObject);
            ref.check(1, someValue);
        }
    }
}
