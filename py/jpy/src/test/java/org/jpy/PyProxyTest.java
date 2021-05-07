package org.jpy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.regex.Pattern;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PyProxyTest {
  private PyModule MODULE;

  @Before
  public void setUp() throws Exception {
    PyLib.startPython(new File("src/test/python/fixtures").getCanonicalPath());
    assertTrue(PyLib.isPythonRunning());

    MODULE = PyModule.importModule("proxy_test_classes");
    //PyLib.Diag.setFlags(PyLib.Diag.F_ALL);
  }

  @After
  public void tearDown() {
    //PyLib.Diag.setFlags(Diag.F_OFF);
    PyLib.stopPython();
  }

  interface HasStr {

  }

  interface DoesNotHaveStr {

  }

  interface HasToStringWithArg {
    String toString(Object arg);
  }

  interface HasHashCodeWithArg {
    int hashCode(Object arg);
  }

  interface HasHashNegativeOne {

  }

  interface HasEqualsWithoutArg {
    boolean equals();
  }

  interface EqAlwaysTrue {

  }

  interface EqAlwaysFalse {

  }

  interface ProxyAsArgument {
    boolean self_is_other(ProxyAsArgument other);
  }

  interface ThrowsException {

  }

  interface NonBooleanEq {

  }

  @Test
  public void hasStrToString() {
    PyObject pyObj = MODULE.call("HasStr");
    HasStr proxy = pyObj.createProxy(HasStr.class);

    assertEquals("This is my __str__ method", pyObj.toString());
    assertEquals("This is my __str__ method", proxy.toString());
  }

  @Test
  public void doesNotHaveStrToString() {
    PyObject pyObj = MODULE.call("DoesNotHaveStr");
    DoesNotHaveStr proxy = pyObj.createProxy(DoesNotHaveStr.class);

    // python2 uses instance, python3 uses object
    Pattern pattern = Pattern
        .compile("^<proxy_test_classes.DoesNotHaveStr (instance|object) at 0x[0-9a-f]+>$");

    String pyObjToString = pyObj.toString();
    String proxyToString = proxy.toString();

    assertTrue(pattern.matcher(pyObjToString).matches());
    assertTrue(pattern.matcher(proxyToString).matches());
    assertEquals(pyObjToString, proxyToString);
  }

  @Test
  public void hasToStringWithArg() {
    PyObject pyObj = MODULE.call("HasToStringWithArg");

    HasToStringWithArg proxy = pyObj.createProxy(HasToStringWithArg.class);

    assertEquals("weird", proxy.toString(this));
    assertNotEquals("weird", proxy.toString());
  }

  @Test
  public void hasHashCodeWithArg() {
    PyObject pyObj = MODULE.call("HasHashCodeWithArg");

    HasHashCodeWithArg proxy = pyObj.createProxy(HasHashCodeWithArg.class);

    assertEquals(0, proxy.hashCode(this));
    assertEquals(1, proxy.hashCode());
  }

  @Test
  public void hasHashCodeNegativeOne() {
    /*
    >>> class Hash:
    ...     def __hash__(self):
    ...             return -1
    ...
    >>> h = Hash()
    >>> hash(h)
    -2
    >>> h.__hash__()
    -1
     */
    PyObject pyObj = MODULE.call("HasHashNegativeOne");

    // Python reserves -1 for an error status on PyObject_Hash. When __hash__ returns -1,
    // python changes it to -2.
    assertEquals(pyObj.hash(), -2);

    HasHashNegativeOne proxy = pyObj.createProxy(HasHashNegativeOne.class);
    assertEquals(proxy.hashCode(), Long.hashCode(-2L));
  }

  @Test
  public void hasEqualsWithoutArg() {
    PyObject pyObj = MODULE.call("HasEqualsWithoutArg");

    HasEqualsWithoutArg proxy = pyObj.createProxy(HasEqualsWithoutArg.class);

    assertTrue(proxy.equals());
    assertEquals(proxy, proxy);
  }

  @Test
  public void eqAlwaysTrue() {
    PyObject pyObj1 = MODULE.call("EqAlwaysTrue");
    EqAlwaysTrue proxy1 = pyObj1.createProxy(EqAlwaysTrue.class);

    PyObject pyObj2 = MODULE.call("EqAlwaysTrue");
    EqAlwaysTrue proxy2 = pyObj2.createProxy(EqAlwaysTrue.class);

    assertEquals(proxy1, proxy1);
    assertEquals(proxy2, proxy2);

    assertEquals(proxy1, proxy2);
    assertEquals(proxy2, proxy1);

    // note: can't call assertEquals, b/c that does null checks
    assertTrue(proxy1.equals(null));
    assertTrue(proxy2.equals(null));
  }

  @Test
  public void eqAlwaysFalse() {
    PyObject pyObj1 = MODULE.call("EqAlwaysFalse");
    EqAlwaysFalse proxy1 = pyObj1.createProxy(EqAlwaysFalse.class);

    PyObject pyObj2 = MODULE.call("EqAlwaysFalse");
    EqAlwaysFalse proxy2 = pyObj2.createProxy(EqAlwaysFalse.class);

    assertNotEquals(proxy1, proxy1);
    assertNotEquals(proxy2, proxy2);

    assertNotEquals(proxy1, proxy2);
    assertNotEquals(proxy2, proxy1);

    // note: can't call assertEquals, b/c that does null checks
    assertFalse(proxy1.equals(null));
    assertFalse(proxy2.equals(null));
  }

  @Test
  public void eqAlwaysTrueVsFalse() {
    /*
    >>> class EqAlwaysTrue:
    ...     def __eq__(self, other):
    ...             return True
    ...
    >>> class EqAlwaysFalse:
    ...     def __eq__(self, other):
    ...             return False
    ...
    >>> a = A()
    >>> b = B()
    >>> a == b
    True
    >>> b == a
    False
     */

    PyObject pyObj1 = MODULE.call("EqAlwaysTrue");
    EqAlwaysTrue alwaysTrue = pyObj1.createProxy(EqAlwaysTrue.class);

    PyObject pyObj2 = MODULE.call("EqAlwaysFalse");
    EqAlwaysFalse alwaysFalse = pyObj2.createProxy(EqAlwaysFalse.class);

    assertEquals(alwaysTrue, alwaysFalse);
    assertNotEquals(alwaysFalse, alwaysTrue);
  }

  @Test
  public void canUnwrapProxyPassedAsArgument() {
    PyObject pyObject1 = MODULE.call("ProxyAsArgument");
    ProxyAsArgument proxy1 = pyObject1.createProxy(ProxyAsArgument.class);

    PyObject pyObject2 = MODULE.call("ProxyAsArgument");
    ProxyAsArgument proxy2 = pyObject2.createProxy(ProxyAsArgument.class);

    assertTrue(proxy1.self_is_other(proxy1));
    assertTrue(proxy2.self_is_other(proxy2));

    assertFalse(proxy1.self_is_other(proxy2));
    assertFalse(proxy2.self_is_other(proxy1));

    assertEquals(proxy1, proxy1);
    assertEquals(proxy2, proxy2);

    assertNotEquals(proxy1, proxy2);
    assertNotEquals(proxy2, proxy1);
  }

  @Test
  public void equalsException() {
    PyObject pyObject = MODULE.call("ThrowsException");
    ThrowsException throwsException = pyObject
        .createProxy(ThrowsException.class);
    try {
      throwsException.equals(throwsException);
      fail("Expected an RuntimeException to be thrown");
    } catch (RuntimeException e) {
      //throw e;
      //assertTrue(e.getMessage().contains("Value: this __eq__ always raises Exception"));
    }
  }

  @Test
  public void hashCodeException() {
    PyObject pyObject = MODULE.call("ThrowsException");
    ThrowsException throwsException = pyObject
        .createProxy(ThrowsException.class);
    try {
      throwsException.hashCode();
      fail("Expected an RuntimeException to be thrown");
    } catch (RuntimeException e) {
      //assertTrue(e.getMessage().contains("Value: this __hash__ always raises Exception"));
    }
  }

  @Test
  public void toStringException() {
    PyObject pyObject = MODULE.call("ThrowsException");
    ThrowsException throwsException = pyObject
        .createProxy(ThrowsException.class);
    try {
      throwsException.toString();
      fail("Expected an RuntimeException to be thrown");
    } catch (RuntimeException e) {
      //assertTrue(e.getMessage().contains("Value: this __str__ always raises Exception"));
    }
  }

  @Test
  public void nonBooleanEquals() {
    PyObject pyObject = MODULE.call("NonBooleanEq");
    NonBooleanEq proxy = pyObject.createProxy(NonBooleanEq.class);
    assertEquals(proxy, proxy);
  }
}
