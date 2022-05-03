import unittest
import jpyutil
import jpy

def some_function(x):
  return x * x

def some_function2(x):
  return x * x * x

class TheContext:
  def __init__(self, amount):
    self.amount = amount

  def plus(self, other):
    return self.amount + other

# Note: all jpy unit testing should go in this class for now, due to
# IDS-4102
class TestJpy(unittest.TestCase):
  # While we would like to do this, we *can't* until
  # IDS-4102 is fixed
  '''
  def setUp(self):
    jpyutil.init_jvm()

  def tearDown(self):
    jpy.destroy_jvm()
  '''

  @classmethod
  def setUpClass(cls):
    jpyutil.init_jvm()

  @classmethod
  def tearDownClass(cls):
    jpy.destroy_jvm()

  def test_has_jvm(self):
    self.assertTrue(jpy.has_jvm())

  def test_has_jvm_again(self):
    self.assertTrue(jpy.has_jvm())

  def test_has_local_classes(self):
    jpy.get_type('io.deephaven.jpy.integration.Empty')

  def test_has_local_classes_dne(self):
    with self.assertRaises(ValueError):
      jpy.get_type('io.deephaven.jpy.integration.DoesNotExist')

  def test_has_jpy_classes(self):
    jpy.get_type('org.jpy.PyLib')

  def test_has_gil(self):
    PyLib = jpy.get_type('org.jpy.PyLib')
    self.assertFalse(PyLib.hasGil())

  def test_reenter_python(self):
    ReenterPython = jpy.get_type('io.deephaven.jpy.integration.ReenterPython')
    self.assertEquals(42, ReenterPython.calc1Plus41InPython())

  def test_ping_pong_stack(self):
    PingPongStack = jpy.get_type('io.deephaven.jpy.integration.PingPongStack')
    self.assertEquals('test_jpy(java,5)(python,4)(java,3)(python,2)(java,1)', PingPongStack.pingPongPython('test_jpy', 5))
    self.assertEquals('test_jpy(java,4)(python,3)(java,2)(python,1)', PingPongStack.pingPongPython('test_jpy', 4))

  # todo: consider running tests where JPY is *not* on the classpath, which is a completely acceptable use case
  def test_org_jpy_pylib(self):
    jpy.get_type('org.jpy.PyLib')

  def test_pass_function_to_java(self):
    PassPyObjectToJava = jpy.get_type('io/deephaven/jpy/integration/PassPyObjectToJava')
    #jpy.diag.flags = jpy.diag.F_ALL
    PassPyObjectToJava.from_python_with_love(some_function)

  def test_pass_function_to_java_var(self):
    PassPyObjectToJava = jpy.get_type('io/deephaven/jpy/integration/PassPyObjectToJava')
    #jpy.diag.flags = jpy.diag.F_ALL
    PassPyObjectToJava.from_python_with_love_var(some_function, some_function2)

  def test_pass_the_context_to_java(self):
    PassPyObjectToJava = jpy.get_type('io/deephaven/jpy/integration/PassPyObjectToJava')

    context_42 = TheContext(42)
    self.assertEquals(43, PassPyObjectToJava.invoke_the_context_plus(context_42, 1))

    context_99 = TheContext(99)
    self.assertEquals(104, PassPyObjectToJava.invoke_the_context_plus(context_99, 5))

  def test_py_object_overload_test_1(self):
    PassPyObjectToJava = jpy.get_type('io/deephaven/jpy/integration/PassPyObjectToJava')
    self.assertEquals("String", PassPyObjectToJava.overload_test_1('a string'))
    self.assertEquals("PyObject", PassPyObjectToJava.overload_test_1(42))

  def test_numpy_array(self):
    import numpy
    jpy_array = jpy.array('int', range(100))
    jpy_array_id = id(jpy_array)
    jpy_array_refcount = get_refcount(jpy_array_id)
    np_array = numpy.frombuffer(jpy_array, numpy.int32)
    self.assertEqual(list(jpy_array), list(np_array))
    self.assertEqual(get_refcount(jpy_array_id), jpy_array_refcount + 1)
    np_array = None
    self.assertEqual(get_refcount(jpy_array_id), jpy_array_refcount)

    mv = memoryview(b'123412341234')
    mv_id = id(mv)
    mv_refcount = get_refcount(mv_id)
    np_array = numpy.frombuffer(mv, numpy.int32)
    self.assertEqual(get_refcount(mv_id), mv_refcount + 1)
    np_array = None
    self.assertEqual(get_refcount(mv_id), mv_refcount)

  def test_pyobject_unwrap(self):
    class CustomClass:
      def __init__(self):
        pass

    obj = CustomClass()
    obj_id = id(obj)
    self.assertEqual(get_refcount(obj_id), 1)

    # Note: a temporary PyObject is created, and that holds onto a ref until Java GCs.
    # While the following counts are racy, it is probably very rare to fail here.
    echo = jpy.get_type('io.deephaven.jpy.integration.Echo').echo(obj)
    self.assertTrue(obj is echo)
    self.assertEqual(get_refcount(obj_id), 3)

    del obj
    self.assertEqual(get_refcount(obj_id), 2)

    del echo
    self.assertEqual(get_refcount(obj_id), 1)

  def test_pyobject_unwrap_via_array(self):
    # Very similar to test_pyobject_unwrap, but we are doing the unwrapping via array
    class CustomClass:
      def __init__(self):
        pass

    obj = CustomClass()
    obj_id = id(obj)
    self.assertEqual(get_refcount(obj_id), 1)

    obj_in_array = jpy.array('org.jpy.PyObject', [obj])
    self.assertEqual(get_refcount(obj_id), 2)

    extracted_obj = obj_in_array[0]
    self.assertTrue(obj is extracted_obj)
    self.assertEqual(get_refcount(obj_id), 3)

    del extracted_obj
    self.assertEqual(get_refcount(obj_id), 2)

    del obj
    self.assertEqual(get_refcount(obj_id), 1)

    # Note: the ref count will not decrease until Java GCs and PyObject does the decRef.
    # While this del + check is racy, it is probably very rare to fail here.
    del obj_in_array
    self.assertEqual(get_refcount(obj_id), 1)


  def test_pyproxy_unwrap(self):
    class SomeJavaInterfaceImpl:
      def __init__(self):
        pass

      def foo(self, bar, baz):
        return bar + baz

    obj = SomeJavaInterfaceImpl()
    obj_id = id(obj)
    self.assertEqual(get_refcount(obj_id), 1)

    # Note: a temporary PyObject is created, and that holds onto a ref until Java GCs.
    # While the following counts are racy, it is probably very rare to fail here.
    obj_proxy = jpy.get_type('io.deephaven.jpy.integration.SomeJavaInterface').proxy(obj)
    self.assertTrue(obj is obj_proxy)
    self.assertEqual(get_refcount(obj_id), 3)

    del obj
    self.assertEqual(get_refcount(obj_id), 2)

    del obj_proxy
    self.assertEqual(get_refcount(obj_id), 1)


def get_refcount(obj_id):
  import ctypes
  return ctypes.c_long.from_address(obj_id).value

