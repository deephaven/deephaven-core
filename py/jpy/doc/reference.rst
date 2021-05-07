#########
Reference
#########


**********
Python API
**********


This reference addresses the jpy Python module.

jpy Functions
=============

.. py:function:: create_jvm(options)
    :module: jpy

    Create the Java VM  using the given *options* sequence of strings. Possible option strings are of the form:

    +-------------------------------+------------------------------------------------------------------------------------------------------------------------+
    | Option                        |  Meaning                                                                                                               |
    +===============================+========================================================================================================================+
    | ``-D<name>=<value>``          |  Set a `Java system property <http://docs.oracle.com/javase/7/docs/api/java/lang/System.html#getProperties%28%29>`_.   |
    |                               |  The most important system property is ``java.class.path`` to include your Java libraries. You may also consider       |
    |                               |  ``java.library.path`` if your Java code uses native code provided in shared libraries.                                |
    +-------------------------------+------------------------------------------------------------------------------------------------------------------------+
    | ``-verbose[:class|gc|jni]``   |  Enable verbose output. The options can be followed by a comma-separated list                                          |
    |                               |  of names indicating what kind of messages will be printed by the JVM.                                                 |
    |                               |  For example, ``-verbose:gc,class`` instructs the JVM to print GC and class                                            |
    |                               |  loading related messages. Standard names include: gc, class, and jni.                                                 |
    +-------------------------------+------------------------------------------------------------------------------------------------------------------------+
    | ``-X<value>``                 |  Set a non-standard JVM option which usually begins with ``-X`` or an underscore. For example,                         |
    |                               |  the Oracle JDK/JRE supports ``-Xms`` and ``-Xmx`` options to allow programmers specify the initial                    |
    |                               |  and maximum heap size. Please refer to the documentation of the used Java Runtime Environment (JRE).                  |
    +-------------------------------+------------------------------------------------------------------------------------------------------------------------+

    The function throws a runtime error on failure. It has no return value.

    Usage example::

        jpy.create_jvm(['-Xmx512M', '-Djava.class.path=/usr/home/norman/jpy-test/classes'])



.. py:function:: destroy_jvm()
    :module: jpy

    Destroy the Java Virtual Machine. The function has no effect if the JVM is has not yet been created or has already
    been destroyed. No return value.


.. py:function:: get_type(name, resolve=False)
    :module: jpy

    Return a type object for the given, fully qualified Java type *name* which is the name of a Java primitive type,
    a Java class name, or a Java array type name.

    Java class names must be fully qualified, e.g. ``'java.awt.Point'``. For inner classes a dollar sign is
    used to separate it from its containing class, e.g. ``'java.awt.geom.Ellipse2D$Float'``.

    Java array type names have a trailing opening bracket, followed by either a Java class name and a trailing semicolon
    or followed by one of the primitive type indicators:

    * ``'Z'``, the Java ``boolean`` type (an 8-bit Boolean value)
    * ``'C'``, Java ``char`` type (a 16-bit unicode character)
    * ``'B'``, Java ``byte`` type (an 8-bit signed integer number)
    * ``'S'``, Java ``short`` type (a 16-bit signed integer number)
    * ``'I'``, Java ``int`` type (a 32-bit signed integer number)
    * ``'J'``, Java ``long`` type (a 64-bit signed integer number)
    * ``'F'``, Java ``float`` type (a 32-bit floating point number)
    * ``'D'``, Java ``double`` type (a 64-bit floating point number)

    Examples: ``'[java.awt.Point;'`` (1d object array), ``'[[[F'`` (3d float array).

    If the returned Java type has public constructors it can be used to create Java object instances in the same way
    Python objects are created from their types, e.g.::

        String = jpy.get_type('java.lang.String')
        s = String(‘Hello jpy!’)
        s = s.substring(0, 5)

    The returned Java types are also used to access the type's static fields and methods::

        Runtime = jpy.get_type('java.lang.Runtime')
        rt = Runtime.getRuntime()
        tm = rt.totalMemory()

    The returned Java types have a `jclass` attribute which returns the actual Java object. This allows for using
    the Java types where a Java method would expect a parameter of type `java.lang.Class`.

    To instantiate Java array objects, the :py:func:`jpy.array()` function is used.

    Implementation note: All types loaded so far from the Java VM are stored in the global :py:data:`jpy.types` variable.
    If the requested type does not already exists in :py:data:`jpy.types`, the class is newly loaded from the Java VM.
    The root class of all Java types retrieved that way is :py:class:`jpy.JType`.

    Make sure that :py:func:`jpy.create_jvm()` has already been called. Otherwise the function fails with a runtime
    exception.


.. py:function:: array(item_type, init)
    :module: jpy

    Create a Java array object for the given *item_type* and of the given initializer *init*.

    *item_type* may be a *type* object as returned by the :py:func:`jpy.get_type()` function or a type *name* as it is
    used for the :py:func:`jpy.get_type()` function. In addition, the name of a Java primitive type can be used:

    * ``'boolean'`` (an 8-bit Boolean value)
    * ``'char'`` (a 16-bit unicode character)
    * ``'byte'`` (an 8-bit signed integer number)
    * ``'short'`` (a 16-bit signed integer number)
    * ``'int'`` (a 32-bit signed integer number)
    * ``'long'`` (a 64-bit signed integer number)
    * ``'float'`` (a 32-bit floating point number)
    * ``'double'`` (a 64-bit floating point number)

    The value for the *init* parameter may bei either an array length in the range ``0`` to ``2**31-1`` or a sequence
    of objects which all must be convertible to the given *item_type*.

    Make sure that :py:func:`jpy.create_jvm()` has already been called. Otherwise the function fails with a runtime
    exception.

    Examples:::

        a = jpy.array('java.lang.String', ['A', 'B', 'C'])
        a = jpy.array('int', [1, 2, 3])
        a = jpy.array('float', 512)



.. py:function:: cast(jobj, type)
    :module: jpy

    Convert a Java object to a Java object with the given Java *type* (type object or type name, see
    :py:func:`jpy.get_type()`). If *jobj* is already of *type*, *jobj* is returned. If *jobj* is an instance of
    *type*, a new wrapper object will be created for this type, otherwise ``None`` is returned.

    This function is useful if you need to convert the `java.util.Object` values returned e.g. by Java collections
    (implementations of the `java.util.Set`, `java.util.Map`, `java.util.List` & Co.) to specific types. For example::

        ArrayList = jpy.get_type('java.util.ArrayList')
        File = jpy.get_type('java.io.File')
        al = ArrayList()
        al.add(File('/home/bibo/.jpy'))
        item = al.get(0)
        # item has type java.util.Object, but actually is a java.io.File
        print(type(item))
        item = jpy.cast(item, File)
        # item has now type java.io.File
        print(type(item))

    Make sure that :py:func:`jpy.create_jvm()` has already been called. Otherwise the function fails with a runtime
    exception.

Variables
=========

.. py:data:: types
    :module: jpy

    A dictionary that maps Java class names to the respective Python type objects (wrapped Java classes).
    You should never modify the value of this variable nor directly modify the dictionary's contents.

.. py:data:: type_callbacks
    :module: jpy

    Contains callbacks which are called before jpy translates Java methods to Python methods while Java classes are
    being loaded. These callbacks can be used to annotate Java methods so that jpy can better translate them to
    Python. This is a powerful but advanced jpy feature that you usually don't have to use.

    Consider a Java method::

        double[] readData(long offset, int length, double[] data);

    of some Java class ``Reader``. From the method's documentation we know that if we pass ``null`` for *data*,
    it will create a new array of the given length, read data into it and the return that instance.
    If we pass an existing array it will be reused instead. From plain Java class introspection, jpy can neither
    detect if a primitive array parameter is modified by a method and/or whether it shall serve as the method's
    return value.

    To overcome the problem of such semantics inherent to a Java method implementation, jpy uses a dictionary
    ``type_callbacks`` in which you can register a Java class name with a callable of following signature: ::

        callback(type, method)

    This can be used to equip specific Java methods of a class with additional information while the Java class
    is being loaded from the Java VM. *type* is the Java class and *method* is the current class method being
    loaded. *method* is of type :py:class:`jpy.JMethod`. The callback should return either ``True`` or ``False``.
    If it returns ``False``, jpy will not add the given method to the Python version of the Java class.

    Here is an example: ::

        def annotate_Reader_readData_methods(type, method):
            if method.name == 'readData' and method.param_count == 3:
                param_type_str = str(method.get_param_type(1))
                if param_type_str == "<class '[I'>" || param_type_str == "<class '[D'>":
                    method.set_param_mutable(2, True)
                    method.set_param_return(2, True)
            return True

        class_name = 'com.acme.Reader'
        jpy.type_callbacks[class_name] = annotate_Reader_readData_methods
        # This will invoke the callback above
        Reader = jpy.get_type(class_name)

    Once a method parameter is annotated that way, jpy can transfer the semantics of a Java method to Python.
    For example::

        import numpy as np

        r = Reader('test.tif')
        a = np.array(1024, np.dtype=np.float64)
        a = r.read(0, len(a), a)
        r.close()

    Here a call to the ``read`` method will modify the numpy array's content as desired and return the
    same array instance as indicated by the Java method's specification.

.. py:data:: type_translations
    :module: jpy

    Contains callbacks which are called when instantiating a Python object from a Java object.
    After the standard wrapping of the Java object as a Python object, the Java type name is looked up in this
    dictionary.  If the returned item is a callable, the callable is called with the JPy object as an argument,
    and the callable's result is returned to the user.


.. py:data:: VerboseExceptions.enabled
    :module: jpy

    If set to true, then jpy will produce more verbose exception messages; which include the full Java stack trace.
    If set to false, then jpy produces exceptions using only the underlying Java exception's toString method.

.. py:data:: diag
    :module: jpy

    An object used to control output of diagnostic information for debugging. This variable is only useful for jpy
    modification and further development.

.. py:data:: diag.flags
    :module: jpy

    Integer bit-combination of diagnostic flags (see following F_* constants).
    If this value is not zero, diagnostic messages are printed to the standard output stream for any subsequent
    jpy library calls. Its default value is ``jpy.diag.F_OFF`` which is zero.

    For example::

        jpy.diag.flags = jpy.diag.F_EXEC + jpy.diag.F_JVM

    The following flags are defined:

    * ``F_OFF`` - Don't print any diagnostic messages
    * ``F_ERR`` - Errors: print diagnostic information when erroneous states are detected
    * ``F_TYPE`` - Type resolution: print diagnostic messages while generating Python classes from Java classes
    * ``F_METH`` - Method resolution: print diagnostic messages while resolving Java overloaded methods
    * ``F_EXEC`` - Execution: print diagnostic messages when Java code is executed
    * ``F_MEM`` - Memory: print diagnostic messages when wrapped Java objects are allocated/deallocated
    * ``F_JVM`` - JVM: print diagnostic information usage of the Java VM Invocation API
    * ``F_ALL`` - Print all possible diagnostic messages


Types
=====

You will never have to use the following type directly. But it may be of use to know where they come from when they are
referred to, e.g. in error messages.

.. py:class:: JType
    :module: jpy

    This type is the base class for all type representing Java classes. It is actually a meta-type used to dynamically
    create Python type instances from loaded Java classes. Such derived types are returned by
    :py:func:`jpy.get_type` instead or can be directly looked up in :py:data:`jpy.types`.


.. py:class:: JOverloadedMethod
    :module: jpy

    This type represents an overloaded Java method. It is composed of one or more :py:class:`jpy.JMethod` objects.


.. py:class:: JMethod
    :module: jpy

    This type represents a Java method. It is part of a :py:class:`jpy.JOverloadedMethod`.

    .. py:attribute:: name

        The method's name. Read-only attribute.

    .. py:attribute:: return_type

        The method's return type.  Read-only attribute.

    .. py:attribute:: param_count

        The method's parameter count.  Read-only attribute.

    .. py:method:: JMethod.get_param_type(i) -> type

        Get the type of the *i*-th Java method parameter.

    .. py:method:: JMethod.is_param_return(i) -> bool

        Return ``True`` if arguments passed to the *i*-th Java method parameter will be the return value of the method, ``False`` otherwise.

    .. py:method:: JMethod.set_param_return(i, value)

        Set if arguments passed to the *i*-th Java method parameter will be the return value of the method, with *value* being a Boolean.

    .. py:method:: JMethod.is_param_output(i) -> bool

        Return ``True`` if the arguments passed to the *i*-th Java method parameter is a mere output (and not read from), ``False`` otherwise.

    .. py:method:: JMethod.set_param_output(i, value)

        Set if arguments passed to the *i*-th Java method parameter is a mere output (and not read from), with *value* being a Boolean.
        Used to optimise Python buffer to Java array parameter passing.

    .. py:method:: JMethod.is_param_mutable(i) -> bool

        Return ``True`` if the arguments passed to the *i*-th Java method parameter is mutable, ``False`` otherwise.

    .. py:method:: JMethod.set_param_mutable(i, value)

        Set if arguments passed to the *i*-th Java method parameter is mutable, with *value* being a Boolean.


.. py:class:: JField
    :module: jpy

    This type represents is used to represent Java class fields.


Type Conversions
================

This section describes the type possible type conversions made by jpy when Python values are passed as arguments
to Java typed parameters. In the tables given below are the generated match values ranging from (types never match)
to 100 (full match) when comparing a given Java parameter type (rows) with a provided Python value (columns). These
match values are also used for finding the best matching Java method overload for a given Python argument tuple.


Java primitive types
--------------------


+--------------+--------------+----------+---------+------------+--------+
|              | ``NoneType`` | ``bool`` | ``int`` |  ``float`` | number |
+==============+==============+==========+=========+============+========+
| ``boolean``  |      1       |   100    |    10   |      0     |    0   |
+--------------+--------------+----------+---------+------------+--------+
| ``char``     |      0       |    10    |   100   |      0     |    0   |
+--------------+--------------+----------+---------+------------+--------+
| ``byte``     |      0       |    10    |   100   |      0     |    0   |
+--------------+--------------+----------+---------+------------+--------+
| ``short``    |      0       |    10    |   100   |      0     |    0   |
+--------------+--------------+----------+---------+------------+--------+
| ``int``      |      0       |    10    |   100   |      0     |    0   |
+--------------+--------------+----------+---------+------------+--------+
| ``long``     |      0       |    10    |   100   |      0     |    0   |
+--------------+--------------+----------+---------+------------+--------+
| ``float``    |      0       |     1    |    10   |     90     |   50   |
+--------------+--------------+----------+---------+------------+--------+
| ``double``   |      0       |     1    |    10   |    100     |   50   |
+--------------+--------------+----------+---------+------------+--------+

Java object types
-----------------

+-------------------------+--------------+----------+---------+------------+---------+
|                         | ``NoneType`` | ``bool`` | ``int`` |  ``float`` | ``str`` |
+=========================+==============+==========+=========+============+=========+
| ``java.lang.Boolean``   |      1       |   100    |    10   |      0     |    0    |
+-------------------------+--------------+----------+---------+------------+---------+
| ``java.lang.Character`` |      1       |    10    |   100   |      0     |    0    |
+-------------------------+--------------+----------+---------+------------+---------+
| ``java.lang.Byte``      |      1       |    10    |   100   |      0     |    0    |
+-------------------------+--------------+----------+---------+------------+---------+
| ``java.lang.Short``     |      1       |    10    |   100   |      0     |    0    |
+-------------------------+--------------+----------+---------+------------+---------+
| ``java.lang.Integer``   |      1       |    10    |   100   |      0     |    0    |
+-------------------------+--------------+----------+---------+------------+---------+
| ``java.lang.Long``      |      1       |    10    |   100   |      0     |    0    |
+-------------------------+--------------+----------+---------+------------+---------+
| ``java.lang.Float``     |      1       |     1    |    10   |     90     |    0    |
+-------------------------+--------------+----------+---------+------------+---------+
| ``java.lang.Double``    |      1       |     1    |    10   |    100     |    0    |
+-------------------------+--------------+----------+---------+------------+---------+
| ``java.lang.String``    |      1       |     0    |     0   |      0     |   100   |
+-------------------------+--------------+----------+---------+------------+---------+
| ``java.lang.Object``    |      1       |    10    |    10   |     10     |    10   |
+-------------------------+--------------+----------+---------+------------+---------+jpy

Java primitive array types
--------------------------

+----------------+--------------+-----+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+
|                | ``NoneType`` | seq | buf('b') | buf('B') | buf('u') | buf('h') | buf('H') | buf('i') | buf('I') | buf('l') | buf('L') | buf('q') | buf('Q') | buf('f') | buf('d') |
+================+==============+=====+==========+==========+==========+==========+==========+==========+==========+==========+==========+==========+==========+==========+==========+
| ``boolean[]``  |      1       |  10 |   100    |   100    |     0    |     0    |     0    |     0    |     0    |     0    |     0    |     0    |     0    |     0    |     0    |
+----------------+--------------+-----+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+
| ``char[]``     |      1       |  10 |     0    |     0    |   100    |    80    |    90    |     0    |     0    |     0    |     0    |     0    |     0    |     0    |     0    |
+----------------+--------------+-----+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+
| ``byte[]``     |      1       |  10 |   100    |    90    |     0    |     0    |     0    |     0    |     0    |     0    |     0    |     0    |     0    |     0    |     0    |
+----------------+--------------+-----+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+
| ``short[]``    |      1       |  10 |     0    |     0    |     0    |   100    |    90    |     0    |     0    |     0    |     0    |     0    |     0    |     0    |     0    |
+----------------+--------------+-----+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+
| ``int[]``      |      1       |  10 |     0    |     0    |     0    |     0    |     0    |   100    |    90    |   100    |    90    |     0    |     0    |     0    |     0    |
+----------------+--------------+-----+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+
| ``long[]``     |      1       |  10 |     0    |     0    |     0    |     0    |     0    |     0    |     0    |     0    |     0    |   100    |    90    |     0    |     0    |
+----------------+--------------+-----+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+
| ``float[]``    |      1       |  10 |     0    |     0    |     0    |     0    |     0    |     0    |     0    |     0    |     0    |     0    |     0    |   100    |     0    |
+----------------+--------------+-----+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+
| ``double[]``   |      1       |  10 |     0    |     0    |     0    |     0    |     0    |     0    |     0    |     0    |     0    |     0    |     0    |     0    |   100    |
+----------------+--------------+-----+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+

If a python buffer is passed as argument to a primitive array parameter, but it doesn't match the buffer types
given above, the a match value of 10 applies, as long as the item size of a buffer matches the Java array item size.

Java object array types
-----------------------

For String arrays, if a sequence is matched with a value of 80 if all the elements in the sequence are Python strings.


todo



********
Java API
********

jpy's Java API documentation has been generated from Java source code using the javadoc tool.
It can be found `here <_static/java-apidocs/index.html>`_.
