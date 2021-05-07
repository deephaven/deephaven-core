############
Introduction
############

jpy is a **bi-directional** Python-Java bridge which you can use to embed Java code in Python programs or the other
way round. It has been designed particularly with regard to maximum data transfer speed between the two languages.
It comes with a number of outstanding features:

* Fully translates Java class hierarchies to Python
* Transparently handles Java method overloading
* Support of Java multi-threading
* Fast and memory-efficient support of primitive Java array parameters via `Python buffers <http://docs.python.org/3.3/c-api/buffer.html>`_
  (e.g. `numpy arrays <http://docs.scipy.org/doc/numpy/reference/arrays.html>`_)
* Support of Java methods that modify primitive Java array parameters (mutable parameters)
* Java arrays translate into Python sequence objects
* Java API for accessing Python objects (``jpy.jar``)

jpy has been tested with Python 2.7, 3.3, 3.4 and  Oracle Java 7 and 8 JDKs. It will presumably also work with Python 2.6 or
3.2 and a Java 6 JDK.

The initial development of jpy has been driven by the need to write Python extensions to an established scientific
imaging application programmed in Java, namely the `BEAM <http://www.brockmann-consult.de/beam/>`_ toolbox
funded by the European Space Agency (ESA).
Writing such Python plug-ins for a Java application usually requires a bi-directional communication between Python and
Java since the Python extension code must be able to call back into the Java APIs.


************
How it works
************

The jpy Python module is entirely written in the C programming language. The same resulting shared library is used
as a Python jpy module and also as native library for the Java library (``jpy.jar``).

Python programs that import the ``jpy`` module can load Java classes, access Java class fields, and call class
constructors and methods.

Java programs with ``jpy.jar`` on the classpath can import Python modules, access module attributes such as class
types and variables, and call any callable objects such as module-level functions, class constructors, as well as
static and instance class methods.


Calling Java from Python
========================

Instantiate Python objects from Java classes and call their public methods and fields::

    import jpy

    File = jpy.get_type('java.io.File')

    file = File('test/it')
    name = file.getName()



Calling Python from Java
========================

Access Python attributes and call Python functions from Java::

    PyModule sys = PyModule.importModule("sys");
    PyObject path = sys.getAttribute("path");
    path.call("append", "/usr/home/norman/");
    String value = path.getStringValue();


Implementing Java interfaces using Python
=========================================

With jpy you can implement Java interfaces using Python. We instantiating Java (proxy) objects from Python modules or
classes. If you call methods of the resulting Java object, jpy will delegate the calls to the matching Python
module functions or class methods. Here is how this works.

Assuming we have a Java interface ``PlugIn.java`` ::

    public interface PlugIn {
        String[] process(String arg);
    }

and a Python implementation ``bibo_plugin.py`` ::

    class BiboPlugIn:
        def process(self, arg):
            return arg.split();


then we can call the Python code from Java as follows ::

    // Import the Python module
    PyModule plugInModule = PyLib.importModule("bibo_plugin");

    // Call the Python class to instantiate an object
    PyObject plugInObj = plugInModule.call("BiboPlugin");

    // Create a Java proxy object for the Python object
    PlugIn plugIn = plugInObj.createProxy(PlugIn.class);

    String[] result = plugIn.process('Abcdefghi jkl mnopqr stuv wxy z');


*******************
Current limitations
*******************

* Java non-final, static class fields are currently not supported:
  The reason is that Java classes are represented in jpy's Python API as dynamically allocated, built-in
  extension types. Built-in extension types cannot have (as of Python 3.3) static, computed
  attributes which we would need for getting/setting Java static class fields.
* Public final static fields are represented as normal (non-computed) type attributes:
  Their values are Python representations of the final Java values. The limitation here is, that they
  can be overwritten from Python, because Python does not know final/constant attributes. This could
  only be achieved with computed attributes, but as said before, they are not supported for
  built-in extension types.
* It is currently not possible to shutdown the Java VM from Python and then restart it.


********************************
Other projects with similar aims
********************************

* `JPype <http://jpype.sourceforge.net/>`_ - allow python programs full access to java class libraries
* `Jython <http://www.jython.org/>`_ - Python for the Java Platform
* `JyNI <http://jyni.org/>`_ - Jython Native Interface
* `Jynx <https://code.google.com/p/jynx/>`_ - improve integration of Java with Python
