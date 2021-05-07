############
Installation
############


jpy's installation is currently the full build process from sources.
We will try to ease the installation process in the future.

After successful installation you will be able

* to use Java from Python by importing the jpy module ``import jpy`` and
* to use Python from Java by importing the jpy Java API classes ``import org.jpy.*;`` from ``jpy.jar`` on your Java classpath.

*******************
Getting the Sources
*******************

The first step is to clone the jpy repository or download the sources from the `jpy Project page <https://github.com/bcdev/jpy>`_.
We recommend you clone the repository using the `git <http://git-scm.com/>`_ tool::

    git clone https://github.com/bcdev/jpy.git

If you don't want to use git, you can also download stable source releases from the
`jpy releases page <https://github.com/bcdev/jpy/releases>`_ on GitHub.

In the following it is assumed that the jpy sources are either checked out or unpacked into a directory named ``jpy``.

.. _build:

******************
Build from Sources
******************

Change into the checkout directory (``cd jpy``) and follow the build steps below. After successful build, the
``build`` directory will contain the platform-dependent jpy versions:

    build/
        lib-*os-platform*-*python-version*/
            jpy.so (Unixes only)
            jdl.so (Unixes only)
            jpy.pyd (Windows only)
            jdl.pyd (Windows only)
            jpyutil.py
            jpyconfig.py
            jpyconfig.properties


************************
Running Java from Python
************************

In order to use jpy from Python you will have to add the respective directory ``lib-``*os-platform*``-``*python-version*
for your platform and Python version to your Python path. This can be done either programmatically in Python, e.g.

    import sys
    sys.path.append('build/lib-*os-platform*-*python-version*')

You could also alter the ``PYTHONPATH`` environment variable,

    export PYTHONPATH=$PYTHONPATH:build/lib-*os-platform*-*python-version*

Finally you could copy the contained files into your Python
installation's ``site-packages`` directory to make jpy permanently available.

************************
Running Python from Java
************************

To use the jpy Java API, put ``lib/jpy.jar`` on your classpath. Or if you use Maven add the following dependency to your
project:

    <dependency>
        <groupId>org.jpy</groupId>
        <artifactId>jpy</artifactId>
        <version>0.8</version>
    </dependency>

The jpy Java API requires a maximum of two configuration parameters:

* ``jpy.jpyLib`` - path to the 'jpy' Python module, namely the ``jpy.so`` (Unix) or ``jpy.pyd`` (Windows) file
* ``jpy.jdlLib`` - path to the 'jdl' Python module, namely the ``jpy.so`` (Unix) file. Not used on Windows.

Another optional parameter

* ``jpy.debug`` - which is either ``true`` or ``false`` can be used to output extra debugging information.

All the parameters can be passed directly to the JVM either as Java system properties or by using the single system property

* ``jpy.config`` - which is a path to a Java properties files containing the definitions of the two parameters named above.

Such property file is also written for each build and is found in ``build/lib-<os-platform>-<python-version>/jpyconfig.properties``.

Setting PYTHONHOME
------------------

If the environment variable ``PYTHONHOME`` is not set when you call Python from Java, you may get an error about
file system encodings not being found. It is possible to set the location of Python from your
Java program.  Use ``PyLib.setPythonHome(pathToPythonHome)`` to do that, where ``pathToPythonHome`` is a ``String`` that 
contains the location of the Python installation.

========================
Build for Linux / Darwin
========================

You will need

* `Python 2.7 or 3.3 <http://www.python.org/>`_ or higher
* `Oracle JDK 7 <http://www.oracle.com/technetwork/java/javase/downloads/>`_ or higher
* `Maven 3 <http://maven.apache.org/>`_ or higher
* For Linux: ``gcc``
* For Darwin: `Xcode <https://itunes.apple.com/de/app/xcode/id497799835?mt=12>`_

To build and test the jpy Python module use the following commands::

    export JDK_HOME=<path to the JDK installation directory>
    export JAVA_HOME=$JDK_HOME
    python setup.py --maven build

where ``JAVA_HOME`` is used by Maven and ``JDK_HOME`` by ``setup.py``.
On Darwin, you may find the current JDK/Java home using the following expression::

    export JDK_HOME=$(/usr/libexec/java_home)

If you encounter linkage errors during setup saying that something like a ``libjvm.so`` (Linux) or ``libjvm.dylib``
(Darwin) cannot be found, then you can try adding its containing directory to the ``LD_LIBRARY_PATH`` environment
variable, e.g.::

    export LD_LIBRARY_PATH=$JDK_HOME/jre/lib/server:$LD_LIBRARY_PATH


===========================
Build for Microsoft Windows
===========================

Python 2.7
----------

You will need

* `Python 2.7 <http://www.python.org/>`_ or higher (2.6 may work as well but is not tested)
* `Oracle JDK 7 <http://www.oracle.com/technetwork/java/javase/downloads/>`_ or higher (JDK 6 may work as well)
* `Maven 3 <http://maven.apache.org/>`_ or higher
* `Microsoft Visual C++ 10 <http://www.microsoft.com/en-us/download/details.aspx?id=8279>`_ or higher

Note that if you build for a 32-bit Python, make sure to also install a 32-bit JDK. Accordingly, for a 64-bit Python,
you will need a 64-bit JDK. If you use the free Microsoft Visual C++ Express edition, then you only can build for
a 32-bit Python.

Open the command-line and execute::

    SET VS90COMNTOOLS=%VS100COMNTOOLS%
    SET JDK_HOME=<path to the JDK installation directory>
    SET JAVA_HOME=%JDK_HOME%
    SET PATH=%JDK_HOME%\jre\bin\server;%PATH%

Then, to actually build and test the jpy Python module use the following command::

    python setup.py --maven build


Python 3.3 and higher
---------------------

You will need

* `Python 3.3 <http://www.python.org/>`_ or higher (3.2 may work as well but is not tested)
* `Oracle JDK 7 <http://www.oracle.com/technetwork/java/javase/downloads/>`_ or higher (JDK 6 may work as well)
* `Maven 3 <http://maven.apache.org/>`_ or higher
* `Microsoft Windows SDK 7.1 <http://www.microsoft.com/en-us/download/details.aspx?id=8279>`_ or higher

If you build for a 32-bit Python, make sure to also install a 32-bit JDK. Accordingly, for a 64-bit Python, you will
need a 64-bit JDK.

The Python setup tools (``distutils``) can make use of the command-line C/C++ compilers of the free Microsoft Windows SDK.
These will by used by ``distutils`` if the ``DISTUTILS_USE_SDK`` environment variable is set. The compilers are made accessible via
the command-line by using the ``setenv`` tool of the Windows SDK. In order to install the Windows SDK do the following

1. If you already use Microsoft Visual C++ 2010, make sure to uninstall the x86 and amd64 compiler redistributables first. Otherwise the installation of the Windows SDK will definitely fail. This may also apply to higher versions of Visual C++.
2. Download and install `Windows SDK 7.1 <http://www.microsoft.com/en-us/download/details.aspx?id=8279>`_. (This step failed for me the first time. A second 'repair' install was successful.)
3. Download and install `Windows SDK 7.1 SP1 <http://www.microsoft.com/en-us/download/details.aspx?id=4422>`_.

Open the command-line and execute::

    "C:\Program Files\Microsoft SDKs\Windows\v7.1\bin\setenv" /x64 /release

to prepare a build of the 64-bit version of jpy. Use::

    "C:\Program Files\Microsoft SDKs\Windows\v7.1\bin\setenv" /x86 /release

to prepare a build of the 32-bit version of jpy. Now set other environment variables::

    SET DISTUTILS_USE_SDK=1
    SET JDK_HOME=<path to the JDK installation directory>
    SET JAVA_HOME=%JDK_HOME%
    SET PATH=%JDK_HOME%\jre\bin\server;%PATH%

Then, to actually build and test the jpy Python module use the following command::

    python setup.py --maven build




**********************
Typical Build Problems
**********************

=====================
Environment variables
=====================

Make sure that ``JAVA_HOME`` and ``JDK_HOME`` are always set, not only when installing, but also when using jpy. Additionally make sure that your ``PATH`` environment variable contains the ``JAVA_HOME``.

Set environment variables on `Windows <http://www.computerhope.com/issues/ch000549.htm>`_

Set environment variables on `Linux <http://unix.stackexchange.com/questions/117467/how-to-permanently-set-environmental-variables>`_

==============================================
Binary incompatibility between Python and Java
==============================================

When used from Python, jpy must be able to find an installed Java Virtual Machine (JVM) on your computer. This is
usually the one that has been linked to the Python module during the build process.

If the JVM cannot be found, you will have to adapt the ``LD_LIBRARY_PATH`` (Unix) or ``PATH`` (Windows) environment
variables to contain the path to the JVM shared libraries. That is ``libjvm.dylib`` (Darwin), ``libjvm.so`` (Linux) and
``jvm.dll`` (Windows). Make sure to use matching platform architectures, e.g. only use a 64-bit JVM for a 64-bit Python.

Otherwise the JVM may be found but you will get error similar to the following one (Windows in this case)::

    >>> import jpy
    Exception in thread "main" java.lang.UnsatisfiedLinkError: C:\Python33-amd64\Lib\site-packages\jpy.pyd: Can't load AMD 64-bit .dll on a IA 32-bit platform


======================================
Unable to find vcvarsall.bat (Windows)
======================================

If you build for Python 2.7, ``setup.py`` may fail with the following message::

    C:\Users\Norman\JavaProjects\jpy>c:\Python27-amd64\python.exe setup.py install
    Building a 64-bit library for a Windows system
    running install
    running build
    running build_ext
    building 'jpy' extension
    error: Unable to find vcvarsall.bat

This happens, because ``distutils`` uses an environment variable of an older Microsoft Visual C++ version,
namely ``VS90COMNTOOLS``. Make sure to it to the value of your current version. For example::

    SET VS90COMNTOOLS=%VS100COMNTOOLS%


=========================
DLL load failed (Windows)
=========================

``setup.py`` may fail with the following message::

    C:\Users\Norman\JavaProjects\jpy>c:\Python27\python.exe setup.py install
    Building a 32-bit library for a Windows system
    running install
    running build
    running build_ext
    ...
    running install_lib
    running install_egg_info
    Removing c:\Python27\Lib\site-packages\jpy-0.7.2-py2.7.egg-info
    Writing c:\Python27\Lib\site-packages\jpy-0.7.2-py2.7.egg-info
    Importing module 'jpy' in order to retrieve its shared library location...
    Traceback (most recent call last):
      File "setup.py", line 133, in <module>
        import jpy
    ImportError: DLL load failed: %1 is not a valid Win32 application

Fix this by adding the path to the Java VM shared library (``jvm.dll``) to the ``PATH`` environment variable::

    SET PATH=%JDK_HOME%\jre\bin\server;%PATH%

