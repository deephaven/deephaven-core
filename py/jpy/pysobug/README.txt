
#############################
Python shared library problem
#############################

Problem: Native Python C-extensions can't find Python symbols if Python interpreter is embedded into a shared library.
This problem does not occur on Windows systems.

* mypydl.c - Source for a shared libary (mypydl.so) that embeds a Python interpreter
* mypy.c - Source for a main program (mypy) that dynamically loads the shared library mypydl.so
* mypymod.c - Native Python extension module (mypymod) that uses the Python C-API

make.sh - builds mypydl.so, mypy and the mypymod Python extension module.

The main program mypy interprets all given arguments as Python code and executes it::

    $ bash ./make.sh
    $ ./mypy "print('Hello')"
    mypy: executing [print('Hello')]
    Hello
    mypy: status 0

Here is how to reproduce the problem::

    $ ./mypy "import mypymod"
    mypy: executing [import mypymod]
    Traceback (most recent call last):
      File "<string>", line 1, in <module>
    ImportError: /home/norman/.local/lib/python3.4/site-packages/mypymod.cpython-34m.so: undefined symbol: Py_BuildValue
    ...
    mypy: status -1

Or with numpy::

    $ ./mypy "import numpy"
    mypy: executing [import numpy]
    ...
    ImportError: /home/norman/.local/lib/python3.4/site-packages/numpy/core/multiarray.cpython-34m.so: undefined symbol: PyExc_SystemError
    mypy: status -1


If the Python shared library is explicitly pre-loaded, then these errors don't occur::

    $ LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libpython3.4m.so ./mypy "import mypymod"
    mypy: executing [import mypymod]
    PyInit_mypymod: enter
    PyInit_mypymod: exit: module=0x7f72d34a4188
    mypy: status 0

The error also doesn't occur, if I link my Python extension module explicitly against the Python shared library::

    extension = Extension('mypymod', sources=['mypymod.c'], libraries=['python3.4m'])
    setup(name='mypymod', ext_modules=[extension])

Understandably but unfortunately, most Python extensions don't declare the dependency to a specific Python version explicitly.