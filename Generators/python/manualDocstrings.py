import os
import sys


allManualDocs = {}

########################################################################################################################
# TableTools custom methods population

pathName = "io.deephaven.engine.util.TableTools"
className = pathName
dd = {
    "className": className,
    "methods": {},
    "path": pathName,
    "text": "Tools for users to manipulate tables.",
    "typeName": "class"
}
methodD = dd['methods']
allManualDocs[pathName] = dd

methodD['charCol'] = """Creates a new ColumnHolder of type `char` that can be used when creating in-memory tables.

:param name: name for the column
:param data: variable argument for the data
:return: (io.deephaven.engine.table.impl.util.ColumnHolder) a Deephaven ColumnHolder object

`data` structure:
  * an int
  * a string - will be interpreted as list of characters
  * a :class:`numpy.ndarray` of integer or one-character string type
  * a :class:`pandas.Series` whose values are a numpy array described above
  * an iterable of integers or strings - if string, only the first character will be used"""

methodD['byteCol'] = """Creates a new ColumnHolder of type `byte` that can be used when creating in-memory tables.

:param name: name for the column
:param data: variable argument for the data
:return: (io.deephaven.engine.table.impl.util.ColumnHolder) a Deephaven ColumnHolder object

data structure:
  * an int or list of ints
  * a :class:`numpy.ndarray` of integer or floating point values. `NaN` values will be mapped to `NULL_BYTE`
    constant values, and all other values simply cast.
  * a :class:`pandas.Series` whose values are a numpy array described above"""

methodD['shortCol'] = """Creates a new ColumnHolder of type `short` that can be used when creating in-memory tables.
    
:param name: name for the column
:param data: variable argument for the data
:return: (io.deephaven.engine.table.impl.util.ColumnHolder) a Deephaven ColumnHolder object

data structure:
  * an int or list of ints
  * a :class:`numpy.ndarray` of integer or floating point values. `NaN` values will be mapped to `NULL_SHORT`
    constant values, and all other values simply cast.
  * a :class:`pandas.Series` whose values are a numpy array described above"""

methodD['intCol'] = """Creates a new ColumnHolder of type `int` that can be used when creating in-memory tables.

:param name: name for the column
:param data: variable argument for the data
:return: (io.deephaven.engine.table.impl.util.ColumnHolder) a Deephaven ColumnHolder object

data structure:
  * an int or list of ints
  * a :class:`numpy.ndarray` of integer or floating point values. `NaN` values will be mapped to `NULL_INT`
    constant values, and all other values simply cast.
  * a :class:`pandas.Series` whose values are a numpy array described above"""

methodD['longCol'] = """Creates a new ColumnHolder of type `long` that can be used when creating in-memory tables.

:param name: name for the column
:param data: variable argument for the data
:return: (io.deephaven.engine.table.impl.util.ColumnHolder) a Deephaven ColumnHolder object

data structure:
  * an int or list of ints
  * a :class:`numpy.ndarray` of integer or floating point values. `NaN` values will be mapped to `NULL_LONG`
    constant values, and all other values simply cast.
  * a :class:`pandas.Series` whose values are a numpy array described above"""

methodD['floatCol'] = """Creates a new ColumnHolder of type `float` that can be used when creating in-memory tables.

:param name: name for the column
:param data: variable argument for the data
:return: (io.deephaven.engine.table.impl.util.ColumnHolder) a Deephaven ColumnHolder object

data structure:
  * a int or float or list of ints or floats
  * a :class:`numpy.ndarray` of integer or floating point values. `NaN` values will be mapped to `NULL_FLOAT`
    constant values, and all other values simply cast.
  * a :class:`pandas.Series` whose values are a numpy array described above"""

methodD['doubleCol'] = """Creates a new ColumnHolder of type `double` that can be used when creating in-memory tables.

:param name: name for the column
:param data: variable argument for the data
:return: (io.deephaven.engine.table.impl.util.ColumnHolder) a Deephaven ColumnHolder object

data structure:
  * an int or float or list of ints or floats
  * a :class:`numpy.ndarray` of integer or floating point values. `NaN` values will be mapped to `NULL_DOUBLE`
     constant values, and all other values simply cast.
  * a :class:`pandas.Series` whose values are a numpy array described above"""

methodD['col'] = """Returns a ColumnHolder that can be used when creating in-memory tables.

:param name: name for the column
:param data: variable argument for the data
:return: (io.deephaven.engine.table.impl.util.ColumnHolder) a Deephaven ColumnHolder object

data structure:
  * an int, bool, float, datetime, date, string or iterable of (one) such
  * :class:`numpy.ndarray` containing boolean, numerical, datetime64, object, or string data (type inferred)
  * :class:`pandas.Series` object whose values are such a numpy array"""

methodD['colSource'] = """Creates a column of appropriate type, used for creating in-memory tables.
    
:param data: variable argument for the data
:return: (io.deephaven.engine.table.impl.sources.ColumnSource<T>) a Deephaven ColumnSource of inferred type

data structure:
  * a java object, or list of java objects
  * an int, bool, float, datetime, date, string or iterable of (one) such
  * :class:`pandas.Series` object whose values are such a numpy array"""

methodD['objColSource'] = """Creates a column of appropriate object type, used for creating in-memory tables.

:param data: variable argument for the data
:return: (io.deephaven.engine.table.impl.sources.ColumnSource) a Deephaven ColumnSource of inferred type
data structure:
    * a java object, or list of java objects
    * an int, bool, float, datetime, date, string or iterable of (one) such
* :class:`numpy.ndarray` containing boolean, numerical, datetime64, object, or string data (type inferred)
* :class:`pandas.Series` object whose values are such a numpy array"""

# End TableTools custom methods file creation
########################################################################################################################


if __name__ == '__main__':
    # NOTE: this will fail (currently) unless the working directory is this location
    from docGenUtil import populateCurrentDocs, classDocGeneration, finalize

    # NOTE: weak arg parsing here, do we need more?
    if len(sys.argv) < 2:
        raise ValueError("The script requires at least one argument: devroot")

    if sys.argv[1].lower() in ['-h', '--help']:
        print("Called as:\n"
              "  python manualDocstrings.py <devroot> <assertNoChange>[False]\n"
              "\n"
              "    - <devroot> specifies the development root, below which we expect directories\n"
              "        `build/docs/javadoc` and `Integrations/python/deephaven/doc`\n"
              "    - <assertNoChange> [default `False`] optional argument.\n"
              "        * False indicates to extract the javadocs to .json format below\n"
              "           `Integrations/python/deephaven/doc`\n"
              "        * True indicates to check that the .json files in the file system below\n"
              "           `Integrations/python/deephaven/doc` match what WOULD be generated.\n"
              "           **NO ACTUAL GENERATION HERE**")

    # Parse the arguments
    devRoot = sys.argv[1]
    outDir = os.path.join(devRoot, 'out', 'docCustom')

    assertNoChange = False
    if len(sys.argv) > 2:
        assert_t = sys.argv[2].lower()
        if assert_t in ['true', 't', '1']:
            assertNoChange = True

    # walk the contents of outDir, and figure the current list of javadoc extracts
    currentDocs = populateCurrentDocs(outDir)

    for pathName in allManualDocs:
        details = allManualDocs[pathName]
        # finalize the generation task for this class
        classDocGeneration(currentDocs, assertNoChange, details, outDir)

    # validate, if necessary
    finalize(currentDocs, assertNoChange, '\nTo resolve failure, run the task "./gradlew :Generators:generateManPyDoc" '
                                          'to regenerate, and then commit the generated changes.\n'
                                          'To diagnose trouble, run the generation task followed by \"git diff\" to see the changes.\n'
                                          'To diagnose possible indeterminism in the generation process, regenerate the code and check '
                                          'the diff **multiple times**.')
