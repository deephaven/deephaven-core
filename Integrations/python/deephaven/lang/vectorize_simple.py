
"""
Utilities for vectorization.

**The contents of this module are intended only for internal Deephaven use and may change at any time.**
"""

import ast
import traceback
from collections import OrderedDict
import collections
from io import UnsupportedOperation

import numba as nb
import numba.types
import numba.typing
import numpy
from numba.npyufunc import vectorize
from numpy.core.multiarray import ndarray

from deephaven.lang.analyzer import Analyzer

numpy_primitives = {"float64",
                    "float32",
                    "int64",
                    "int32",
                    "int16",
                    "uint16",
                    "int8",
                    "bool"}


def new_vec_function(src, cols=None, refs=None):
    """
    Creates a new vectorized function.
    :return: new function
    """
    invoker, used_cols = compile_function(src, cols)
    return invoker.fun

def export_locals(glob,loc):
    """
    Combines two dictionaries.
    :return: combined dictionary.
    """
    glob = glob.copy()
    glob.update(loc)
    return glob

def arguments_for(name, type):
    """
    Returns vectorized arguments for a column name and type.
    :param name: column name
    :param type: column type
    :return: argument types, arguments, and unpacks
    """
    dimensions_count = 0
    unpack = "{0} = __arg0_{0}__[__iterator_i__]".format(name)
    if "[" in type:
        tail = type[type.find("[") + 1:-1]
        argument_types = [type[:type.find("[")] + "[:]"]
        if argument_types=="unicode_type":
            raise TypeError("Cannot support arrays of String type")
        dimensions_count = tail.count(":")
    elif type == "unicode_type":
        dimensions_count = 1
        argument_types = [type]
    else:
        argument_types = [type + "[:]"]



    if dimensions_count > 0:
        unpack = "{0} = __arg0_{0}__[__arg1_{0}__[__iterator_i__-1] if __iterator_i__ > 0 else 0:__arg1_{0}__[__iterator_i__]]".format(
            name)
        if (dimensions_count > 1):
            raise UnsupportedOperation("Multidimensional arrays are not supported yet")
            '''
            shape_args = ["__arg{}_{}__,".format(i, name) for i in range(2, dimensions_count + 1)]
            unpack = unpack + ".reshape([{1},len{0}:__arg1_{0}__[__iterator_i__]/({2})])".format(name,
                                                                                                 ",".join(shape_args),
                                                                                                 "*".join(shape_args))
            '''
    arguments = []
    for i in range(0, dimensions_count + 1):
        arguments.append("__arg{}_{}__".format(i, name))
    for i in range(0, dimensions_count):
        argument_types.append("int32[:]")
    return argument_types, arguments, unpack


def vectorized_arguments(used_cols):
    """
    Returns vectorized arguments for used columns.
    :param used_cols: used columns
    :return: argument types, arguments, and unpacks
    """
    argument_types = []
    arguments = []
    unpacks = []
    for name, type in used_cols.items():
        arg_types, args, unpack = arguments_for(name, type)
        arguments.extend(args)
        unpacks.append(unpack)
        argument_types.extend(arg_types)
    return argument_types, arguments, unpacks


def compile_function(src, cols=None, globs=None, loc=None):
    """Compiles a function."""
    # extract only the referenced globals

    colsDict = dict(cols)
    cols = colsDict.items()
    # debug: print("\n\nSource:\n{}\n\n".format(src))
    # parse user's source so we can identify used columns and references
    node = ast.parse("{}\n".format(src), '<string>', 'exec')
    analyze = Analyzer(colsDict, None)
    # visit the ast to determine which columns and references are actually used
    analyze.visit(node)

    global fixed
    # Doing plain string manipulation, we need to add indent to the user-supplied source.
    expression = ''.join('    {}'.format(line) for line in src.splitlines(True))

    all_args = []
    used_cols = [name for name, type in cols if name in analyze.used_cols]
    all_args.extend(used_cols)

    fixed = (
        "def __tmp__({}):\n"
        "    return {}\n").format(",".join(all_args), expression)
    # exec the function-defining code above, saving it into `loc`
    if (loc == None):
        loc = {}
    exec (fixed, globs, loc)

    compiled = loc['__tmp__']

    # apply @vectorize to the result
    types_for_used_columns = [type for name, type in cols if name in analyze.used_cols]
    is_vectorize_friendly = not any(type == "unicode_type" or "[" in type for type in types_for_used_columns)

    expression = ''.join('        {}'.format(line) for line in src.splitlines(True))
    used_cols_name_to_type = OrderedDict([(name, type) for name, type in cols if name in analyze.used_cols])
    return_type = numba.jit(["(" + ",".join(types_for_used_columns) + ",)"if len(
        analyze.used_cols) > 0 else ()], nopython=True)(compiled).nopython_signatures[0].return_type
    if isinstance(return_type, numba.types.scalars.Literal):
        return {'result': return_type.literal_value, 'columns': used_cols,
                'return_type': return_type.literal_type.name}

    if isinstance(return_type, numba.types.IterableType):
        is_vectorize_friendly = False

    if (is_vectorize_friendly):
        vectorized_function = vectorize(["(" + ",".join(types_for_used_columns) + ",)" if len(
            analyze.used_cols) > 0 else ()])(compiled)
        # debug: print("Signature ")
        # debug: print(list(result._dispatcher.overloads))
        return_type = [sig.return_type for sig in vectorized_function._dispatcher.overloads.keys()][0]

        if isinstance(return_type, numba.types.scalars.Literal):
            return {'result': return_type.literal_value, 'columns': used_cols,
                    'return_type': return_type.literal_type.name}

        if (len(analyze.used_cols) == 0):
            fixed = (
                "def __tmp__(__count__):\n"
                "    __result__ = numpy.empty(__count__,dtype=numpy.dtype('{}'))\n"
                "    for __i__ in range(__count__):\n"
                "        __result__[__i__] = {}\n"
                "    return __result__\n"
            ).format(return_type, src)

            globs["numpy"] = numpy
            exec (fixed, globs, loc)

            compiled = loc['__tmp__']
            vectorized_function = numba.jit(
                numba.typing.signature(nb.types.Array(return_type, 1, 'C'), (numba.types.int32)))(compiled)
    else:
        argument_types, arguments, unpack = vectorized_arguments(used_cols_name_to_type)
        is_primitive = return_type.name in numpy_primitives
        numpy_return_type_name = return_type.name
        if numpy_return_type_name == "bool":
            numpy_return_type_name = "bool_"
        element_add = "__dest__[__iterator_i__] =  {}" if is_primitive else "__dest__.append({})"
        result_init = "__dest__ = {}\n".format("numpy.empty(__arg_count__,numpy.{})".format(numpy_return_type_name) if is_primitive else "[]")
        manually_vectorize_code = "def __tmp__(__arg_count__,{}):\n" \
                                  "    {}\n" \
                                  "    for __iterator_i__ in range(0,__arg_count__):\n" \
                                  "        {}\n" \
                                  "        {}\n" \
                                  "    return __dest__".format(",".join(arguments), result_init, "\n        ".join(unpack), element_add.format(expression))
        #debug print(manually_vectorize_code)
        exec (manually_vectorize_code, globs, loc)
        manually_vectorize = loc['__tmp__']
        # debug print(argument_types)
        argsSig, rt = numba.sigutils.normalize_signature("(int32," + ",".join(argument_types) + ",)" if len(
            argument_types) > 0 else "(int32,)")
        for i in range(0, len(argsSig)):
            if isinstance(argsSig[i], numba.types.Array):
                argsSig[i].mutable = False
        vectorized_function = numba.jit([argsSig], nopython=True)(
            manually_vectorize)

    # wrap the returned function, so any expected errors come with a meaningful error message.
    def invoker(args):
        try:
            # debug: print("args:")
            # debug: print(len(args))
            # debug: print(list(args))
            args = list(args)
            # debug: print("done args")
            #print(vectorized_function.nopython_signatures[0])
            return vectorized_function.__call__(*args[1:]) if is_vectorize_friendly and len(args) > 1 else vectorized_function.__call__(*args)
        except TypeError:
            traceback.print_exc()
            # determine the types of referenced variables to help user identify their problem.
            types = '\n'.join("ndarray[{}]".format(ar.dtype.name) if
                              hasattr(ar, 'dtype') and isinstance(ar, ndarray)
                              else '{}({})'.format(ar.dtype.name, ar) if
            hasattr(ar, 'dtype')
            else "<foo>"
                              for ar in args)
            raise TypeError(("Vectorized functions may only reference primitives and other vectorized functions.\n"
                             "Argument types:\n"
                             "{}\n"
                             "Source:\n"
                             "{}").format(types, fixed))

    return {'fun': invoker, 'columns': used_cols, 'return_type': return_type.name}
