"""
The analyzer is used for inspecting the user's source, detecting column and variable references
as well as inferring return types using numba's internal infrastructure.

**The contents of this module are intended only for internal Deephaven use and may change at any time.**
"""

import numba
import numpy
from _ast import Load, Store, Name, Return, Module, Num, Expr
from ast import NodeVisitor, fix_missing_locations, dump
from collections import OrderedDict
from numpy.core.multiarray import dtype

from deephaven.lang.ListBasedSet import ListBasedSet
from deephaven.lang.constants import debugMode, timingMode, default_globals
from deephaven.lang.tools import new_func, assign, read, method_call, field_read, merge_two_dicts, to_numba_type, \
    arg_or_name, values


# The methods we are overriding have non-standard uppercase in their names.
# noinspection PyPep8Naming
class Analyzer(NodeVisitor):
    """Analyze the parsed source code from the user.

    This operation is O(n) on the input source, which should usually be quite small.
    If you do not specify an explicit return type, we will use numba type inference to guess it.
    """

    # TODO: flesh this out with all types that we are willing to support.
    # Note that we'll use 1 instead of 0 for default values to at least make divide by zero much less probable.
    # The contents of this map determine what kind of ad-hoc variables we create when seeding numba type-inferer.
    # We assign default values of the desired types to the names of variables used by code-to-compile,
    # Then run the inferencing pipeline to get the "what-numba-will-expect" type, so we can generate an optimal impl.
    type_map = {

        # numba types
        numba.bool_: lambda: method_call(field_read('np', 'bool_'), [Num(n=1)]),
        numba.int_: lambda: method_call(field_read('np', 'int_'), [Num(n=1)]),
        numba.float_: lambda: method_call(field_read('np', 'float_'), [Num(n=1)]),
        numba.int8: lambda: method_call(field_read('np', 'int8'), [Num(n=1)]),
        numba.int16: lambda: method_call(field_read('np', 'int16'), [Num(n=1)]),
        numba.int32: lambda: method_call(field_read('np', 'int32'), [Num(n=1)]),
        numba.int64: lambda: method_call(field_read('np', 'int64'), [Num(n=1)]),
        numba.uint8: lambda: method_call(field_read('np', 'uint8'), [Num(n=1)]),
        numba.uint16: lambda: method_call(field_read('np', 'uint16'), [Num(n=1)]),
        numba.uint32: lambda: method_call(field_read('np', 'uint32'), [Num(n=1)]),
        numba.uint64: lambda: method_call(field_read('np', 'uint64'), [Num(n=1)]),
        numba.float32: lambda: method_call(field_read('np', 'float32'), [Num(n=1.0)]),
        numba.float64: lambda: method_call(field_read('np', 'float64'), [Num(n=1.0)]),

        # explicitly not adding the 128bit variants: https://github.com/numpy/numpy/issues/9992
        # only complex128 is supported, since complex is 64/128 instead of 32/64
        numba.complex64: lambda: method_call(field_read('np', 'complex64'), [Num(n=1.0)]),
        numba.complex128: lambda: method_call(field_read('np', 'complex128'), [Num(n=1.0)]),

        # numpy types
        numpy.bool_: lambda: method_call(field_read('np', 'bool_'), [Num(n=1)]),
        numpy.int_: lambda: method_call(field_read('np', 'int_'), [Num(n=1)]),
        numpy.float_: lambda: method_call(field_read('np', 'float_'), [Num(n=1)]),
        numpy.int8: lambda: method_call(field_read('np', 'int8'), [Num(n=1)]),
        numpy.int16: lambda: method_call(field_read('np', 'int16'), [Num(n=1)]),
        numpy.int32: lambda: method_call(field_read('np', 'int32'), [Num(n=1)]),
        numpy.int64: lambda: method_call(field_read('np', 'int64'), [Num(n=1)]),
        numpy.uint8: lambda: method_call(field_read('np', 'uint8'), [Num(n=1)]),
        numpy.uint16: lambda: method_call(field_read('np', 'uint16'), [Num(n=1)]),
        numpy.uint32: lambda: method_call(field_read('np', 'uint32'), [Num(n=1)]),
        numpy.uint64: lambda: method_call(field_read('np', 'uint64'), [Num(n=1)]),
        numpy.float32: lambda: method_call(field_read('np', 'float32'), [Num(n=1.0)]),
        numpy.float64: lambda: method_call(field_read('np', 'float64'), [Num(n=1.0)]),
        numpy.complex64: lambda: method_call(field_read('np', 'complex64'), [Num(n=1.0)]),
        numpy.complex128: lambda: method_call(field_read('np', 'complex128'), [Num(n=1.0)]),

        numpy.object_: lambda: method_call(field_read('np', 'complex128'), [Num(n=1.0)]),
    }  #: The contents of this map determine what kind of ad-hoc variables we create when seeding numba type-inferer.

    def __init__(self, cols, refs):
        """
        Creates an analyzer suitable for visiting expressions to find references to columns and variables.

        :param cols: A mapping from column names to numba (or numpy) types. np.int32, nb.float64, etc.
                     While it may be possible to pass a scalar value and extract the type,
                     this is silly in the context of columns,
                     as you have the column definition before you have column values.
        :param refs: A mapping of variable names to values.  Sending your global() scope can make sense,
                     though you are encouraged to create a minimal map containing on the values you need.
                     This dict is used only to determine whether a given ref is used or not;
                     if you construct manually, you should supply that dict as glob=my_refs in vectorize.new_vec_function
        """
        self.ignore_refs = frozenset([
            'np', 'numpy', 'nb', 'numba', 'None', 'True', 'False'
        ])
        self.all_names = OrderedDict()
        self.lenient = True
        self.used_cols = ListBasedSet()
        self.used_refs = ListBasedSet()
        self.processed = ListBasedSet()
        self.all_references = []
        self.array_reads = []
        self.array_writes = []
        self.all_assign = []
        self.last_assign = None
        self.in_function = False
        self.empty = not cols and not refs
        self.cols = {} if cols is None else cols
        self.refs = merge_two_dicts(globals(), default_globals()) if refs is None else refs
        self.last_statement = None
        self.performed_inspect = False

    def visit_Name(self, node):
        """This represents any Name ast node"""
        # TODO: reenable the assertion below through strenuous testing
        # assert isinstance(node.ctx, Load), "Only Load references are allowed; you sent {}".format(type(node.ctx))
        if isinstance(node.ctx, Load):
            if (node.id in self.ignore_refs):
                return
            if (debugMode):
                print('Found name %s' % dump(node))
            self.all_references.append(node)
            self.all_names[node.id] = node
            self._add_ref(node.id)

    def visit_Subscript(self, node):
        """This represent an array access"""
        if (isinstance(node.value, Name)):
            if (isinstance(node.value.ctx, Load)):
                self.array_reads.append(node)
                self.visit_Name(node.value)
            elif (isinstance(node.value.ctx, Store)):
                self.array_writes.append(node)
            self._add_ref(node.value.id)
        else: self.generic_visit(node)

    def visit_FunctionDef(self, node):
        """We will ignore all function defs, as we don't want to visit their bodies (or the `Name`s in the arg list)"""
        # print("Ignoring function {}".format(dump(node)))
        pass

    def generic_visit(self, node):
        """Called on every visit; we override mostly to set a breakpoint or add extra logging"""
        # here for debugging; set a breakpoint if you want to inspect each node
        # print('Generic %s' % dump(node))
        super(Analyzer, self).generic_visit(node)

    def visit_Assign(self, node):
        """If you use any assignments, we expect the last assignment to be the 'final output to return'"""
        # presence of an assignment is important; assignment may be required/optional/illegal depending on context.
        # an assign as a final statement will be treated as the output of the whole expression
        self.all_assign.append(node)
        self.last_assign = node
        self.generic_visit(node.value)

    def visit_Statement(self, node):
        """Visit a statement."""
        self.last_statement = node
        self.generic_visit(node)

    def _add_ref(self, ident):
        (self.used_cols if ident in self.cols else self.used_refs).add(ident)


