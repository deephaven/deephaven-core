# distutils: language = c++
# cython: language_level = 3
# cython: cpp_locals=True
import numpy as np
import numpy.typing as npt
from cdeephaven_client cimport CClient, CColumnSource, CGenericChunk, CRowSequence
from cdeephaven_client cimport CSubscriptionHandle, CTable, CTableHandle
from cdeephaven_client cimport CTableHandleManager, CTickingUpdate
from cdeephaven_client cimport CHumanReadableElementTypeName, CHumanReadableStaticTypeName
from cdeephaven_client cimport CCythonSupport
from cython.operator cimport dereference as deref
from inspect import signature
from libc.stdint cimport int8_t, int16_t, int32_t, int64_t
from libcpp cimport bool
from libcpp.memory cimport shared_ptr, unique_ptr
from libcpp.string cimport string
from libcpp.utility cimport move, pair
from libcpp.vector cimport vector
from typing import Dict, Sequence, Union

# Simple wrapper of the corresponding C++ Client class. The only job here is
# to make a connection to the database and for the caller to make a
# TableHandleManager.
cdef class Client:
    cdef CClient client

    @staticmethod
    def connect(target: str) -> Client:
        bytes = target.encode()

        result = Client()
        result.client = move(CClient.connect(bytes))
        return result

    def get_manager(self) -> TableHandleManager:
        result = TableHandleManager()
        result.manager = self.client.getManager()
        return result

# Simple wrapper of the corresponding C++ TableHandleManager class.
cdef class TableHandleManager:
    cdef CTableHandleManager manager

    def fetch_table(self, table_name: str) -> TableHandle:
        bytes = table_name.encode()
        table_handle = self.manager.fetchTable(bytes)
        return TableHandle.create(move(table_handle))

    def empty_table(self, numRows: size_t) -> TableHandle:
        table_handle = self.manager.emptyTable(numRows)
        return TableHandle.create(move(table_handle))

    def time_table(self, start: size_t, nanos: size_t) -> TableHandle:
        table_handle = self.manager.timeTable(start, nanos)
        return TableHandle.create(move(table_handle))

# A trampoline function. By the time we get to this point, the
# user-supplied Python callback is in a void*. We cast it back to a Python
# object and invoke its on_tick method.
cdef void _on_tick_callback(CTickingUpdate update, void *user_data) with gil:
    ptu = TickingUpdate.create(move(update))
    (<object>user_data).on_tick(ptu)

# A trampoline function. By the time we get to this point, the
# user-supplied Python callback is in a void*. We cast it back to a Python
# object and invoke its on_error method.
cdef void _on_error_callback(string error, void *userData) with gil:
    (<object>userData).on_error(error)

cdef _check_has_method(callback, methodName: str, expected_num_params: int):
    attr =  getattr(callback, methodName, None)
    if not callable(attr):
        raise RuntimeError(
            f"Provided callback object does not have an {methodName}() method")
    num_params = len(signature(attr).parameters)
    if expected_num_params != num_params:
        raise RuntimeError(
            f"Callback method {methodName}() needs to take {expected_num_params} parameters, but takes {num_params}")

# Wrapper of the corresponding C++ TableHandle class.
cdef class TableHandle:
    cdef CTableHandle table_handle

    @staticmethod
    cdef TableHandle create(CTableHandle table_handle):
        result = TableHandle()
        result.table_handle = table_handle
        return result
	
    cpdef update(self, column_specs: [str]):
        cdef vector[string] c_column_specs
        for item in column_specs:
            encoded = item.encode()
            c_column_specs.push_back(encoded)
        result = self.table_handle.update(move(c_column_specs))
        return TableHandle.create(move(result))

    def tail(self, size_t num_rows):
        result = self.table_handle.tail(num_rows)
        return TableHandle.create(move(result))

    def toString(self, want_headers: bool) -> str:
        result = self.table_handle.toString(want_headers)
        return result.decode()
    
    # 'callback' is a Python object that defines an on_tick and on_error method,
    # like so
    #
    # class MyCallback:
    #     def on_tick(self, update: dh.TickingUpdate):
    #         ...
    #     def on_error(self, message: str)
    #         ...
    def subscribe(self, callback) -> SubscriptionHandle:
        _check_has_method(callback, "on_tick", 1)
        _check_has_method(callback, "on_error", 1)
        
        c_handle = self.table_handle.subscribe(_on_tick_callback, <void*>callback,
            _on_error_callback, <void*>callback)
        # Take special care to hold our own a reference to 'callback' so it
        # doesn't deallocated before the C++ library is done using it
        inner = SubscriptionHandleImpl.create(self.table_handle, callback, c_handle)
        return SubscriptionHandle(inner)

    def unsubscribe(self, handle: SubscriptionHandle):
        handle.impl.shutdown()

# This is a normal Python class that exists solely to have a finalizer that
# calls shutdown() on the impl class that it owns.
class SubscriptionHandle:
    def __init__(self, impl):
        self.impl = impl

    def __del__(self):
        self.impl.shutdown()

# Objects of this class are typically owned by SubscriptionHandle
cdef class SubscriptionHandleImpl:
    # The C++ TableHandle. We need this so we can unsubscribe.
    cdef CTableHandle table_handle
    # The caller's callback. We hold a reference here so it is not destroyed
    # until such time as we unsubscribe.
    cdef object callback
    # The C++ subcriptionHandle. Needed as a parameter to unsubcribe.
    cdef shared_ptr[CSubscriptionHandle] subscription_handle

    @staticmethod
    cdef SubscriptionHandleImpl create(CTableHandle table_handle,
                                       object callback, shared_ptr[CSubscriptionHandle] subscription_handle):
        result = SubscriptionHandleImpl()
        result.table_handle = move(table_handle)
        result.callback = callback
        result.subscription_handle = move(subscription_handle)
        return result

    def shutdown(self):
        self.table_handle.unsubscribe(self.subscription_handle)

# Simple wrapper of the corresponding C++ TickingUpdate class.
cdef class TickingUpdate:
    cdef CTickingUpdate ticking_update

    @staticmethod
    cdef TickingUpdate create(CTickingUpdate update):
        result = TickingUpdate()
        result.ticking_update = move(update)
        return result

    @property
    def prev(self) -> Table:
        return Table.create(self.ticking_update.prev())

    @property
    def before_removes(self) -> Table:
        return Table.create(self.ticking_update.beforeRemoves())

    @property
    def removed_rows(self) -> RowSequence:
        return RowSequence.create(self.ticking_update.removedRows())

    @property
    def after_removes(self) -> Table:
        return Table.create(self.ticking_update.afterRemoves())

    @property
    def before_adds(self) -> Table:
        return Table.create(self.ticking_update.beforeAdds())

    @property
    def added_rows(self) -> RowSequence:
        return RowSequence.create(self.ticking_update.addedRows())

    @property
    def after_adds(self) -> Table:
        return Table.create(self.ticking_update.afterAdds())

    @property
    def before_modifies(self) -> Table:
        return Table.create(self.ticking_update.beforeModifies())

    @property
    def after_modifies(self) -> Table:
        return Table.create(self.ticking_update.afterModifies())

    @property
    def modified_rows(self) -> [RowSequence]:
        result = []
        mod_rows = self.ticking_update.modifiedRows()
        for i in range(mod_rows.size()):
            result.append(RowSequence.create(mod_rows[i]))  
        return result

    @property
    def all_modified_rows(self) -> RowSequence:
        rs = self.ticking_update.allModifiedRows()
        return RowSequence.create(rs)

    @property
    def current(self) -> Table:
        return Table.create(self.ticking_update.current())

# Simple wrapper of the corresponding C++ Table class.
cdef class Table:
    cdef shared_ptr[CTable] table

    @staticmethod
    cdef Table create(shared_ptr[CTable] table):
        result = Table()
        result.table = move(table)
        return result

    def get_column(self, columnIndex: size_t) -> ColumnSource:
        cs = deref(self.table).getColumn(columnIndex)
        return ColumnSource.create(move(cs))

    def get_column_by_name(self, name: str, strict: bool) -> ColumnSource | None:
        name_bytes = name.encode()
        result = deref(self.table).getColumn(name_bytes, strict)
        if result == NULL:
            return None
        return ColumnSource.create(move(result))

    @property
    def columns(self) -> [ColumnSource]:
        ncols = deref(self.table).numColumns()
        return [ColumnSource.create(deref(self.table).getColumn(i)) for i in range(ncols)]

    def get_column_index(self, name: unicode, strict: bool) -> size_t:
        name_as_string = <string>name.encode()
        return deref(self.table).getColumnIndex(name_as_string, strict)

    def get_row_sequence(self) -> RowSequence:
        result = deref(self.table).getRowSequence()
        return RowSequence.create(move(result))
        
    @property
    def num_rows(self) -> size_t:
        return deref(self.table).numRows()

    @property
    def num_columns(self) -> size_t:
        return deref(self.table).numColumns()

    @property
    def schema(self) -> Schema:
        return Schema.create(self.table)

    def to_string(self, want_headers: bool, want_row_numbers: bool, row_sequence = None) -> str:
        if row_sequence is None:
            result = deref(self.table).toString(want_headers, want_row_numbers)
        elif isinstance(row_sequence, list):
            raise RuntimeError("TODO")
        elif isinstance(row_sequence, RowSequence):
            result = deref(self.table).toString(want_headers, want_row_numbers, (<RowSequence>row_sequence).row_sequence)
        return result.decode()

# cdef object _determine_numpy_type(type: CythonSupport.ElementTypeId):
#     # Put this dict somewhere so we are not initializing it every time
#     type_map = {
#         CythonSupport.ElementTypeId.BOOL: np.bool_(),
#         CythonSupport.ElementTypeId.INT8: np.int8(),
#         CythonSupport.ElementTypeId.INT16: np.int16(),
#         CythonSupport.ElementTypeId.INT32: np.int32(),
#         CythonSupport.ElementTypeId.INT64: np.int64(),
#         CythonSupport.ElementTypeId.FLOAT: np.float32(),
#         CythonSupport.ElementTypeId.DOUBLE: np.float64(),
#     }
#
#     # TODO: deal with CHAR and STRING
#     result = type_map[type]
#     type_as_int = <int>type
#     if result is None:
#         raise RuntimeError(f"Couldn't find mapping for type code {type_as_int}")
#     return result

cdef _dh_type_to_np_type(dh_type: CCythonSupport.ElementTypeId):
    if dh_type == CCythonSupport.ElementTypeId.INT8:
        return np.int8()
    if dh_type == CCythonSupport.ElementTypeId.INT16:
        return np.int16()
    if dh_type == CCythonSupport.ElementTypeId.INT32:
        return np.int32()
    if dh_type == CCythonSupport.ElementTypeId.INT64:
        return np.int64()
    if dh_type == CCythonSupport.ElementTypeId.FLOAT:
        return np.float32()
    if dh_type == CCythonSupport.ElementTypeId.DOUBLE:
        return np.float64()
    if dh_type == CCythonSupport.ElementTypeId.BOOL:
        return np.bool()
    if dh_type == CCythonSupport.ElementTypeId.CHAR:
        return np.uint16()
    if dh_type == CCythonSupport.ElementTypeId.STRING:
        return None  # TODO
    if dh_type == CCythonSupport.ElementTypeId.TIMESTAMP:
        return None  # TODO
    return None

cdef class Schema:
    _names: Sequence[str]
    _numpy_types: Sequence[npt.DTypeLike]
    _numpy_dict: Dict[str, npt.DTypeLike]

    @staticmethod
    cdef Schema create(shared_ptr[CTable] table):
        c_names = CCythonSupport.getColumnNames(deref(table))
        c_types = CCythonSupport.getColumnTypes(deref(table))
        names: [str] = []
        types: [npt.DTypeLike] = []
        dict: Dict[str, npt.DTypeLike] = {}
        cdef size_t i
        for i in range(c_names.size()):
            name = c_names[i].decode()
            type = c_types[i]

            names.append(name)
            np_type = _dh_type_to_np_type(type)
            if np_type is not None:
                dict[name] = np_type

        result = Schema()
        result._names = names
        result._numpy_types = types
        result._numpy_dict = dict
        return result

    def get_numpy_type_by_name(self, name: str, strict: bool):
        if strict:
            return self._numpy_dict[name]
        else:
            return self._numpy_dict.get(name)

    @property
    def names(self) -> Sequence[str]:
        return self._names

    @property
    def numpy_types(self) -> Sequence[npt.DTypeLike]:
        return self._numpy_types

    @property
    def dict(self):
        return self._numpy_dict


# Simple wrapper of the corresponding C++ RowSequence class.
cdef class RowSequence:
    cdef shared_ptr[CRowSequence] row_sequence

    @staticmethod
    cdef RowSequence create(shared_ptr[CRowSequence] row_sequence):
        result = RowSequence()
        result.row_sequence = move(row_sequence)
        return result

    def take(self, size: size_t) -> RowSequence:
        row_sequence = deref(self.row_sequence).take(size)
        return RowSequence.create(move(row_sequence))

    def drop(self, size: size_t) -> RowSequence:
        row_sequence = deref(self.row_sequence).drop(size)
        return RowSequence.create(move(row_sequence))

    @property
    def size(self) -> size_t:
        return deref(self.row_sequence).size()
	
    @property
    def empty(self) -> bool:
        return deref(self.row_sequence).empty()

# A Cython fused type indicating all the valid element types for ColumnSource. For the C++
# "string" type we use object. The C++ DateTime type is not yet implemented.
ctypedef fused column_source_element_type_t:
    bool
    int8_t
    int16_t
    int32_t
    int64_t
    float
    double
    object

# A Cython fused type indicating all the valid *primitive* element types for
# ColumnSource, i.e. excluding object.
ctypedef fused column_source_primitive_element_type_t:
    bool
    int8_t
    int16_t
    int32_t
    int64_t
    float
    double

# Confirm that the element type of dest_data is the same as the element type of columnSource.
# Technically we don't need to do this check, as the C++ library has its own error checking,
# but this gives us an opportunity to provide a more friendly error message here.
cdef _check_compatibility(const CColumnSource &column_source,
    column_source_element_type_t[::1] dest_data):
    cdef const char *cs_name = CHumanReadableElementTypeName.getName(column_source)
    cdef const char *dd_name_for_comparison = NULL

    if column_source_element_type_t is object:
        # "object" is a special case. If the user passes us an array of objects, then
        # the user expects the column source to be of type string. So the name for comparison
        # is "string" but (if they don't match) the name we put in the error message will be
        # "object".
        dd_name_for_comparison = CHumanReadableStaticTypeName[string].getName()
        dd_name_for_rendering = "object"
    else:
        dd_name_for_comparison = CHumanReadableStaticTypeName[column_source_element_type_t].getName()
        dd_name_for_rendering = dd_name_for_comparison.decode()

    if cs_name != dd_name_for_comparison:
        cs_name_for_rendering = cs_name.decode()
        raise RuntimeError(f"column_source has element type {cs_name_for_rendering} but caller buffer has element type {dd_name_for_rendering}")

# Wrapper of the corresponding C++ ColumnSource class. Does a little bit of
# extra work to support the fill_chunk method.
cdef class ColumnSource:
    cdef shared_ptr[CColumnSource] column_source

    @staticmethod
    cdef ColumnSource create(shared_ptr[CColumnSource] column_source):
        result = ColumnSource()
        result.column_source = move(column_source)
        return result

    # The fill_chunk method uses a Cython "fused type" which is sort of like a C++ template with
    # explicit template instantiation. 'dest_data' is a memory view of some caller data structure
    # (perhaps a NumPy array or a compact array created by the Python array module) whose element
    # type is one of the types defined by column_source_element_type_t: int8_t, int16_t, etc.
    cpdef fill_chunk(self, rows: RowSequence,
        column_source_element_type_t[::1] dest_data,
        bool[::1] optionalDestNullFlags):

        _check_compatibility(deref(self.column_source), dest_data)
        
        rs_size = rows.size
        dest_size = dest_data.shape[0]

        if (dest_size < rs_size):
            raise RuntimeError(f"size of dest_data {dest_size} < row sequence size {rs_size}")

        if optionalDestNullFlags is not None:
            boolSize = optionalDestNullFlags.shape[0]
            if (boolSize < rs_size):
                raise RuntimeError(
                    f"size of optionalDestNullFlags {boolSize} < row sequence size {rs_size}")

        # get a pointer to the dest null flags, if the parameter is not None
        cdef CGenericChunk[bool] optionalBooleanChunk
        cdef CGenericChunk[bool] *null_flags_ptr = NULL
        if optionalDestNullFlags is not None:
            optionalBooleanChunk = CGenericChunk[bool].createView(&optionalDestNullFlags[0], rs_size)
            null_flags_ptr = &optionalBooleanChunk

        if column_source_element_type_t is object:
            self._fill_string_chunk(rows, dest_data, null_flags_ptr)
        else:
            self._fill_other_chunk(rows, dest_data, null_flags_ptr)

    # Helper method for fill_chunk when the dest_data element type is "object" (and the ColumnSource
    # is a string type). In this case we have to allocate a temporary chunk of strings, fill it, and
    # then copy the data out of it to Python strings.
    cdef _fill_string_chunk(self, rows: RowSequence, object[::1] dest_data,
        CGenericChunk[bool] *null_flags_ptr): 
        rsSize = rows.size
        dest_chunk = CGenericChunk[string].create(rsSize)
        deref(self.column_source).fillChunk(deref(rows.row_sequence), &dest_chunk, null_flags_ptr)
        i: ssize_t = 0
        for i in range(rsSize):
            dest_data[i] = dest_chunk.data()[i]

    # Helper method for fill_chunk when the dest_data element type is any type except "object".
    # In this case we point a Chunk directly to the dest_data buffer, and then the C++ library
    # fills that chunk directly.
    cdef _fill_other_chunk(self, rows: RowSequence,
        column_source_primitive_element_type_t[::1] dest_data,
        CGenericChunk[bool] *null_flags_ptr):
        rsSize = rows.size
        data_chunk = CGenericChunk[column_source_primitive_element_type_t].createView(
	        &dest_data[0], rsSize)
        deref(self.column_source).fillChunk(deref(rows.row_sequence), &data_chunk, null_flags_ptr)
