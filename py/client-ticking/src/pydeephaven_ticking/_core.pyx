# distutils: language = c++
# cython: language_level = 3

#
# Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#
import numpy as np
import numpy.typing as npt
import pyarrow as pa
from pydeephaven_ticking._core cimport assign_shared_ptr
from cython.operator cimport dereference as deref
from pydeephaven_ticking._core cimport char16_t
from pydeephaven_ticking._core cimport CColumnSource, CGenericChunk, CRowSequence, CSchema, CClientTable
from pydeephaven_ticking._core cimport CHumanReadableElementTypeName, CHumanReadableStaticTypeName
from pydeephaven_ticking._core cimport CCythonSupport, ElementTypeId, CDateTime
from pydeephaven_ticking._core cimport CTickingUpdate, CBarrageProcessor, CNumericBufferColumnSource
from libc.stdint cimport int8_t, int16_t, int32_t, int64_t, intptr_t, uint8_t, uint16_t, uint32_t, uint64_t
from libcpp cimport bool
from libcpp.memory cimport shared_ptr, unique_ptr
from libcpp.string cimport string
from libcpp.utility cimport move, pair
from libcpp.vector cimport vector
from typing import Sequence, cast

# Simple wrapper of the corresponding C++ TickingUpdate class.
cdef class TickingUpdate:
    cdef CTickingUpdate ticking_update

    @staticmethod
    cdef TickingUpdate create(CTickingUpdate update):
        result = TickingUpdate()
        result.ticking_update = move(update)
        return result

    @property
    def prev(self) -> ClientTable:
        return ClientTable.create(self.ticking_update.Prev())

    @property
    def before_removes(self) -> ClientTable:
        return ClientTable.create(self.ticking_update.BeforeRemoves())

    @property
    def removed_rows(self) -> RowSequence:
        return RowSequence.create(self.ticking_update.RemovedRows())

    @property
    def after_removes(self) -> ClientTable:
        return ClientTable.create(self.ticking_update.AfterRemoves())

    @property
    def before_adds(self) -> ClientTable:
        return ClientTable.create(self.ticking_update.BeforeAdds())

    @property
    def added_rows(self) -> RowSequence:
        return RowSequence.create(self.ticking_update.AddedRows())

    @property
    def after_adds(self) -> ClientTable:
        return ClientTable.create(self.ticking_update.AfterAdds())

    @property
    def before_modifies(self) -> ClientTable:
        return ClientTable.create(self.ticking_update.BeforeModifies())

    @property
    def after_modifies(self) -> ClientTable:
        return ClientTable.create(self.ticking_update.AfterModifies())

    @property
    def modified_rows(self) -> [RowSequence]:
        result = []
        mod_rows = self.ticking_update.ModifiedRows()
        for i in range(mod_rows.size()):
            result.append(RowSequence.create(mod_rows[i]))
        return result

    @property
    def all_modified_rows(self) -> RowSequence:
        rs = self.ticking_update.AllModifiedRows()
        return RowSequence.create(rs)

    @property
    def current(self) -> ClientTable:
        return ClientTable.create(self.ticking_update.Current())

# Simple wrapper of the corresponding C++ ClientTable class.
cdef class ClientTable:
    cdef shared_ptr[CClientTable] _table

    @staticmethod
    cdef ClientTable create(shared_ptr[CClientTable] table):
        result = ClientTable()
        result._table = move(table)
        return result

    def get_column(self, columnIndex: int) -> ColumnSource:
        cs = deref(self._table).GetColumn(columnIndex)
        return ColumnSource.create(move(cs))

    def get_column_by_name(self, name: str, strict: bool) -> ColumnSource | None:
        name_bytes = name.encode()
        result = deref(self._table).GetColumn(name_bytes, strict)
        if result == NULL:
            return None
        return ColumnSource.create(move(result))

    @property
    def columns(self) -> [ColumnSource]:
        ncols = deref(self._table).NumColumns()
        return [ColumnSource.create(deref(self._table).GetColumn(i)) for i in range(ncols)]

    def get_column_index(self, name: unicode, strict: bool) -> int | None:
        name_as_string = <string>name.encode()
        res = deref(self._table).GetColumnIndex(name_as_string, strict)
        if not res.has_value():
            return None
        return deref(res)

    def get_row_sequence(self) -> RowSequence:
        result = deref(self._table).GetRowSequence()
        return RowSequence.create(move(result))

    @property
    def num_rows(self) -> size_t:
        return deref(self._table).NumRows()

    @property
    def num_columns(self) -> size_t:
        return deref(self._table).NumColumns()

    @property
    def schema(self) -> Schema:
        c_schema = deref(self._table).Schema()
        return Schema.create_from_c_schema(move(c_schema))

    def to_string(self, want_headers: bool, want_row_numbers: bool, row_sequence = None) -> str:
        cdef vector[shared_ptr[CRowSequence]] row_sequences
        if row_sequence is None:
            result = deref(self._table).ToString(want_headers, want_row_numbers)
        elif isinstance(row_sequence, list):
            for rs in row_sequence:
                row_sequences.push_back((<RowSequence>rs).row_sequence)
            result = deref(self._table).ToString(want_headers, want_row_numbers, move(row_sequences))
        elif isinstance(row_sequence, RowSequence):
            result = deref(self._table).ToString(want_headers, want_row_numbers, (<RowSequence>row_sequence).row_sequence)
        else:
            raise RuntimeError("Don't know how to handle", row_sequence)
        return result.decode()


# A wrapper of the corresponding C++ schema class. This wrapper also determines the corresponding pyarrow types
# and stores them here, in case that is useful for some low-level caller.
cdef class Schema:
    cdef shared_ptr[CSchema] _schema
    _names: Sequence[str]
    _pa_types: Sequence[pa.DataType]

    @staticmethod
    cdef Schema create_from_c_schema(shared_ptr[CSchema] schema):
        c_names = deref(schema).Names()
        c_types = deref(schema).Types()
        names: [str] = []
        types: [pa.DataType] = []
        cdef size_t i
        for i in range(c_names.size()):
            name = c_names[i].decode()
            type = c_types[i]
            pa_type = _dh_type_to_pa_type(type)
            names.append(name)
            types.append(pa_type)
        return Schema._createHelper(move(schema), names, types)

    @staticmethod
    cdef Schema _createHelper(shared_ptr[CSchema] schema, names: Sequence[str], types: Sequence[pa.DataType]):
        result = Schema()
        result._schema = move(schema)
        result._names = names
        result._pa_types = types
        return result

    @property
    def names(self) -> Sequence[str]:
        return self._names

    @property
    def pa_types(self) -> Sequence[pa.DataType]:
        return self._pa_types


# Simple wrapper of the corresponding C++ RowSequence class.
cdef class RowSequence:
    cdef shared_ptr[CRowSequence] row_sequence

    @staticmethod
    cdef RowSequence create(shared_ptr[CRowSequence] row_sequence):
        result = RowSequence()
        result.row_sequence = move(row_sequence)
        return result

    def Take(self, size: int) -> RowSequence:
        row_sequence = deref(self.row_sequence).Take(size)
        return RowSequence.create(move(row_sequence))

    def Drop(self, size: int) -> RowSequence:
        row_sequence = deref(self.row_sequence).Drop(size)
        return RowSequence.create(move(row_sequence))

    @property
    def size(self) -> size_t:
        return deref(self.row_sequence).Size()

    @property
    def empty(self) -> bool:
        return deref(self.row_sequence).Empty()

# The "primitive" set of types, used for the method _fill_primitive_chunk.
ctypedef fused nparray_primitive_dtype_t:
    int8_t
    int16_t
    int32_t
    int64_t
    float
    double
    bool

# A wrapper of the corresponding C++ ColumnSource class. In order to be Python-friendly, provides a get_chunk()
# method not available in C++: this method allocates a PyArrow array of the right type, populates it (including
# correct values for nulls), and returns it.
cdef class ColumnSource:
    cdef shared_ptr[CColumnSource] column_source

    @staticmethod
    cdef ColumnSource create(shared_ptr[CColumnSource] column_source):
        result = ColumnSource()
        result.column_source = move(column_source)
        return result

    def get_chunk(self, rows: RowSequence) -> pa.Array:
        cdef size_t size
        cdef ElementTypeId element_type_id

        size = rows.size

        null_flags = np.zeros(size, dtype=np.bool_)
        cdef bool[::1] null_flags_view = null_flags
        boolean_chunk = CGenericChunk[bool].CreateView(&null_flags_view[0], size)
        cdef CGenericChunk[bool] *null_flags_ptr = &boolean_chunk
        arrow_type: pa.DataType

        element_type_id = CCythonSupport.GetElementTypeId(deref(self.column_source))
        if element_type_id == ElementTypeId.kChar:
            dest_data = np.zeros(size, np.uint16)
            self._fill_char_chunk(rows, dest_data, null_flags_ptr)
            arrow_type = pa.uint16()
        elif element_type_id == ElementTypeId.kInt8:
            dest_data = np.zeros(size, np.int8)
            self._fill_primitive_chunk[int8_t](rows, dest_data, null_flags_ptr)
            arrow_type = pa.int8()
        elif element_type_id == ElementTypeId.kInt16:
            dest_data = np.zeros(size, np.int16)
            self._fill_primitive_chunk[int16_t](rows, dest_data, null_flags_ptr)
            arrow_type = pa.int16()
        elif element_type_id == ElementTypeId.kInt32:
            dest_data = np.zeros(size, np.int32)
            self._fill_primitive_chunk[int32_t](rows, dest_data, null_flags_ptr)
            arrow_type = pa.int32()
        elif element_type_id == ElementTypeId.kInt64:
            dest_data = np.zeros(size, np.int64)
            self._fill_primitive_chunk[int64_t](rows, dest_data, null_flags_ptr)
            arrow_type = pa.int64()
        elif element_type_id == ElementTypeId.kFloat:
            dest_data = np.zeros(size, np.float32)
            self._fill_primitive_chunk[float](rows, dest_data, null_flags_ptr)
            arrow_type = pa.float32()
        elif element_type_id == ElementTypeId.kDouble:
            dest_data = np.zeros(size, np.float64)
            self._fill_primitive_chunk[double](rows, dest_data, null_flags_ptr)
            arrow_type = pa.float64()
        elif element_type_id == ElementTypeId.kBool:
            dest_data = np.zeros(size, np.bool_)
            self._fill_primitive_chunk[bool](rows, dest_data, null_flags_ptr)
            arrow_type = pa.bool_()
        elif element_type_id == ElementTypeId.kString:
            dest_data = np.zeros(size, object)
            self._fill_string_chunk(rows, dest_data, null_flags_ptr)
            arrow_type = pa.string()
        elif element_type_id == ElementTypeId.kTimestamp:
            dest_data = np.zeros(size, dtype="datetime64[ns]")
            dest_data_as_int64 = dest_data.view(dtype=np.int64)
            self._fill_timestamp_chunk(rows, dest_data_as_int64, null_flags_ptr)
            arrow_type = pa.timestamp("ns", tz="UTC")
        elif element_type_id == ElementTypeId.kLocalDate:
            dest_data = np.zeros(size, dtype=np.int64)
            dest_data_as_int64 = dest_data.view(dtype=np.int64)
            self._fill_localdate_chunk(rows, dest_data_as_int64, null_flags_ptr)
            arrow_type = pa.date64()
        elif element_type_id == ElementTypeId.kLocalTime:
            dest_data = np.zeros(size, dtype=np.int64)
            dest_data_as_int64 = dest_data.view(dtype=np.int64)
            self._fill_localtime_chunk(rows, dest_data_as_int64, null_flags_ptr)
            arrow_type = pa.time64("ns")
        else:
           raise RuntimeError(f"Unexpected ElementTypeId {<int>element_type_id}")

        return pa.array(dest_data, type=arrow_type, mask=null_flags)

    # fill_chunk helper method for any of the primitive data types.
    cdef _fill_primitive_chunk(self, rows: RowSequence, nparray_primitive_dtype_t[::1] dest_data,
        CGenericChunk[bool] *null_flags_ptr):
        rsSize = rows.size
        data_chunk = CGenericChunk[nparray_primitive_dtype_t].CreateView(&dest_data[0], rsSize)
        deref(self.column_source).FillChunk(deref(rows.row_sequence), &data_chunk, null_flags_ptr)

    # fill_chunk helper method for Deephaven element type char (mapped to uint16_t).
    # In this case we use a column source type of char16_t and we do shameless type aliasing.
    cdef _fill_char_chunk(self, rows: RowSequence, uint16_t[::1] dest_data, CGenericChunk[bool] *null_flags_ptr):
        cdef extern from *:
            """
            static_assert(sizeof(uint16_t) == sizeof(char16_t));
            """
        rsSize = rows.size
        # hacky cast
        data_chunk = CGenericChunk[char16_t].CreateView(<char16_t*>&dest_data[0], rsSize)
        deref(self.column_source).FillChunk(deref(rows.row_sequence), &data_chunk, null_flags_ptr)

    # fill_chunk helper method for Deephaven element type string (mapped to object).
    # In this case we have to allocate a temporary chunk of strings, fill it, and then copy the data out of it to
    # Python strings. TODO(kosak): This is too many copies and should be improved.
    cdef _fill_string_chunk(self, rows: RowSequence, object[::1] dest_data, CGenericChunk[bool] *null_flags_ptr):
        rsSize = rows.size
        dest_chunk = CGenericChunk[string].Create(rsSize)
        deref(self.column_source).FillChunk(deref(rows.row_sequence), &dest_chunk, null_flags_ptr)
        cdef ssize_t i
        for i in range(rsSize):
            dest_data[i] = dest_chunk.data()[i]

    # fill_chunk helper method for timestamp. In this case we shamelessly treat the Python timestamp
    # type as an int64, and then further shamelessly pretend that it's a Deephaven DateTime type.
    cdef _fill_timestamp_chunk(self, rows: RowSequence, int64_t[::1] dest_data, CGenericChunk[bool] *null_flags_ptr):
        cdef extern from "<type_traits>":
            """
            static_assert(deephaven::dhcore::DateTime::IsBlittableToInt64());
            """
        rsSize = rows.size
        dest_chunk = CGenericChunk[CDateTime].CreateView(<CDateTime*>&dest_data[0], rsSize)
        deref(self.column_source).FillChunk(deref(rows.row_sequence), &dest_chunk, null_flags_ptr)

    # fill_chunk helper method for LocalDate. In this case we shamelessly treat the Python timestamp
    # type as an int64, and then further shamelessly pretend that it's a Deephaven LocalDate type.
    cdef _fill_localdate_chunk(self, rows: RowSequence, int64_t[::1] dest_data, CGenericChunk[bool] *null_flags_ptr):
        cdef extern from "<type_traits>":
            """
            static_assert(deephaven::dhcore::LocalDate::IsBlittableToInt64());
            """
        rsSize = rows.size
        dest_chunk = CGenericChunk[CLocalDate].CreateView(<CLocalDate*>&dest_data[0], rsSize)
        deref(self.column_source).FillChunk(deref(rows.row_sequence), &dest_chunk, null_flags_ptr)

    # fill_chunk helper method for LocalTime. In this case we shamelessly treat the Python timestamp
    # type as an int64, and then further shamelessly pretend that it's a Deephaven LocalTime type.
    cdef _fill_localtime_chunk(self, rows: RowSequence, int64_t[::1] dest_data, CGenericChunk[bool] *null_flags_ptr):
        cdef extern from "<type_traits>":
            """
            static_assert(deephaven::dhcore::LocalTime::IsBlittableToInt64());
            """
        rsSize = rows.size
        dest_chunk = CGenericChunk[CLocalTime].CreateView(<CLocalTime*>&dest_data[0], rsSize)
        deref(self.column_source).FillChunk(deref(rows.row_sequence), &dest_chunk, null_flags_ptr)

# Converts an Arrow array to a C++ ColumnSource of the right type. The created column source does not own the
# memory used, so it is only valid as long as the original Arrow array is valid.
cdef shared_ptr[CColumnSource] _convert_arrow_array_to_column_source(array: pa.Array) except *:
    if isinstance(array, pa.lib.StringArray):
        return _convert_arrow_string_array_to_column_source(cast(pa.lib.StringArray, array))
    if isinstance(array, pa.lib.BooleanArray):
        return _convert_arrow_boolean_array_to_column_source(cast(pa.lib.BooleanArray, array))
    if isinstance(array, pa.lib.TimestampArray):
        return _convert_arrow_timestamp_array_to_column_source(cast(pa.lib.TimestampArray, array))
    if isinstance(array, pa.lib.Date64Array):
        return _convert_arrow_date64_array_to_column_source(cast(pa.lib.Date64Array, array))
    if isinstance(array, pa.lib.Time64Array):
        return _convert_arrow_time64_array_to_column_source(cast(pa.lib.Time64Array, array))
    buffers = array.buffers()
    if len(buffers) != 2:
        raise RuntimeError(f"Expected 2 simple type buffers, got {len(buffers)}")

    # buffers[0] is the validity array, but we don't care about it because for numeric types we rely on
    # the Deephaven null convention which reserves a special value in the range to mean null.
    databuf = buffers[1]
    cdef const void *address = <const void *><intptr_t>databuf.address
    cdef size_t total_size = databuf.size

    # not sure I'm supposed to look inside pa.lib, but that's the only place I can find DoubleArray
    cdef shared_ptr[CColumnSource] result
    if isinstance(array, pa.lib.UInt16Array):
        assign_shared_ptr(result, CNumericBufferColumnSource[char16_t].CreateUntyped(address, total_size // 2))
    elif isinstance(array, pa.lib.Int8Array):
        assign_shared_ptr(result, CNumericBufferColumnSource[int8_t].CreateUntyped(address, total_size))
    elif isinstance(array, pa.lib.Int16Array):
        assign_shared_ptr(result, CNumericBufferColumnSource[int16_t].CreateUntyped(address, total_size // 2))
    elif isinstance(array, pa.lib.Int32Array):
        assign_shared_ptr(result, CNumericBufferColumnSource[int32_t].CreateUntyped(address, total_size // 4))
    elif isinstance(array, pa.lib.Int64Array):
        assign_shared_ptr(result, CNumericBufferColumnSource[int64_t].CreateUntyped(address, total_size // 8))
    elif isinstance(array, pa.lib.FloatArray):
        assign_shared_ptr(result, CNumericBufferColumnSource[float].CreateUntyped(address, total_size // 4))
    elif isinstance(array, pa.lib.DoubleArray):
        assign_shared_ptr(result, CNumericBufferColumnSource[double].CreateUntyped(address, total_size // 8))
    else:
        raise RuntimeError(f"Can't find a column source type for {type(array)}")

    return move(result)

# Converts an Arrow BooleanArray to a C++ BooleanColumnSource. The created column source does not own the
# memory used, so it is only valid as long as the original Arrow array is valid.
cdef shared_ptr[CColumnSource] _convert_arrow_boolean_array_to_column_source(array: pa.BooleanArray) except *:
    num_elements = len(array)
    buffers = array.buffers()
    if len(buffers) != 2:
        raise RuntimeError(f"Expected 2 boolean buffers, got {len(buffers)}")
    validity = buffers[0]
    data = buffers[1]

    cdef const uint8_t *validity_begin = NULL;
    cdef const uint8_t *validity_end = NULL;
    if validity is not None:
        validity_begin = <const uint8_t *><intptr_t>validity.address
        validity_end = <const uint8_t *><intptr_t>(validity.address + validity.size)

    cdef const uint8_t *data_begin = <const uint8_t *><intptr_t>data.address
    cdef const uint8_t *data_end = <const uint8_t *><intptr_t>(data.address + data.size)

    return CCythonSupport.CreateBooleanColumnSource(data_begin, data_end, validity_begin, validity_end,
        num_elements)

# Converts an Arrow StringArray to a C++ StringColumnSource. The created column source does not own the
# memory used, so it is only valid as long as the original Arrow array is valid.
cdef shared_ptr[CColumnSource] _convert_arrow_string_array_to_column_source(array: pa.StringArray) except *:
    num_elements = len(array)
    buffers = array.buffers()
    if len(buffers) != 3:
        raise RuntimeError(f"Expected 3 string buffers, got {len(buffers)}")
    validity = buffers[0]
    starts = buffers[1]
    text = buffers[2]

    cdef const uint8_t *validity_begin = NULL;
    cdef const uint8_t *validity_end = NULL;
    if validity is not None:
        validity_begin = <uint8_t *><intptr_t>validity.address
        validity_end = <uint8_t *><intptr_t>(validity.address + validity.size)

    cdef const uint32_t *starts_begin = <const uint32_t*><intptr_t>starts.address
    cdef const uint32_t *starts_end = <const uint32_t*><intptr_t>(starts.address + starts.size)

    cdef const char *text_begin = <const char*><intptr_t>text.address
    cdef const char *text_end = <const char*><intptr_t>(text.address + text.size)

    return CCythonSupport.CreateStringColumnSource(text_begin, text_end, starts_begin, starts_end,
        validity_begin, validity_end, num_elements)

# Converts an Arrow TimestampArray to a C++ DateTimeColumnSource. The created column source does not own the
# memory used, so it is only valid as long as the original Arrow array is valid.
cdef shared_ptr[CColumnSource] _convert_arrow_timestamp_array_to_column_source(array: pa.TimestampArray) except *:
    return _convert_underlying_int64_to_column_source(array, CCythonSupport.CreateDateTimeColumnSource)

# Converts an Arrow Date64Array to a C++ LocalDateColumnSource. The created column source does not own the
# memory used, so it is only valid as long as the original Arrow array is valid.
cdef shared_ptr[CColumnSource] _convert_arrow_date64_array_to_column_source(array: pa.Date64Array) except *:
    return _convert_underlying_int64_to_column_source(array, CCythonSupport.CreateLocalDateColumnSource)

# Converts an Arrow Time64Array to a C++ LocalTimeColumnSource. The created column source does not own the
# memory used, so it is only valid as long as the original Arrow array is valid.
cdef shared_ptr[CColumnSource] _convert_arrow_time64_array_to_column_source(array: pa.Time64Array) except *:
    return _convert_underlying_int64_to_column_source(array, CCythonSupport.CreateLocalTimeColumnSource)

# Signature of one of the factory functions in CCythonSupport: CreateDateTimeColumnSource, CreateLocalDateColumnSource
# or CreateLocalTimeColumnSource.
ctypedef shared_ptr[CColumnSource](*factory_t)(const int64_t *, const int64_t *, const uint8_t *, const uint8_t *, size_t)

# Converts one of the numeric Arrow types with an underlying int64 representation to the
# corresponding ColumnSource type. The created column source does not own the
# memory used, so it is only valid as long as the original Arrow array is valid.
cdef shared_ptr[CColumnSource] _convert_underlying_int64_to_column_source(
        array: pa.NumericArray,
        factory: factory_t) except *:
    num_elements = len(array)
    buffers = array.buffers()
    if len(buffers) != 2:
        raise RuntimeError(f"Expected 2 buffers, got {len(buffers)}")
    validity = buffers[0]
    data = buffers[1]

    cdef const uint8_t *validity_begin = NULL;
    cdef const uint8_t *validity_end = NULL;
    if validity is not None:
        validity_begin = <uint8_t *><intptr_t>validity.address
        validity_end = <uint8_t *><intptr_t>(validity.address + validity.size)

    cdef const int64_t *data_begin = <const int64_t *> <intptr_t> data.address
    cdef const int64_t *data_end = <const int64_t *> <intptr_t> (data.address + data.size)
    return factory(data_begin, data_end, validity_begin, validity_end, num_elements)

# This method converts a PyArrow Schema object to a C++ Schema object.
cdef shared_ptr[CSchema] _pyarrow_schema_to_deephaven_schema(src: pa.Schema) except *:
    if len(src.names) != len(src.types):
        raise RuntimeError("Unexpected: schema lengths are inconsistent")

    cdef vector[string] names
    cdef vector[ElementTypeId] types

    for i in range(len(src.names)):
        name = src.names[i].encode()
        dh_type = _pa_type_to_dh_type(src.types[i])
        names.push_back(name)
        types.push_back(dh_type)

    return CSchema.Create(names, types)

# Code to support processing of Barrage messages. The reason this is somewhat complicated is because there is a
# sharp division of responsibilities: the Python side does all the Arrow interactions and knows Arrow data types;
# meanwhile the C++ side does not know anything about Arrow and only works with Column Sources. For example, when
# a Barrage message comes in, the Python code will turn it into ColumnSources for consumption by the C++ library.
# Meanwhile when client code wants to access data in a ClientTable, C++ will return it as ColumnSources and this
# library will need to translate it back into Arrow arrays for consumption by the user.
cdef class BarrageProcessor:
    cdef CBarrageProcessor _barrage_processor

    # The Python code that drives the Arrow interaction by doing a do_exchange() needs to send a
    # BarrageSubscriptionRequest. However the Python code knows nothing about Barrage and the C++ code knows
    # nothing about Arrow. This method is used to format a BarrageSubscriptionRequest as an array of bytes and
    # give it to the Python code so that it can send it to the server over Arrow.
    @staticmethod
    def create_subscription_request(const unsigned char [::1] ticket_bytes) -> bytearray:
        res = CBarrageProcessor.CreateSubscriptionRequestCython(&ticket_bytes[0],
            ticket_bytes.shape[0])
        return res

    @staticmethod
    def create(pa_schema: pa.Schema) -> BarrageProcessor:
        dh_schema = _pyarrow_schema_to_deephaven_schema(pa_schema)
        result = BarrageProcessor()
        result._barrage_processor = CBarrageProcessor(move(dh_schema))
        return result

    # The Python code that drives the Arrow interaction does not know how to interpret the metadata message
    # (which contain Deephaven indices) nor does it know how to properly interpret the Arrow column data it is getting.
    # (You can't know, without looking at the metadata, where the incoming data is meant to be adds or modifies, and
    # in what row positions it belongs in).
    # Instead the Python code driving the interaction repeatedly waits for incoming Barrage messages, forwards them
    # here, and we in turn forward them to C++. C++ uses these messages to build a TickingUpdate message. An update
    # might be spread out over multiple Barrage messages, so it's possible we could push several Barrage massages to
    # the C++ client before getting a reply. See TableListenerHandle._process_data() for an example of how we are
    # called. The relevant part is reproduced here:
    #
    # while True:
    #   data, metadata = self._reader.read_chunk()
    #   ticking_update = self._bp.process_next_chunk(data.columns, metadata)
    #   if ticking_update is not None:
    #     table_update = TableUpdate(ticking_update)
    #     self._listener.on_update(table_update)
    def process_next_chunk(self, sources: [], const char [::1] raw_metadata) -> TickingUpdate | None:
        cdef vector[shared_ptr[CColumnSource]] column_sources
        cdef vector[size_t] sizes
        for source in sources:
            # source is a ListArray of length 1
            values = source.values
            cs = _convert_arrow_array_to_column_source(values)
            column_sources.push_back(cs)
            sizes.push_back(len(values))

        cdef const void *mdptr = NULL
        cdef size_t mdsize = 0
        if raw_metadata is not None:
            mdptr = &raw_metadata[0]
            mdsize = raw_metadata.shape[0]
        result = self._barrage_processor.ProcessNextChunk(column_sources, sizes, mdptr, mdsize)

        if result.has_value():
            return TickingUpdate.create(deref(result))
        return None

# A class representing the relationship between a Deephaven type and a PyArrow type.
cdef class _EquivalentTypes:
    cdef ElementTypeId dh_type
    pa_type: pa.DataType

    @staticmethod
    cdef create(ElementTypeId dh_type, pa_type: pa.DataType):
        result = _EquivalentTypes()
        result.dh_type = dh_type
        result.pa_type = pa_type
        return result

cdef _equivalentTypes = [
    _EquivalentTypes.create(ElementTypeId.kChar, pa.uint16()),
    _EquivalentTypes.create(ElementTypeId.kInt8, pa.int8()),
    _EquivalentTypes.create(ElementTypeId.kInt16, pa.int16()),
    _EquivalentTypes.create(ElementTypeId.kInt32, pa.int32()),
    _EquivalentTypes.create(ElementTypeId.kInt64, pa.int64()),
    _EquivalentTypes.create(ElementTypeId.kFloat, pa.float32()),
    _EquivalentTypes.create(ElementTypeId.kDouble, pa.float64()),
    _EquivalentTypes.create(ElementTypeId.kBool, pa.bool_()),
    _EquivalentTypes.create(ElementTypeId.kString, pa.string()),
    _EquivalentTypes.create(ElementTypeId.kTimestamp, pa.timestamp("ns", "UTC")),
    _EquivalentTypes.create(ElementTypeId.kLocalDate, pa.date64()),
    _EquivalentTypes.create(ElementTypeId.kLocalTime, pa.time64("ns"))
]

# Converts a Deephaven type (an enum) into the corresponding PyArrow type.
cdef _dh_type_to_pa_type(dh_type: ElementTypeId):
    for et_python in _equivalentTypes:
        et = <_EquivalentTypes>et_python
        if et.dh_type == dh_type:
            return et.pa_type
    raise RuntimeError(f"Can't convert Deephaven type {<int>dh_type} to pyarrow type type")

# Converts a PyArrow type into the corresponding PyArrow type.
cdef ElementTypeId _pa_type_to_dh_type(pa_type: pa.DataType) except *:
    for et_python in _equivalentTypes:
        et = <_EquivalentTypes>et_python
        if et.pa_type == pa_type:
            return et.dh_type
    raise RuntimeError(f"Can't convert pyarrow type {pa_type} to Deephaven type")
