# distutils: language = c++
# cython: language_level = 3

#
# Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#
from libc.stdint cimport int8_t, uint8_t, int32_t, int64_t, uint32_t
from libcpp cimport bool
from libcpp.memory cimport shared_ptr, unique_ptr
from libcpp.string cimport string
from libcpp.utility cimport pair
from libcpp.vector cimport vector

# hack to make Cython aware of the char16_t type
cdef extern from *:
    ctypedef enum char16_t:
        pass

ctypedef void (*CCallback)(CTickingUpdate, void *)
ctypedef void (*cOnTickCallback_t)(CTickingUpdate, void *)
ctypedef void (*cOnErrorCallback_t)(string, void *)

# make our own declaration for std::optional<T>, for now
cdef extern from "<optional>" namespace "std":
    cdef cppclass optional[T]:
        T &operator*()
        bool has_value() const

# https://stackoverflow.com/questions/67626270/inheritance-and-stdshared-ptr-in-cython
cdef extern from *:
    """
    template<typename T1, typename T2>
    void assign_shared_ptr(std::shared_ptr<T1> &lhs, const std::shared_ptr<T2> &rhs) {
         lhs = rhs;
    }
    """
    void assign_shared_ptr[T1, T2](shared_ptr[T1] &lhs, shared_ptr[T2] &rhs)

cdef extern from "deephaven/dhcore/chunk/chunk.h" namespace "deephaven::dhcore::chunk":
    cdef cppclass CChunk "deephaven::dhcore::chunk::Chunk":
        CChunk()
        CChunk(CChunk &&other)

    cdef cppclass CGenericChunk "deephaven::dhcore::chunk::GenericChunk" [T] (CChunk):
        @staticmethod
        CGenericChunk[T] create(size_t size)

        @staticmethod
        CGenericChunk[T] createView(T *data, size_t size)

        const T *data()

cdef extern from "deephaven/dhcore/container/row_sequence.h" namespace "deephaven::dhcore::container":
    cdef cppclass CRowSequence "deephaven::dhcore::container::RowSequence":
        CRowSequence()
        CRowSequence(CRowSequence &&other)

        shared_ptr[CRowSequence] take(size_t size)
        shared_ptr[CRowSequence] drop(size_t size)

        size_t size()
        bool empty()

cdef extern from "deephaven/dhcore/clienttable/client_table.h" namespace "deephaven::dhcore::clienttable":
    cdef cppclass CClientTable "deephaven::dhcore::clienttable::ClientTable":
        shared_ptr[CColumnSource] getColumn(size_t colIndex) except +
        shared_ptr[CColumnSource] getColumn(string name, bool strict) except +
        optional[size_t] getColumnIndex(string name, bool strict) except +
        shared_ptr[CRowSequence] getRowSequence() except +

        size_t numRows()
        size_t numColumns()

        shared_ptr[CSchema] schema()

        string toString(bool wantHeaders, wantRowNumbers) except +
        string toString(bool wantHeaders, wantRowNumbers, shared_ptr[CRowSequence] rowSequence) except +
        string toString(bool wantHeaders, wantRowNumbers, vector[shared_ptr[CRowSequence]] rowSequences) except +

cdef extern from "deephaven/dhcore/clienttable/schema.h" namespace "deephaven::dhcore::clienttable":
    cdef cppclass CSchema "deephaven::dhcore::clienttable::Schema":
        @staticmethod
        shared_ptr[CSchema] create(const vector[string] &names, const vector[ElementTypeId] &types)

        CSchema()

        const vector[string] &names()
        const vector[ElementTypeId] &types()

cdef extern from "deephaven/dhcore/ticking/ticking.h" namespace "deephaven::dhcore::ticking":
    cdef cppclass CTickingUpdate "deephaven::dhcore::ticking::TickingUpdate":
        CTickingUpdate()
        CTickingUpdate(CTickingUpdate &&other)

        shared_ptr[CClientTable] prev()
        shared_ptr[CClientTable] beforeRemoves()
        shared_ptr[CRowSequence] removedRows()
        shared_ptr[CClientTable] afterRemoves()
        shared_ptr[CClientTable] beforeAdds()
        shared_ptr[CRowSequence] addedRows()
        shared_ptr[CClientTable] afterAdds()
        shared_ptr[CClientTable] beforeModifies()
        vector[shared_ptr[CRowSequence]] modifiedRows()
        shared_ptr[CRowSequence] allModifiedRows()
        shared_ptr[CClientTable] afterModifies()
        shared_ptr[CClientTable] current()

cdef extern from "deephaven/dhcore/column/buffer_column_source.h" namespace "deephaven::dhcore::column":
    cdef cppclass CNumericBufferColumnSource "deephaven::dhcore::column::NumericBufferColumnSource" [T]:
        @staticmethod
        shared_ptr[CNumericBufferColumnSource[T]] createUntyped(const void *start, size_t size)

        void fillChunk(const CRowSequence &rows, CChunk *destData, CGenericChunk[bool] *optionalDestNullFlags) except +

cdef extern from "deephaven/dhcore/column/column_source.h" namespace "deephaven::dhcore::column":
    cdef cppclass CColumnSource "deephaven::dhcore::column::ColumnSource":
        void fillChunk(const CRowSequence &rows, CChunk *destData,
            CGenericChunk[bool] *optionalDestNullFlags) except +

cdef extern from "deephaven/dhcore/column/column_source_helpers.h" namespace "deephaven::dhcore::column":
    cdef cppclass CHumanReadableElementTypeName "deephaven::dhcore::column::HumanReadableElementTypeName":
        @staticmethod
        const char *getName(const CColumnSource &columnSource)

    cdef cppclass CHumanReadableStaticTypeName "deephaven::dhcore::column::HumanReadableStaticTypeName" [T]:
        @staticmethod
        const char *getName()

cdef extern from "deephaven/dhcore/types.h" namespace "deephaven::dhcore":
    ctypedef enum ElementTypeId "deephaven::dhcore::ElementTypeId::Enum":
        CHAR "deephaven::dhcore::ElementTypeId::CHAR"
        INT8 "deephaven::dhcore::ElementTypeId::INT8"
        INT16 "deephaven::dhcore::ElementTypeId::INT16"
        INT32 "deephaven::dhcore::ElementTypeId::INT32"
        INT64 "deephaven::dhcore::ElementTypeId::INT64"
        FLOAT "deephaven::dhcore::ElementTypeId::FLOAT"
        DOUBLE "deephaven::dhcore::ElementTypeId::DOUBLE"
        BOOL "deephaven::dhcore::ElementTypeId::BOOL"
        STRING "deephaven::dhcore::ElementTypeId::STRING"
        TIMESTAMP "deephaven::dhcore::ElementTypeId::TIMESTAMP"

    cdef cppclass CDateTime "deephaven::dhcore::DateTime":
        pass

cdef extern from "deephaven/dhcore/utility/cython_support.h" namespace "deephaven::dhcore::utility":
    cdef cppclass CCythonSupport "deephaven::dhcore::utility::CythonSupport":
        @staticmethod
        shared_ptr[CColumnSource] createBooleanColumnSource(const uint8_t *dataBegin, const uint8_t *dataEnd,
            const uint8_t *validityBegin, const uint8_t *validityEnd, size_t numElements)

        @staticmethod
        shared_ptr[CColumnSource] createStringColumnSource(const char *textBegin, const char *textEnd,
            const uint32_t *offsetsBegin, const uint32_t *offsetsEnd,
            const uint8_t *validityBegin, const uint8_t *validityEnd, size_t numElements)

        @staticmethod
        shared_ptr[CColumnSource] createDateTimeColumnSource(const int64_t *dataBegin, const int64_t *dataEnd,
            const uint8_t *validityBegin, const uint8_t *validityEnd, size_t numElements)

        @staticmethod
        ElementTypeId getElementTypeId(const CColumnSource &columnSource)

cdef extern from "deephaven/dhcore/ticking/barrage_processor.h" namespace "deephaven::dhcore::ticking":
    cdef cppclass CBarrageProcessor "deephaven::dhcore::ticking::BarrageProcessor":
        @staticmethod
        string createSubscriptionRequestCython(const void *ticketBytes, size_t size)

        CBarrageProcessor()
        CBarrageProcessor(shared_ptr[CSchema] schema)

        optional[CTickingUpdate] processNextChunk(const vector[shared_ptr[CColumnSource]] &sources,
            const vector[size_t] &sizes, void *metadata, size_t metadataSize) except +
