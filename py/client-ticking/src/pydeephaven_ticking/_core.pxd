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
        CChunk(CChunk other)

    cdef cppclass CGenericChunk "deephaven::dhcore::chunk::GenericChunk" [T] (CChunk):
        @staticmethod
        CGenericChunk[T] Create(size_t size)

        @staticmethod
        CGenericChunk[T] CreateView(T *data, size_t size)

        const T *data()

cdef extern from "deephaven/dhcore/container/row_sequence.h" namespace "deephaven::dhcore::container":
    cdef cppclass CRowSequence "deephaven::dhcore::container::RowSequence":
        CRowSequence()
        CRowSequence(CRowSequence other)

        shared_ptr[CRowSequence] Take(size_t size)
        shared_ptr[CRowSequence] Drop(size_t size)

        size_t Size()
        bool Empty()

cdef extern from "deephaven/dhcore/clienttable/client_table.h" namespace "deephaven::dhcore::clienttable":
    cdef cppclass CClientTable "deephaven::dhcore::clienttable::ClientTable":
        shared_ptr[CColumnSource] GetColumn(size_t colIndex) except +
        shared_ptr[CColumnSource] GetColumn(string name, bool strict) except +
        optional[size_t] GetColumnIndex(string name, bool strict) except +
        shared_ptr[CRowSequence] GetRowSequence() except +

        size_t NumRows()
        size_t NumColumns()

        shared_ptr[CSchema] Schema()

        string ToString(bool wantHeaders, wantRowNumbers) except +
        string ToString(bool wantHeaders, wantRowNumbers, shared_ptr[CRowSequence] rowSequence) except +
        string ToString(bool wantHeaders, wantRowNumbers, vector[shared_ptr[CRowSequence]] rowSequences) except +

cdef extern from "deephaven/dhcore/clienttable/schema.h" namespace "deephaven::dhcore::clienttable":
    cdef cppclass CSchema "deephaven::dhcore::clienttable::Schema":
        @staticmethod
        shared_ptr[CSchema] Create(const vector[string] &names, const vector[ElementTypeId] &types)

        CSchema()

        const vector[string] &Names()
        const vector[ElementTypeId] &Types()

cdef extern from "deephaven/dhcore/ticking/ticking.h" namespace "deephaven::dhcore::ticking":
    cdef cppclass CTickingUpdate "deephaven::dhcore::ticking::TickingUpdate":
        CTickingUpdate()
        CTickingUpdate(CTickingUpdate other)

        shared_ptr[CClientTable] Prev()
        shared_ptr[CClientTable] BeforeRemoves()
        shared_ptr[CRowSequence] RemovedRows()
        shared_ptr[CClientTable] AfterRemoves()
        shared_ptr[CClientTable] BeforeAdds()
        shared_ptr[CRowSequence] AddedRows()
        shared_ptr[CClientTable] AfterAdds()
        shared_ptr[CClientTable] BeforeModifies()
        vector[shared_ptr[CRowSequence]] ModifiedRows()
        shared_ptr[CRowSequence] AllModifiedRows()
        shared_ptr[CClientTable] AfterModifies()
        shared_ptr[CClientTable] Current()

cdef extern from "deephaven/dhcore/column/buffer_column_source.h" namespace "deephaven::dhcore::column":
    cdef cppclass CNumericBufferColumnSource "deephaven::dhcore::column::NumericBufferColumnSource" [T]:
        @staticmethod
        shared_ptr[CNumericBufferColumnSource[T]] CreateUntyped(const void *start, size_t size)

        void FillChunk(const CRowSequence &rows, CChunk *destData, CGenericChunk[bool] *optionalDestNullFlags) except +

cdef extern from "deephaven/dhcore/column/column_source.h" namespace "deephaven::dhcore::column":
    cdef cppclass CColumnSource "deephaven::dhcore::column::ColumnSource":
        void FillChunk(const CRowSequence &rows, CChunk *destData,
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
        kChar "deephaven::dhcore::ElementTypeId::kChar"
        kInt8 "deephaven::dhcore::ElementTypeId::kInt8"
        kInt16 "deephaven::dhcore::ElementTypeId::kInt16"
        kInt32 "deephaven::dhcore::ElementTypeId::kInt32"
        kInt64 "deephaven::dhcore::ElementTypeId::kInt64"
        kFloat "deephaven::dhcore::ElementTypeId::kFloat"
        kDouble "deephaven::dhcore::ElementTypeId::kDouble"
        kBool "deephaven::dhcore::ElementTypeId::kBool"
        kString "deephaven::dhcore::ElementTypeId::kString"
        kTimestamp "deephaven::dhcore::ElementTypeId::kTimestamp"
        kLocalDate "deephaven::dhcore::ElementTypeId::kLocalDate"
        kLocalTime "deephaven::dhcore::ElementTypeId::kLocalTime"

    cdef cppclass CDateTime "deephaven::dhcore::DateTime":
        pass

    cdef cppclass CLocalDate "deephaven::dhcore::LocalDate":
        pass

    cdef cppclass CLocalTime "deephaven::dhcore::LocalTime":
        pass

cdef extern from "deephaven/dhcore/utility/cython_support.h" namespace "deephaven::dhcore::utility":
    cdef cppclass CCythonSupport "deephaven::dhcore::utility::CythonSupport":
        @staticmethod
        shared_ptr[CColumnSource] CreateBooleanColumnSource(const uint8_t *dataBegin, const uint8_t *dataEnd,
            const uint8_t *validityBegin, const uint8_t *validityEnd, size_t numElements)

        @staticmethod
        shared_ptr[CColumnSource] CreateStringColumnSource(const char *textBegin, const char *textEnd,
            const uint32_t *offsetsBegin, const uint32_t *offsetsEnd,
            const uint8_t *validityBegin, const uint8_t *validityEnd, size_t numElements)

        @staticmethod
        shared_ptr[CColumnSource] CreateDateTimeColumnSource(const int64_t *dataBegin, const int64_t *dataEnd,
            const uint8_t *validityBegin, const uint8_t *validityEnd, size_t numElements)

        @staticmethod
        shared_ptr[CColumnSource] CreateLocalDateColumnSource(const int64_t *dataBegin, const int64_t *dataEnd,
            const uint8_t *validityBegin, const uint8_t *validityEnd, size_t numElements)

        @staticmethod
        shared_ptr[CColumnSource] CreateLocalTimeColumnSource(const int64_t *dataBegin, const int64_t *dataEnd,
            const uint8_t *validityBegin, const uint8_t *validityEnd, size_t numElements)

        @staticmethod
        ElementTypeId GetElementTypeId(const CColumnSource &columnSource)

cdef extern from "deephaven/dhcore/ticking/barrage_processor.h" namespace "deephaven::dhcore::ticking":
    cdef cppclass CBarrageProcessor "deephaven::dhcore::ticking::BarrageProcessor":
        @staticmethod
        string CreateSubscriptionRequestCython(const void *ticket_bytes, size_t size)

        CBarrageProcessor()
        CBarrageProcessor(shared_ptr[CSchema] schema)

        optional[CTickingUpdate] ProcessNextChunk(const vector[shared_ptr[CColumnSource]] &sources,
            const vector[size_t] &sizes, void *metadata, size_t metadataSize) except +
