# distutils: language = c++
# cython: language_level = 3

from libc.stdint cimport int32_t
from libcpp cimport bool
from libcpp.memory cimport shared_ptr, unique_ptr
from libcpp.string cimport string
from libcpp.vector cimport vector

ctypedef void (*CCallback)(CTickingUpdate, void *)
ctypedef void (*cOnTickCallback_t)(CTickingUpdate, void *)
ctypedef void (*cOnErrorCallback_t)(string, void *)

cdef extern from "deephaven/client/client.h" namespace "deephaven::client":
    cdef cppclass CClient "deephaven::client::Client":
        @staticmethod
        CClient connect(string target) except +

        CClient()
        CClient(CClient &&other)

        CTableHandleManager getManager()

    cdef cppclass CTableHandleManager "deephaven::client::TableHandleManager":
        CTableHandleManager()
        CTableHandleManager(CTableHandleManager &&other)

        CTableHandle fetchTable(string tableName) except +
        CTableHandle emptyTable(size_t numRows) except +
        CTableHandle timeTable(size_t start, size_t nanos) except +

    cdef cppclass CTableHandle "deephaven::client::TableHandle":
        CTableHandle()
        CTableHandle(CTableHandle &&other)

        CTableHandle update(vector[string] columnSpecs) except +

        CTableHandle tail(size_t numRows) except +

        shared_ptr[CSubscriptionHandle] subscribe(cOnTickCallback_t onTick, void *onTickUserData, cOnErrorCallback_t onError, void *onErrorUserData) except +
        void unsubscribe(shared_ptr[CSubscriptionHandle] handle) except +

        string toString(bool wantHeaders) except +

    cdef cppclass CTickingUpdate "deephaven::client::TickingUpdate":
        CTickingUpdate()
        CTickingUpdate(CTickingUpdate &&other)

        shared_ptr[CTable] prev()
        shared_ptr[CTable] beforeRemoves()
        shared_ptr[CRowSequence] removedRows()
        shared_ptr[CTable] afterRemoves()
        shared_ptr[CTable] beforeAdds()
        shared_ptr[CRowSequence] addedRows()
        shared_ptr[CTable] afterAdds()
        shared_ptr[CTable] beforeModifies()
        vector[shared_ptr[CRowSequence]] modifiedRows()
        shared_ptr[CTable] afterModifies()
        shared_ptr[CTable] current()

    cdef cppclass CSubscriptionHandle "deephaven::client::subscription::SubscriptionHandle":
        pass
        
    cdef cppclass CTable "deephaven::client::table::Table":
        CTable()
        CTable(CTable &&other)

        shared_ptr[CColumnSource] getColumn(size_t colIndex) except +
        shared_ptr[CColumnSource] getColumn(string name, bool strict) except +
        size_t getColumnIndex(string name, bool strict) except +
        shared_ptr[CRowSequence] getRowSequence() except +

        size_t numRows()
        size_t numColumns()

        string toString(bool wantHeaders, wantRowNumbers) except +
        string toString(bool wantHeaders, wantRowNumbers, shared_ptr[CRowSequence] rowSequence) except +

    cdef cppclass CRowSequence "deephaven::client::container::RowSequence":
        CRowSequence()
        CRowSequence(CRowSequence &&other)

        shared_ptr[CRowSequence] take(size_t size)
        shared_ptr[CRowSequence] drop(size_t size)

        size_t size()
        bool empty()

    cdef cppclass CChunk "deephaven::client::chunk::Chunk":
        CChunk()
        CChunk(CChunk &&other)

    cdef cppclass CGenericChunk "deephaven::client::chunk::GenericChunk" [T] (CChunk):
        @staticmethod
        CGenericChunk[T] create(size_t size)

        @staticmethod
        CGenericChunk[T] createView(T *data, size_t size)

        const T *data()

cdef extern from "deephaven/client/column/column_source.h" namespace "deephaven::client::column":
    cdef cppclass CColumnSource "deephaven::client::column::ColumnSource":
        CColumnSource(CColumnSource &&other)

        void fillChunk(const CRowSequence &rows, CChunk *destData,
            CGenericChunk[bool] *optionalDestNullFlags) except +

cdef extern from "deephaven/client/column/column_source_helpers.h" namespace "deephaven::client::column":
    cdef cppclass HumanReadableElementTypeName:
        @staticmethod
        const char *getName(const CColumnSource &columnSource)

cdef extern from "deephaven/client/column/column_source_helpers.h" namespace "deephaven::client::column":
    cdef cppclass HumanReadableStaticTypeName [T]:
        @staticmethod
        const char *getName()
