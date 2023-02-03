# distutils: language = c++
# cython: language_level = 3
# cython: cpp_locals=True

from cdeephaven_client cimport CClient, CColumnSource, CGenericChunk, CRowSequence
from cdeephaven_client cimport CSubscriptionHandle, CTable, CTableHandle
from cdeephaven_client cimport CTableHandleManager, CTickingUpdate
from cdeephaven_client cimport HumanReadableElementTypeName, HumanReadableStaticTypeName
from cython.operator cimport dereference as deref
from libc.stdint cimport int8_t, int16_t, int32_t, int64_t
from libcpp cimport bool
from libcpp.memory cimport shared_ptr, unique_ptr
from libcpp.string cimport string
from libcpp.utility cimport move
from libcpp.vector cimport vector

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

    def getManager(self) -> TableHandleManager:
        result = TableHandleManager()
        result.manager = self.client.getManager()
        return result

# Simple wrapper of the corresponding C++ TableHandleManager class.
cdef class TableHandleManager:
    cdef CTableHandleManager manager

    def fetchTable(self, tableName: str) -> TableHandle:
        bytes = tableName.encode()
        tableHandle = self.manager.fetchTable(bytes)
        return TableHandle.create(move(tableHandle))

    def emptyTable(self, numRows: size_t) -> TableHandle:
        tableHandle = self.manager.emptyTable(numRows)
        return TableHandle.create(move(tableHandle))

    def timeTable(self, start: size_t, nanos: size_t) -> TableHandle:
        tableHandle = self.manager.timeTable(start, nanos)
        return TableHandle.create(move(tableHandle))

# Python trampoline functions. By the time we get to this point, the
# user-supplied Python callback is in a void*. We cast it back to a Python
# object and invoke its onTick method.
cdef void onTickCallback(CTickingUpdate update, void *userData) with gil:
    ptu = TickingUpdate.create(move(update))
    (<object>userData).onTick(ptu)

# Python trampoline functions. By the time we get to this point, the
# user-supplied Python callback is in a void*. We cast it back to a Python
# object and invoke its onError method.
cdef void onErrorCallback(string error, void *userData) with gil:
    (<object>userData).onError(error)

cdef checkHasMethod(o, methodName: str):
    if not callable(getattr(o, methodName, None)):
        raise RuntimeError(
            f"Provided callback does not have an {methodName}() method")

# Wrapper of the corresponding C++ TableHandle class.
cdef class TableHandle:
    cdef CTableHandle tableHandle

    @staticmethod
    cdef TableHandle create(CTableHandle tableHandle):
        result = TableHandle()
        result.tableHandle = tableHandle
        return result
	
    cpdef update(self, columnSpecs: [str]):
        cdef vector[string] cColumnSpecs
        for item in columnSpecs:
            encoded = item.encode()
            cColumnSpecs.push_back(encoded)
        result = self.tableHandle.update(move(cColumnSpecs))
        return TableHandle.create(move(result))

    def tail(self, size_t numRows):
        result = self.tableHandle.tail(numRows)
        return TableHandle.create(move(result))

    def toString(self, wantHeaders: bool) -> str:
        result = self.tableHandle.toString(wantHeaders)
        return result.decode()
    
    # 'callback' is a Python object that defines an onTick and onError method,
    # like so
    #
    # class MyCallback:
    #     def onTick(self, update: dh.TickingUpdate):
    #         ...
    #     def onError(self, message: str)
    #         ...
    def subscribe(self, callback) -> SubscriptionOuterHandle:
        checkHasMethod(callback, "onTick")
        checkHasMethod(callback, "onError")
        
        subHandle = self.tableHandle.subscribe(onTickCallback, <void*>callback,
            onErrorCallback, <void*>callback)
        # Take special care to hold our own a reference to 'callback' so it
        # doesn't deallocated before the C++ library is done using it
        inner = SubscriptionInnerHandle.create(self.tableHandle, callback,
            subHandle)
        return SubscriptionOuterHandle(inner)

    def unsubscribe(self, outerHandle: SubscriptionOuterHandle):
        outerHandle.innerHandle.shutdown()

# This is a normal Python class that exists solely to have a finalizer that
# calls shutdown on the inner class that it owns.
class SubscriptionOuterHandle:
    def __init__(self, innerHandle):
        self.innerHandle = innerHandle

    def __del__(self):
        self.innerHandle.shutdown()

# Objects of this class are typically owned by SubscriptionOuterHandle
cdef class SubscriptionInnerHandle:
    # The C++ TableHandle. We need this so we can unsubscribe.
    cdef CTableHandle tableHandle
    # The caller's callback. We hold a reference here so it is not destroyed
    # until such time as we unsubscribe.
    cdef object callback
    # The C++ subcriptionHandle. Needed as a parameter to unsubcribe.
    cdef shared_ptr[CSubscriptionHandle] subscriptionHandle

    @staticmethod
    cdef SubscriptionInnerHandle create(CTableHandle tableHandle,
        object callback, shared_ptr[CSubscriptionHandle] subscriptionHandle):
        result = SubscriptionInnerHandle()
        result.tableHandle = move(tableHandle)
        result.callback = callback
        result.subscriptionHandle = move(subscriptionHandle)
        return result;

    def shutdown(self):
        self.tableHandle.unsubscribe(self.subscriptionHandle)

# Simple wrapper of the corresponding C++ TickingUpdate class.
cdef class TickingUpdate:
    cdef CTickingUpdate tickingUpdate

    @staticmethod
    cdef TickingUpdate create(CTickingUpdate update):
        result = TickingUpdate()
        result.tickingUpdate = move(update)
        return result;

    @property
    def prev(self) -> Table:
        return Table.create(self.tickingUpdate.prev())

    @property
    def beforeRemoves(self) -> Table:
        return Table.create(self.tickingUpdate.beforeRemoves())

    @property
    def removedRows(self) -> RowSequence:
        return RowSequence.create(self.tickingUpdate.removedRows())

    @property
    def afterRemoves(self) -> Table:
        return Table.create(self.tickingUpdate.afterRemoves())

    @property
    def beforeAdds(self) -> Table:
        return Table.create(self.tickingUpdate.beforeAdds())

    @property
    def addedRows(self) -> RowSequence:
        return RowSequence.create(self.tickingUpdate.addedRows())

    @property
    def afterAdds(self) -> Table:
        return Table.create(self.tickingUpdate.afterAdds())

    @property
    def beforeModifies(self) -> Table:
        return Table.create(self.tickingUpdate.beforeModifies())

    @property
    def afterModifies(self) -> Table:
        return Table.create(self.tickingUpdate.afterModifies())

    @property
    def modifiedRows(self) -> [RowSequence]:
        result = []
        modRows = self.tickingUpdate.modifiedRows()
        for i in range(modRows.size()):
            result.append(RowSequence.create(modRows[i]))  
        return result

    @property
    def current(self) -> Table:
        return Table.create(self.tickingUpdate.current())

# Simple wrapper of the corresponding C++ Table class.
cdef class Table:
    cdef shared_ptr[CTable] table

    @staticmethod
    cdef Table create(shared_ptr[CTable] table):
        result = Table()
        result.table = move(table)
        return result

    def getColumn(self, columnIndex: size_t) -> ColumnSource:
        cs = deref(self.table).getColumn(columnIndex)
        return ColumnSource.create(move(cs))

    def getColumnByName(self, name: str, strict: bool) -> ColumnSource:
        nameBytes = name.encode()
        result = deref(self.table).getColumn(nameBytes, strict)
        if (result == NULL):
            return None
        return ColumnSource.create(move(result))

    def getColumnIndex(self, name: unicode, strict: bool) -> size_t:
        nameAsString = <string>name.encode()
        return deref(self.table).getColumnIndex(nameAsString, strict)

    def getRowSequence(self) -> RowSequence:
        result = deref(self.table).getRowSequence()
        return RowSequence.create(move(result))
        
    @property
    def numRows(self) -> size_t:
        return deref(self.table).numRows()

    @property
    def numColumns(self) -> size_t:
        return deref(self.table).numColumns()

    def toString(self, wantHeaders: bool, wantRowNumbers: bool, rowSequence = None) -> str:
        if rowSequence is None:
            result = deref(self.table).toString(wantHeaders, wantRowNumbers)
        elif isinstance(rowSequence, list):
            print("TODO")
        elif isinstance(rowSequence, RowSequence):
            result = deref(self.table).toString(wantHeaders, wantRowNumbers, (<RowSequence>rowSequence).rowSequence)
        return result.decode()
    
# Simple wrapper of the corresponding C++ RowSequence class.
cdef class RowSequence:
    cdef shared_ptr[CRowSequence] rowSequence

    @staticmethod
    cdef RowSequence create(shared_ptr[CRowSequence] rowSequence):
        result = RowSequence()
        result.rowSequence = move(rowSequence)
        return result;

    def take(self, size: size_t) -> RowSequence:
        rowSequence = deref(self.rowSequence).take(size)
        return RowSequence.create(move(rowSequence))

    def drop(self, size: size_t) -> RowSequence:
        rowSequence = deref(self.rowSequence).drop(size)
        return RowSequence.create(move(rowSequence))

    @property
    def size(self) -> size_t:
        return deref(self.rowSequence).size()
	
    @property
    def empty(self) -> bool:
        return deref(self.rowSequence).empty()

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

# Confirm that the element type of destData is the same as the element type of Column Source.
# Technically we don't need to do this check, as the C++ library has its own error checking,
# but this gives us an opportunity to provide a more friendly error message here.
cdef checkCompatibility(const CColumnSource &columnSource,
    column_source_element_type_t[::1] destData):
    cdef const char *csName = HumanReadableElementTypeName.getName(columnSource)
    cdef const char *ddNameForComparison = NULL

    if column_source_element_type_t is object:
        # "object" is a special case. If the user passes us an array of objects, then
        # the user expects the column source to be of type string. So the name for comparison
        # is "string" but (if they don't match) the name we put in the error message will be
        # "object".
        ddNameForComparison = HumanReadableStaticTypeName[string].getName()
        ddNameForRendering = "object"
    else:
        ddNameForComparison = HumanReadableStaticTypeName[column_source_element_type_t].getName()
        ddNameForRendering = ddNameForComparison.decode()

    if csName != ddNameForComparison:
        csNameForRendering = csName.decode()
        raise RuntimeError(f"columnSource has element type {csNameForRendering} but caller buffer has element type {ddNameForRendering}")

# Wrapper of the corresponding C++ ColumnSource class. Does a little bit of
# extra work to support the fillChunk method.
cdef class ColumnSource:
    cdef shared_ptr[CColumnSource] columnSource

    @staticmethod
    cdef ColumnSource create(shared_ptr[CColumnSource] columnSource):
        result = ColumnSource()
        result.columnSource = move(columnSource)
        return result;

    # The fillChunk method uses a Cython "fused type" which is sort of like a C++ template with
    # explicit template instantiation. 'destData' is a memory view of some caller data structure
    # (perhaps a NumPy array or a compact array created by the Python array module) whose element
    # type is one of the types defined by column_source_element_type_t: int8_t, int16_t, etc.
    cpdef fillChunk(self, rows: RowSequence,
        column_source_element_type_t[::1] destData,
        bool[::1] optionalDestNullFlags):

        checkCompatibility(deref(self.columnSource), destData)        
        
        rsSize = rows.size
        destSize = destData.shape[0]

        if (destSize < rsSize):
            raise RuntimeError(f"size of destData {destSize} < row sequence size {rsSize}")

        if optionalDestNullFlags is not None:
            boolSize = optionalDestNullFlags.shape[0]
            if (boolSize < rsSize):
                raise RuntimeError(
                    f"size of optionalDestNullFlags {boolSize} < row sequence size {rsSize}")

        # get a pointer to the dest null flags, if the parameter is not None
        cdef CGenericChunk[bool] optionalBooleanChunk
        cdef CGenericChunk[bool] *nullFlagsPtr = NULL
        if optionalDestNullFlags is not None:
            optionalBooleanChunk = CGenericChunk[bool].createView(&optionalDestNullFlags[0], rsSize)
            nullFlagsPtr = &optionalBooleanChunk

        if column_source_element_type_t is object:
            self.fillStringChunk(rows, destData, nullFlagsPtr)
        else:
            self.fillOtherChunk(rows, destData, nullFlagsPtr)

    # Helper method for fillChunk when the destData element type is "object" (and the ColumnSource
    # is a string type). In this case we have to allocate a temporary chunk of strings, fill it, and
    # then copy the data out of it to Python strings.
    cdef fillStringChunk(self, rows: RowSequence, object[::1] destData,
        CGenericChunk[bool] *nullFlagsPtr): 
        rsSize = rows.size
        destChunk = CGenericChunk[string].create(rsSize)
        deref(self.columnSource).fillChunk(deref(rows.rowSequence), &destChunk, nullFlagsPtr)
        i: ssize_t = 0
        for i in range(rsSize):
            destData[i] = destChunk.data()[i]

    # Helper method for fillChunk when the destData element type is any type except "object".
    # In this case we point a Chunk directly to the destData buffer, and then the C++ library
    # fills that chunk directly.
    cdef fillOtherChunk(self, rows: RowSequence,
        column_source_primitive_element_type_t[::1] destData,
        CGenericChunk[bool] *nullFlagsPtr):
        rsSize = rows.size
        dataChunk = CGenericChunk[column_source_primitive_element_type_t].createView(
	    &destData[0], rsSize)
        deref(self.columnSource).fillChunk(deref(rows.rowSequence), &dataChunk, nullFlagsPtr)
