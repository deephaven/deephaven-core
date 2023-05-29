to_arrow_table <- function(tableHandle, record_batch_reader=FALSE) {

    # first, we retrieve a pointer to the underlying C struct
    ptr <- tableHandle$.get_stream()

    # create recordBatchReader with arrow library
    rbr <- RecordBatchStreamReader$import_from_c(ptr)

    # return recordBatchReader if desired, otherwise return arrow table
    if (record_batch_reader) {
        return(rbr)
    }
    return(rbr$read_table())

}