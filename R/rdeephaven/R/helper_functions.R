# TODO: internal documentation

to_record_batch_reader <- function(tableHandle) {

    ptr <- tableHandle$.get_arrowArrayStream_ptr()

    # create R RecordBatchStreamReader from C++ ArrowArrayStream pointed to by ptr
    rbr <- RecordBatchStreamReader$import_from_c(ptr)
    return(rbr)
}

to_arrow_table <- function(tableHandle) {

    # create R RecordBatchStreamReader
    rbr <- to_record_batch_reader(tableHandle)

    # use RecordBatchStreamReader to get data as arrow table
    return(rbr$read_table())
}

to_data_frame <- function(tableHandle) {

    arrow_table <- to_arrow_table(tableHandle)
    return(as.data.frame(arrow_table))
}