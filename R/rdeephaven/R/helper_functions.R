# TODO: internal documentation

################ FUNCTIONS TO GET FROM DH TO R

dh_to_record_batch_reader <- function(tableHandle) {

    ptr <- tableHandle$.get_arrowArrayStream_ptr()

    # create R RecordBatchStreamReader from C++ ArrowArrayStream pointed to by ptr
    rbr <- RecordBatchStreamReader$import_from_c(ptr)
    return(rbr)
}

dh_to_arrow_table <- function(tableHandle) {

    # create R RecordBatchStreamReader
    rbr <- dh_to_record_batch_reader(tableHandle)

    # use RecordBatchStreamReader to get data as arrow table
    return(rbr$read_table())
}

dh_to_data_frame <- function(tableHandle) {

    arrow_table <- dh_to_arrow_table(tableHandle)
    return(as.data.frame(arrow_table))
}

################ FUNCTIONS TO GET FROM R TO DH

rbr_to_dh_table <- function(client, rbr, name) {
    ptr <- client$.new_arrowArrayStream_ptr()
    rbr$export_to_c(ptr)
    dh_table <- client$.new_table_from_arrowArrayStream_ptr(ptr, name)
    return(dh_table)
}

arrow_to_dh_table <- function(client, arrow_tbl, name) {
    rbr <- as_record_batch_reader(arrow_tbl)
    dh_table <- rbr_to_dh_table(client, rbr, name)
    return(dh_table)
}

df_to_dh_table <- function(client, data_frame, name) {
    arrow_tbl <- arrow_table(data_frame)
    dh_table <- arrow_to_dh_table(client, arrow_tbl, name)
    return(dh_table)
}