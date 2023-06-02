################ FUNCTIONS TO GET FROM DH TO R

#' @title Convert a Deephaven TableHandle to an Arrow RecordBatchStreamReader
#' @description Converts a Deephaven TableHandle to an [Arrow RecordBatchStreamReader](https://arrow.apache.org/docs/r/reference/RecordBatchReader.html),
#' which can itself be used to produce Arrow tables and R data frames.
#' 
#' @param table_handle A Deephaven TableHandle.
#' @returns An Arrow RecordBatchStreamReader.
dh_to_rbs_reader <- function(table_handle) {

    if (class(table_handle)[[1]] != "Rcpp_TableHandle") {
        stop("Error: 'table_handle' must be a Deephaven TableHandle object. Check ?TableHandle for more information.")
    }

    ptr <- table_handle$.get_arrowArrayStream_ptr()

    # create R RecordBatchStreamReader from C++ ArrowArrayStream pointed to by ptr
    rbr <- RecordBatchStreamReader$import_from_c(ptr)
    return(rbr)
}

#' @title Convert a Deephaven TableHandle to an Arrow Table.
#' @description Converts a Deephaven TableHandle to an [Arrow Table](https://arrow.apache.org/docs/r/reference/Table.html),
#' R data frames.
#' 
#' @param table_handle A Deephaven TableHandle.
#' @returns An Arrow Table.
dh_to_arrow_table <- function(table_handle) {

    if (class(table_handle)[[1]] != "Rcpp_TableHandle") {
        stop("Error: 'table_handle' must be a Deephaven TableHandle object. Check ?TableHandle for more information.")
    }

    # create R RecordBatchStreamReader
    rbsr <- dh_to_rbs_reader(table_handle)

    # use RecordBatchStreamReader to get data as arrow table
    return(rbsr$read_table())
}

#' @title Convert a Deephaven TableHandle to an R Data Frame.
#' @description Converts a Deephaven TableHandle to an R Data Frame.
#' This is the recommended method for converting a Deephaven TableHandle to something that is usable in R.
#' 
#' @param table_handle A Deephaven TableHandle.
#' @returns An R Data Frame.
dh_to_data_frame <- function(table_handle) {

    if (class(table_handle)[[1]] != "Rcpp_TableHandle") {
        stop("Error: 'table_handle' must be a Deephaven TableHandle object. Check ?TableHandle for more information.")
    }

    arrow_table <- dh_to_arrow_table(table_handle)
    return(as.data.frame(as.data.frame(arrow_table))) # need as.data.frame twice because first only gives tibble?
}

################ FUNCTIONS TO GET FROM R TO DH

#' @title Push an Arrow RecordBatchReader to the server as a Deephaven Table.
#' @description Converts an Arrow RecordBatchReader to a Deephaven Table, pushes that new table to the Deephaven server,
#' and returns a TableHandle corresponding to the new table.
#' 
#' @param client A Deephaven Client object maintaining the connection to the server.
#' @param rbr An Arrow RecordBatchReader containing the data to be converted to a Deephaven Table.
#' @param name A string containing the name of the new Deephaven Table to be created on the server.
#' @returns A Deephaven TableHandle, providing a reference to the new table on the server.
rbr_to_dh_table <- function(client, rbr, name) {

    if (class(client)[[1]] != "Rcpp_Client") {
        stop("Error: 'client' must be a Deephaven Client object. Check ?Client for more information.")
    }
    if (class(rbr)[[1]] != "RecordBatchReader") {
        stop("Error: 'rbr' must be an Arrow RecordBatchReader.")
    }
    if (class(name)[[1]] != "character") {
        stop("Error: 'name' must be a character or a string.")
    }

    ptr <- client$.new_arrowArrayStream_ptr()
    rbr$export_to_c(ptr)
    dh_table <- client$.new_table_from_arrowArrayStream_ptr(ptr, name)
    return(dh_table)
}

#' @title Push an Arrow Table to the server as a Deephaven Table.
#' @description Converts an Arrow Table to a Deephaven Table, pushes that new table to the Deephaven server,
#' and returns a TableHandle corresponding to the new table.
#' 
#' @param client A Deephaven Client object maintaining the connection to the server.
#' @param arrow_tbl An Arrow Table containing the data to be converted to a Deephaven Table.
#' @param name A string containing the name of the new Deephaven Table to be created on the server.
#' @returns A Deephaven TableHandle, providing a reference to the new table on the server.
arrow_to_dh_table <- function(client, arrow_tbl, name) {

    if (class(client)[[1]] != "Rcpp_Client") {
        stop("Error: 'client' must be a Deephaven Client object. Check ?Client for more information.")
    }
    if (class(arrow_tbl)[[1]] != "Table") {
        stop("Error: 'arrow_tbl' must be an Arrow Table.")
    }
    if (class(name)[[1]] != "character") {
        stop("Error: 'name' must be a character or a string.")
    }

    rbr <- as_record_batch_reader(arrow_tbl)
    dh_table <- rbr_to_dh_table(client, rbr, name)
    return(dh_table)
}

#' @title Push an R Data Frame to the server as a Deephaven Table.
#' @description Converts an R Data Frame to a Deephaven Table, pushes that new table to the Deephaven server,
#' and returns a TableHandle corresponding to the new table.
#' 
#' @param client A Deephaven Client object maintaining the connection to the server.
#' @param data_frame An R Data Frame containing the data to be converted to a Deephaven Table.
#' @param name A string containing the name of the new Deephaven Table to be created on the server.
#' @returns A Deephaven TableHandle, providing a reference to the new table on the server.
df_to_dh_table <- function(client, data_frame, name) {
    
    if (class(client)[[1]] != "Rcpp_Client") {
        stop("Error: 'client' must be a Deephaven Client object. Check ?Client for more information.")
    }
    if (class(data_frame)[[1]] != "data.frame") {
        stop("Error: 'data_frame' must be an R Data Frame.")
    }
    if (class(name)[[1]] != "character") {
        stop("Error: 'name' must be a character or a string.")
    }

    arrow_tbl <- arrow_table(data_frame)
    dh_table <- arrow_to_dh_table(client, arrow_tbl, name)
    return(dh_table)
}
