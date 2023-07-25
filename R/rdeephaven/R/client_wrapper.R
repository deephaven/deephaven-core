#' @title The Deephaven Client
#' @description The Deephaven Client class is responsible for establishing and maintaining
#' a connection to a running Deephaven server and facilitating basic server requests.
#' 
#' @usage NULL
#' @format NULL
#' @docType class
#'
#' @examples
#' 
#' # connect to the Deephaven server running on "localhost:10000" using anonymous 'default' authentication
#' client_options <- ClientOptions$new()
#' client <- Client$new(target="localhost:10000", client_options=client_options)
#' 
#' # open a table that already exists on the server
#' new_table_handle1 <- client$open_table("table_on_the_server")
#' 
#' # create a new dataframe, import onto the server, and retrieve a reference
#' new_data_frame <- data.frame(matrix(rnorm(10 * 1000), nrow = 10))
#' new_table_handle2 <- client$import_table(new_data_frame)
#' 
#' # run a python script on the server (default client options specify a Python console)
#' client$run_script("print([i for i in range(10)])")

#' @export
Client <- R6Class("Client", cloneable = FALSE,
    public = list(

        #' @description
        #' Connect to a running Deephaven server.
        #' @param target The address of the Deephaven server.
        #' @param client_options ClientOptions instance with the parameters needed to connect to the server.
        #' See ?ClientOptions for more information.
        initialize = function(target, client_options) {
            verify_string("target", target)
            if (first_class(client_options) != "ClientOptions") {
                stop(paste("'client_options' should be a Deephaven ClientOptions object. Got an object of type", first_class(client_options), "instead."))
            }
            private$internal_client <- new(INTERNAL_Client, target=target,
                                           client_options=client_options$internal_client_options)
        },

        #' @description
        #' Opens a table named 'name' from the server if it exists.
        #' @param name Name of the table to open from the server, passed as a string.
        #' @return TableHandle reference to the requested table.
        open_table = function(name) {
            verify_string("name", name)
            if (!private$check_for_table(name)) {
                stop(paste0("The table '", name, "' you're trying to pull does not exist on the server."))
            }
            return(TableHandle$new(private$internal_client$open_table(name)))
        },

        #' @description
        #' Creates a "zero-width" table on the server. Such a table knows its number of rows but has no columns.
        #' @param size Number of rows in the empty table.
        #' @return TableHandle reference to the new table, which has not yet been named on the server.
        #'         See TableHandle$bind_to_variable() for naming a new table on the server.
        empty_table = function(size) {
            verify_int("size", size)
            return(TableHandle$new(private$internal_client$empty_table(size)))
        },

        #' @description
        #' Creates a ticking table.
        #' @param start_time_nanos When the table should start ticking (in units of nanoseconds since the epoch).
        #' @param period_nanos Table ticking frequency (in nanoseconds).
        #' @return TableHandle reference to the new table, which has not yet been named on the server.
        #'         See TableHandle$bind_to_variable() for naming a new table on the server.
        time_table = function(start_time_nanos, period_nanos) {
            verify_int(start_time_nanos)
            verify_int(period_nanos)
            return(TableHandle$new(private$internal_client$time_table(start_time_nanos, period_nanos)))
        }

        #' @description
        #' Imports a new table to the Deephaven server. Note that this new table is not automatically bound to
        #' a variable name on the server. See `?TableHandle` for more information.
        #' @param table_object An R Data Frame, a dplyr Tibble, an Arrow Table, or an Arrow RecordBatchReader
        #' containing the data to import to the server.
        #' @return TableHandle reference to the new table.
        import_table = function(table_object) {
            table_object_class = class(table_object)
            if (table_object_class[[1]] == "data.frame") {
                return(TableHandle$new(private$df_to_dh_table(table_object)))
            }
            if (table_object_class[[1]] == "tbl_df") {
                return(TableHandle$new(private$tibble_to_dh_table(table_object)))
            }
            else if (table_object_class[[1]] == "RecordBatchReader") {
                return(TableHandle$new(private$rbr_to_dh_table(table_object)))
            }
            else if ((length(table_object_class) == 4 &&
                      table_object_class[[1]] == "Table" &&
                      table_object_class[[3]] == "ArrowObject")) {
                return(TableHandle$new(private$arrow_to_dh_table(table_object)))
            }
            else {
                stop(paste0("'table_object' must be either an R Data Frame, a dplyr Tibble, an Arrow Table, or an Arrow Record Batch Reader. Got an object of class ", table_object_class[[1]], " instead."))
            }
        },

        #' @description
        #' Runs a script on the server. The script must be in the language that the server console was started with.
        #' @param script Code to be executed on the server, passed as a string.
        run_script = function(script) {
            verify_string("script", script)
            private$internal_client$run_script(script)
        }
    ),
    private = list(

        check_for_table = function(name) {
            return(private$internal_client$check_for_table(name))
        },
        
        rbr_to_dh_table = function(rbr) {
            ptr = private$internal_client$new_arrow_array_stream_ptr()
            rbr$export_to_c(ptr)
            return(private$internal_client$new_table_from_arrow_array_stream_ptr(ptr))
        },

        arrow_to_dh_table = function(arrow_tbl) {
            rbr = as_record_batch_reader(arrow_tbl)
            return(private$rbr_to_dh_table(rbr))
        },

        tibble_to_dh_table = function(tibbl) {
            arrow_tbl = arrow_table(tibbl)
            return(private$arrow_to_dh_table(arrow_tbl))
        },

        df_to_dh_table = function(data_frame) {
            arrow_tbl = arrow_table(data_frame)
            return(private$arrow_to_dh_table(arrow_tbl))
        },

        internal_client = NULL
    )
)
