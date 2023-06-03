######################################## INCOMPLETE DOCS ########################################

#' @title The Deephaven Client
#' @description The Deephaven Client class is responsible for establishing and maintaining
#' a connection to a running Deephaven server and facilitating basic server requests. This is
#' the primary interface for interacting with the Deephaven server from R.
#' 
#' @section Connecting to the Server via Client
#' 
#' To connect to a Deephaven server, you must create a new DeephavenClient object via `Client$new(...)`,
#' with the following arguments:
#' 
#' - `target`: The URL that the Deephaven server is running on.
#' - `client_options`: A `ClientOptions` instance that contains the relevant information for connecting to your server.
#'   Check the documentation on client options via `?ClientOptions`
#' 
#' @section Methods
#' 
#' - `$open_table(name)`: Looks for a table named 'name' on the server, and returns a Deephaven TableHandle reference
#'    reference to that table if it exists. See the documentation on table handles via `?TableHandle` for more information.
#' - `$import_table(table_object)`: Imports a new table to the Deephaven server and returns a Deephaven TableHandle
#'    reference to the new table. Here, `table_object` can be an R Data Frame, an R Tibble, an Arrow Table, or an Arrow
#'    RecordBatchReader. Note that this new table is not automatically bound to a variable name on the server.
#'    See `?TableHandle` for more information.
#' - `run_script(script)`: Runs a script on the server. The script must be in the language that the server console was
#'    started with, and should be passed as a string to this method.
#' 
#' @examples
#' 
#' client_options <- ClientOptions$new()
#' client_options$set_default_authentication()
#' 
#' # connect to the Deephaven server running on "localhost:10000" with anonymous 'default' authentication
#' client <- Client$new(target="localhost:10000", client_options=client_options)
#' 
#' # open a table that already exists on the server
#' new_table_handle1 <- client$open_table("table_on_the_server")
#' 
#' # create a new dataframe, import onto the server, and retrieve a reference
#' new_data_frame <- data.frame(matrix(rnorm(10 * 1000), nrow = 10))
#' new_table_handle2 <- client$import_table(new_data_frame)
#' 
#' # run a python script, since default client options specify a Python console
#' client$run_script("print([i for i in range(10)])")

#################################################################################################


Client <- R6Class("Client",
    public = list(

        initialize = function(target, client_options) {
            private$internal_client <- new(INTERNAL_Client, target=target,
                                           client_options=client_options$internal_client_options)
        },

        open_table = function(name) {
            private$verify_string(name, "name")
            if (!private$check_for_table(name)) {
                stop("The table you're trying to pull does not exist on the server.")
            }
            return(TableHandle$new(private$internal_client$open_table(name)))
        },

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
                stop("'table_object' must be either an R Data Frame, an R Tibble, an Arrow Table, or an Arrow Record Batch Reader.")
            }
        },

        run_script = function(script) {
            private$verify_string(script, "script")
            private$internal_client$run_script(script)
        }
    ),
    private = list(

        internal_client = NULL,

        check_for_table = function(name) {
            return(private$internal_client$check_for_table(name))
        },

        verify_string = function(stringCandidate, arg_name) {
            if (class(stringCandidate) != "character") {
                stop(paste0("'", arg_name, "' must be passed as a string."))
            }
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
        }
    )
)