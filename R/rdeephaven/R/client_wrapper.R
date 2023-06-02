######################################## INCOMPLETE DOCS ########################################

#' @title The Deephaven Client
#' @description The DeephavenClient class is responsible for establishing and maintaining
#' a connection to a running Deephaven server and facilitating basic server requests. This is
#' the primary interface for interacting with the Deephaven server from R.
#' 
#' @section Connecting to the Server via Client
#' 
#' To connect to a Deephaven server, you must create a new DeephavenClient object via `DeephavenClient$new(...)`,
#' with the following arguments:
#' 
#' - `target`: The URL that the Deephaven server is running on. This argument is NOT OPTIONAL.
#' - `session_type`: If you started a Python or Groovy server, you can start up a corresponding
#'    console here with "python" or "groovy". Defaults to Python.
#' - `auth_type`: The type of authentication your server is using. Can be "default", "basic", or "custom".
#'    Defaults to "default".
#' - `key`: The key credential in a key/value pair. For basic authentication, this is a username.
#'    For custom authentication, it is a general key. For default (anonymous) authentication, use "".
#'    Defaults to "".
#' - `value`: The value credential in a key/value pair. For basic authentication, this is a password.
#'    For custom authentication, it is a  general value. For default (anonymous) authentication, use "".
#' 
#' It is important that every one of these arguments be provided explicitly. We will provide optional and
#' default arguments in a coming release, but for the moment we require the user to provide all arguments.
#' Once the server connection has been established, you have a handful of key methods to handle basic server
#' requests.
#' 
#' @section Methods
#' 
#' - `$open_table(name)`: Looks for a table named 'name' on the server, and returns a Deephaven TableHandle
#'    reference to that table if it exists.
#' - `$delete_table(name)`: Looks for a table named 'name' on the server, and deletes it if it exists.
#'    Importantly, this is only effective 
#' - `check_for_table`: 
#' - `run_script`:

#################################################################################################



DeephavenClient <- R6Class("DeephavenClient",
    public = list(

        initialize = function(target, session_type="python", auth_type="default", credentials=list(key="", value="")) {

            # first, type checking
            private$verify_string(target, "target")
            private$verify_string(session_type, "session_type")
            private$verify_string(auth_type, "auth_type")

            if (!(class(credentials) == "list" && length(credentials) == 2 &&
             class(credentials[[1]]) == "character" && class(credentials[[2]]) == "character")) {
                stop("'credentials' must be passed as a list of two strings representing a key/value pair for authentication.")
            }

            # now value checking
            if (!(session_type == "python" || session_type == "groovy")) {
                warning("The session type you provided is not currently supported. Defaulting to 'python'.")
            }
            if (!(auth_type == "default" || auth_type == "basic" || auth_type == "custom")) {
                warning("The authentication type you provided is not currently supported. Defaulting to 'default'.")
            }

            private$internal_client <- new(INTERNAL_Client, target=target, session_type=session_type,
                                           auth_type=auth_type, key=credentials[[1]], value=credentials[[2]])
        },

        # retrieve a TableHandle reference to a named table on the server
        open_table = function(name) {
            private$verify_string(name, "name")
            if (!private$check_for_table(name)) {
                stop("The table you're trying to pull does not exist on the server.")
            }
            return(TableHandle$new(private$internal_client$open_table(name)))
        },

        # import an R object to a table to the server, and retrieve a TableHandle reference to that table
        import_table = function(table_object) {
            table_object_class = class(table_object)
            if (!(table_object_class[[1]] == "data.frame" ||
                  table_object_class[[1]] == "RecordBatchReader" ||
                  (length(table_object_class) == 3 &&
                   table_object_class[[1]] == "tbl_df") ||
                  (length(table_object_class) == 4 &&
                   table_object_class[[1]] == "Table" &&
                   table_object_class[[3]] == "ArrowObject"))) {
                stop("'table_object' must be either an R Data Frame, an R Tibble, an Arrow Table, or an Arrow Record Batch Reader.")
            }
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
                stop("An unexpected type was passed to 'import_table' and made it through our error handling. 'table_object' must be either an R Data Frame, an Arrow Table, or an Arrow Record Batch Reader.")
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
            dh_table = private$internal_client$new_table_from_arrow_array_stream_ptr(ptr)
            return(dh_table)
        },

        arrow_to_dh_table = function(arrow_tbl) {
            rbr = as_record_batch_reader(arrow_tbl)
            dh_table = private$rbr_to_dh_table(rbr)
            return(dh_table)
        },

        tibble_to_dh_table = function(tibbl) {
            arrow_tbl = arrow_table(tibbl)
            dh_table = private$arrow_to_dh_table(arrow_tbl)
            return(dh_table)
        },

        df_to_dh_table = function(data_frame) {
            arrow_tbl = arrow_table(data_frame)
            dh_table = private$arrow_to_dh_table(arrow_tbl)
            return(dh_table)
        }
    )
)