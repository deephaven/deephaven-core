#' @title Deephaven TableHandles
#' @description Deephaven TableHandles are references to tables living on a Deephaven server. They provide an
#' interface for interacting with tables on the server.
#'
#' @usage NULL
#' @format NULL
#' @docType class
#'
#' @examples
#'
#' # connect to the Deephaven server running on "localhost:10000" using anonymous 'default' authentication
#' client_options <- ClientOptions$new()
#' client <- Client$new(target = "localhost:10000", client_options = client_options)
#'
#' # open a table that already exists on the server
#' new_table_handle1 <- client$open_table("table_on_the_server")
#'
#' # convert the Deephaven table to an R data frame
#' new_data_frame <- new_table_handle1$to_data_frame()
#'
#' # modify new data frame in R
#' new_data_frame$New_Int_Col <- c(1, 2, 3, 4, 5)
#' new_data_frame$New_String_Col <- c("I", "am", "a", "string", "column")
#'
#' # push new data frame to the server and name it "new_table"
#' new_table_handle2 <- client$import_table(new_data_frame)
#' new_table_handle2$bind_to_variable("new_table")
TableHandle <- R6Class("TableHandle",
  public = list(
    initialize = function(table_handle) {
      if (class(table_handle)[[1]] != "Rcpp_INTERNAL_TableHandle") {
        stop("'table_handle' should be an internal Deephaven TableHandle. If you're seeing this,
                you are trying to call the constructor of TableHandle directly, which is not advised.")
      }
      private$internal_table_handle <- table_handle
      private$is_static_field <- private$internal_table_handle$is_static()
    },

    #' @description
    #' Whether the table referenced by this TableHandle is static or not.
    #' @return TRUE if the table is static, or FALSE if the table is ticking.
    is_static = function() {
      return(private$is_static_field)
    },

    #' @description
    #' Number of rows in the table referenced by this TableHandle, currently only implemented for static tables.
    #' @return The number of rows in the table.
    nrow = function() {
      if (!private$is_static_field) {
        stop("The number of rows is not yet supported for dynamic tables.")
      }
      return(private$internal_table_handle$num_rows())
    },

    #' @description
    #' Binds the table referenced by this TableHandle to a variable on the server,
    #' enabling it to be accessed by that name from any Deephaven API.
    #' @param name Name for this table on the server.
    bind_to_variable = function(name) {
      .verify_string("name", name)
      private$internal_table_handle$bind_to_variable(name)
    },

    #' @description
    #' Imports the table referenced by this TableHandle into an Arrow RecordBatchStreamReader.
    #' @return A RecordBatchStreamReader containing the data from the table referenced by this TableHandle.
    to_arrow_record_batch_stream_reader = function() {
      ptr <- private$internal_table_handle$get_arrow_array_stream_ptr()
      rbsr <- RecordBatchStreamReader$import_from_c(ptr)
      return(rbsr)
    },

    #' @description
    #' Imports the table referenced by this TableHandle into an Arrow Table.
    #' @return A Table containing the data from the table referenced by this TableHandle.
    to_arrow_table = function() {
      rbsr <- self$to_arrow_record_batch_stream_reader()
      arrow_tbl <- rbsr$read_table()
      return(arrow_tbl)
    },

    #' @description
    #' Imports the table referenced by this TableHandle into a dplyr Tibble.
    #' @return A Tibble containing the data from the table referenced by this TableHandle.
    to_tibble = function() {
      rbsr <- self$to_arrow_record_batch_stream_reader()
      arrow_tbl <- rbsr$read_table()
      return(as_tibble(arrow_tbl))
    },

    #' @description
    #' Imports the table referenced by this TableHandle into an R Data Frame.
    #' @return A Data Frame containing the data from the table referenced by this TableHandle.
    to_data_frame = function() {
      arrow_tbl <- self$to_arrow_table()
      return(as.data.frame(as.data.frame(arrow_tbl))) # TODO: for some reason as.data.frame on arrow table returns a tibble, not a data frame
    }
  ),
  private = list(
    internal_table_handle = NULL,
    is_static_field = NULL
  )
)
