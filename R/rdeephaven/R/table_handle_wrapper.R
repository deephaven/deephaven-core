NewTableHandle <- R6Class("TableHandle",
    public = list(

        initialize = function(dhTableHandle) {

            # first, type checking
            if (class(dhTableHandle)[[1]] != "Rcpp_INTERNAL_TableHandle") {
                stop("'dhTableHandle' should be an internal Deephaven TableHandle. If you're seeing this,
                you are trying to call the constructor of TableHandle directly, which is not advised.")
            }
            private$internal_table_handle <- dhTableHandle
        },

        bind_to_variable = function(newTableName) {
            if (class(newTableName)[[1]] != "character") {
                stop("'newTableName' should be a character or a string.")
            }
            private$internal_table_handle$bind_to_variable(newTableName)
        },

        to_record_batch_stream_reader = function() {
            ptr = private$internal_table_handle$get_arrowArrayStream_ptr()
            rbsr = RecordBatchStreamReader$import_from_c(ptr)
            return(rbsr)
        },

        to_arrow_table = function() {
            rbsr = self$to_record_batch_stream_reader()
            arrow_tbl = rbsr$read_table()
            return(arrow_tbl)
        },

        to_data_frame = function() {
            arrow_tbl = self$to_arrow_table()
            data_frame = as.data.frame(as.data.frame(arrow_tbl))
            return(data_frame)
        }

    ),
    private = list(

        internal_table_handle = NULL

    )
)