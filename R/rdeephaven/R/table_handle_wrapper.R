TableHandle <- R6Class("TableHandle",
    public = list(

        initialize = function(table_handle) {

            # first, type checking
            if (class(table_handle)[[1]] != "Rcpp_INTERNAL_TableHandle") {
                stop("'table_handle' should be an internal Deephaven TableHandle. If you're seeing this,
                you are trying to call the constructor of TableHandle directly, which is not advised.")
            }
            private$internal_table_handle <- table_handle
        },

        bind_to_variable = function(name) {
            if (class(name)[[1]] != "character") {
                stop("'name' should be a character or a string.")
            }
            private$internal_table_handle$bind_to_variable(name)
        },

        to_arrow_record_batch_stream_reader = function() {
            ptr = private$internal_table_handle$get_arrow_array_stream_ptr()
            rbsr = RecordBatchStreamReader$import_from_c(ptr)
            return(rbsr)
        },

        to_arrow_table = function() {
            rbsr = self$to_arrow_record_batch_stream_reader()
            arrow_tbl = rbsr$read_table()
            return(arrow_tbl)
        },

        to_tibble = function() {
            rbsr = self$to_arrow_record_batch_stream_reader()
            arrow_tbl = rbsr$read_table()
            return(as_tibble(arrow_tbl))
        },

        to_data_frame = function() {
            arrow_tbl = self$to_arrow_table()
            return(as.data.frame(as.data.frame(arrow_tbl))) # for some reason as.data.frame on arrow table returns a tibble, not what I want
        }

    ),
    private = list(
        internal_table_handle = NULL
    )
)
