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
#' client <- Client$new(target="localhost:10000", client_options=client_options)
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
            self$internal_table_handle <- table_handle
            private$is_static_field <- self$internal_table_handle$is_static()
        },

        select = function(columns) {
            return(TableHandle$new(self$internal_table_handle$select(columns)))
        },

        view = function(columns) {
            return(TableHandle$new(self$internal_table_handle$view(columns)))
        },

        drop_columns = function(columns) {
            return(TableHandle$new(self$internal_table_handle$drop_columns(columns)))
        },

        update = function(columns) {
            return(TableHandle$new(self$internal_table_handle$update(columns)))
        },

        update_view = function(columns) {
            return(TableHandle$new(self$internal_table_handle$update_view(columns)))
        },

        where = function(condition) {
            return(TableHandle$new(self$internal_table_handle$where(condition)))
        },

        # TODO: sort = function(sort_pairs) {
        #    return(TableHandle$new(self$internal_table_handle$sort(sort_pairs)))
        #},

        by = function(columns) {
            return(TableHandle$new(self$internal_table_handle$by(columns)))
        },

        min_by = function(columns) {
            return(TableHandle$new(self$internal_table_handle$min_by(columns)))
        },

        max_by = function(columns) {
            return(TableHandle$new(self$internal_table_handle$max_by(columns)))
        },

        sum_by = function(columns) {
            return(TableHandle$new(self$internal_table_handle$sum_by(columns)))
        },

        abs_sum_by = function(columns) {
            return(TableHandle$new(self$internal_table_handle$abs_sum_by(columns)))
        },

        var_by = function(columns) {
            return(TableHandle$new(self$internal_table_handle$var_by(columns)))
        },

        std_by = function(columns) {
            return(TableHandle$new(self$internal_table_handle$std_by(columns)))
        },

        avg_by = function(columns) {
            return(TableHandle$new(self$internal_table_handle$avg_by(columns)))
        },

        first_by = function(columns) {
            return(TableHandle$new(self$internal_table_handle$first_by(columns)))
        },

        last_by = function(columns) {
            return(TableHandle$new(self$internal_table_handle$last_by(columns)))
        },

        median_by = function(columns) {
            return(TableHandle$new(self$internal_table_handle$median_by(columns)))
        },

        percentile_by = function(percentile, columns) {
            return(TableHandle$new(self$internal_table_handle$percentile_by(percentile, columns)))
        },

        count_by = function(count_by_column, columns) {
            return(TableHandle$new(self$internal_table_handle$count_by(count_by_column, columns)))
        },

        w_avg_by = function(weight_column, columns) {
            return(TableHandle$new(self$internal_table_handle$w_avg_by(weight_column, columns)))
        },

        tail_by = function(n, columns) {
            return(TableHandle$new(self$internal_table_handle$tail_by(n, columns)))
        },

        head_by = function(n, columns) {
            return(TableHandle$new(self$internal_table_handle$head_by(n, columns)))
        },

        head = function(n) {
            return(TableHandle$new(self$internal_table_handle$head(n)))
        },

        tail = function(n) {
            return(TableHandle$new(self$internal_table_handle$tail(n)))
        },

        ungroup = function(null_fill, group_by_columns) {
            return(TableHandle$new(self$internal_table_handle$ungroup(null_fill, group_by_columns)))
        },

        #TODO: merge = function(key_column, sources) {
        #    return(TableHandle$new(self$internal_table_handle$merge(key_column, sources)))
        #},

        cross_join = function(right_side, columns_to_match, columns_to_add) {
            return(TableHandle$new(self$internal_table_handle$cross_join(right_side$internal_table_handle,
                                                                            columns_to_match, columns_to_add)))
        },

        natural_join = function(right_side, columns_to_match, columns_to_add) {
            return(TableHandle$new(self$internal_table_handle$natural_join(right_side$internal_table_handle,
                                                                              columns_to_match, columns_to_add)))
        },

        exact_join = function(right_side, columns_to_match, columns_to_add) {
            print(right_side)
            print(columns_to_match)
            print(columns_to_add)
            print(right_side$internal_table_handle)
            return(TableHandle$new(self$internal_table_handle$exact_join(right_side$internal_table_handle,
                                                                            columns_to_match, columns_to_add)))
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
            if(!private$is_static_field) {
                stop("The number of rows is not yet supported for dynamic tables.")
            }
            return(self$internal_table_handle$num_rows())
        },

        #' @description
        #' Binds the table referenced by this TableHandle to a variable on the server,
        #' enabling it to be accessed by that name from any Deephaven API.
        #' @param name Name for this table on the server.
        bind_to_variable = function(name) {
            .verify_string("name", name)
            self$internal_table_handle$bind_to_variable(name)
        },

        #' @description
        #' Imports the table referenced by this TableHandle into an Arrow RecordBatchStreamReader.
        #' @return A RecordBatchStreamReader containing the data from the table referenced by this TableHandle.
        to_arrow_record_batch_stream_reader = function() {
            ptr = self$internal_table_handle$get_arrow_array_stream_ptr()
            rbsr = RecordBatchStreamReader$import_from_c(ptr)
            return(rbsr)
        },

        #' @description
        #' Imports the table referenced by this TableHandle into an Arrow Table.
        #' @return A Table containing the data from the table referenced by this TableHandle.
        to_arrow_table = function() {
            rbsr = self$to_arrow_record_batch_stream_reader()
            arrow_tbl = rbsr$read_table()
            return(arrow_tbl)
        },

        #' @description
        #' Imports the table referenced by this TableHandle into a dplyr Tibble.
        #' @return A Tibble containing the data from the table referenced by this TableHandle.
        to_tibble = function() {
            rbsr = self$to_arrow_record_batch_stream_reader()
            arrow_tbl = rbsr$read_table()
            return(as_tibble(arrow_tbl))
        },

        #' @description
        #' Imports the table referenced by this TableHandle into an R Data Frame.
        #' @return A Data Frame containing the data from the table referenced by this TableHandle.
        to_data_frame = function() {
            arrow_tbl = self$to_arrow_table()
            return(as.data.frame(as.data.frame(arrow_tbl))) # TODO: for some reason as.data.frame on arrow table returns a tibble, not a data frame
        },
        
        internal_table_handle = NULL
    ),
    private = list(
        is_static_field = NULL
    )
)
