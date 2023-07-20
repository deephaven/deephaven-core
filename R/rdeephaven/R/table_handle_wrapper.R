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

#' @export
TableHandle <- R6Class("TableHandle",
    public = list(

        initialize = function(table_handle) {
            if (first_class(table_handle) != "Rcpp_INTERNAL_TableHandle") {
                stop("'table_handle' should be an internal Deephaven TableHandle. If you're seeing this, you are trying to call the constructor of TableHandle directly, which is not advised.")
            }
            self$internal_table_handle <- table_handle
            private$is_static_field <- self$internal_table_handle$is_static()
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
            verify_string("name", name)
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

        ###################### TABLE OPERATIONS #######################

        # FILTERING OPERATIONS

        #' @description
        #' Select columns from a table. The columns can be column names or formulas like
        #' "NewCol = A + 12". See the Deephaven documentation for the difference between "select" and "view".
        #' @param columns The columns to select.
        #' @return A TableHandle referencing the new table.
        select = function(columns) {
            verify_string_vector("columns", columns)
            return(TableHandle$new(self$internal_table_handle$select(columns)))
        },

        #' @description
        #' View columns from a table. The columns can be column names or formulas like
        #' "NewCol = A + 12". See the Deephaven documentation for the difference between select() and view().
        #' @param columns The columns to view.
        #' @return A TableHandle referencing the new table.
        view = function(columns) {
            verify_string_vector("columns", columns)
            return(TableHandle$new(self$internal_table_handle$view(columns)))
        },

        #' @description
        #' Creates a new table from this table where the specified columns have been excluded.
        #' @param columns The columns to exclude.
        #' @return A TableHandle referencing the new table.
        drop_columns = function(columns) {
            verify_string_vector("columns", columns)
            return(TableHandle$new(self$internal_table_handle$drop_columns(columns)))
        },

        #' @description
        #' Creates a new table from this table, but including the additional specified columns.
        #' See the Deephaven documentation for the difference between update() and updateView().
        #' @param columns The columns to add. For example, {"X = A + 5", "Y = X * 2"}.
        #' @return A TableHandle referencing the new table.
        update = function(columns) {
            verify_string_vector("columns", columns)
            return(TableHandle$new(self$internal_table_handle$update(columns)))
        },

        #' @description
        #' Creates a new view from this table, but including the additional specified columns.
        #' See the Deephaven documentation for the difference between update() and updateView().
        #' @param columns The columns to add. For example, {"X = A + 5", "Y = X * 2"}.
        #' @return A TableHandle referencing the new table.
        update_view = function(columns) {
            verify_string_vector("columns", columns)
            return(TableHandle$new(self$internal_table_handle$update_view(columns)))
        },

        #' @description
        #' Creates a new table from this table, filtered by condition. Consult the Deephaven
        #' documentation for more information about valid conditions.
        #' @param condition A Deephaven boolean expression such as "Price > 100" or "Col3 == Col1 * Col2".
        #' @return A TableHandle referencing the new table.
        where = function(condition) {
            verify_string("condition", condition)
            return(TableHandle$new(self$internal_table_handle$where(condition)))
        },

        # AGGREGATION OPERATIONS

        #' @description
        #' Creates a new table from this table with one or more aggregations applied to the specified columns.
        #' @param aggregations Deephaven Aggregations to apply to this table.
        #' @return A TableHandle referencing the new table.
        agg_by = function(aggregations) {
            verify_internal_type("Aggregation", "aggregations", aggregations)
            if ((first_class(aggregations) != "list") && (first_class(aggregations) == "Aggregation")) {
                aggregations = c(aggregations)
            }
            unwrapped_aggregations = lapply(aggregations, private$strip_r6_wrapping_from_aggregation)

            return(TableHandle$new(self$internal_table_handle$agg_by(unwrapped_aggregations)))
        },

        #' @description
        #' Creates a new table from this table, grouped by columns, with the "min" aggregate operation
        #' applied to the remaining columns.
        #' @param columns Columns to group by.
        #' @return A TableHandle referencing the new table.
        min_by = function(columns) {
            verify_string_vector("columns", columns)
            return(TableHandle$new(self$internal_table_handle$min_by(columns)))
        },

        #' @description
        #' Creates a new table from this table, grouped by columns, with the "max" aggregate operation
        #' applied to the remaining columns.
        #' @param columns Columns to group by.
        #' @return A TableHandle referencing the new table.
        max_by = function(columns) {
            verify_string_vector("columns", columns)
            return(TableHandle$new(self$internal_table_handle$max_by(columns)))
        },

        #' @description
        #' Creates a new table from this table, grouped by columns, with the "sum" aggregate operation
        #' applied to the remaining columns.
        #' @param columns Columns to group by.
        #' @return A TableHandle referencing the new table.
        sum_by = function(columns) {
            verify_string_vector("columns", columns)
            return(TableHandle$new(self$internal_table_handle$sum_by(columns)))
        },

        #' @description
        #' Creates a new table from this table, grouped by columns, with the "absSum" aggregate operation
        #' applied to the remaining columns.
        #' @param columns Columns to group by.
        #' @return A TableHandle referencing the new table.
        abs_sum_by = function(columns) {
            verify_string_vector("columns", columns)
            return(TableHandle$new(self$internal_table_handle$abs_sum_by(columns)))
        },

        #' @description
        #' Creates a new table from this table, grouped by columns, with the "avg" aggregate operation
        #' applied to the remaining columns.
        #' @param columns Columns to group by.
        #' @return A TableHandle referencing the new table.
        avg_by = function(columns) {
            verify_string_vector("columns", columns)
            return(TableHandle$new(self$internal_table_handle$avg_by(columns)))
        },

        #' @description
        #' Creates a new table from this table, grouped by columns, having a new column named by
        #' `weightColumn` containing the weighted average of each group.
        #' @param weight_column Name of the output column.
        #' @param columns Columns to group by.
        #' @return A TableHandle referencing the new table.
        w_avg_by = function(weight_column, columns) {
            verify_string("weight_column", weight_column)
            verify_string_vector("columns", columns)
            return(TableHandle$new(self$internal_table_handle$w_avg_by(weight_column, columns)))
        },

        #' @description
        #' Creates a new table from this table, grouped by columns, with the "var" aggregate operation
        #' applied to the remaining columns.
        #' @param columns Columns to group by.
        #' @return A TableHandle referencing the new table.
        var_by = function(columns) {
            verify_string_vector("columns", columns)
            return(TableHandle$new(self$internal_table_handle$var_by(columns)))
        },

        #' @description
        #' Creates a new table from this table, grouped by columns, with the "std" aggregate operation
        #' applied to the remaining columns.
        #' @param columns Columns to group by.
        #' @return A TableHandle referencing the new table.
        std_by = function(columns) {
            verify_string_vector("columns", columns)
            return(TableHandle$new(self$internal_table_handle$std_by(columns)))
        },

        #' @description
        #' Creates a new table from this table, grouped by columns, with the "first" aggregate operation
        #' applied to the remaining columns.
        #' @param columns Columns to group by.
        #' @return A TableHandle referencing the new table.
        first_by = function(columns) {
            verify_string_vector("columns", columns)
            return(TableHandle$new(self$internal_table_handle$first_by(columns)))
        },

        #' @description
        #' Creates a new table from this table, grouped by columns, with the "last" aggregate operation
        #' applied to the remaining columns.
        #' @param columns Columns to group by.
        #' @return A TableHandle referencing the new table.
        last_by = function(columns) {
            verify_string_vector("columns", columns)
            return(TableHandle$new(self$internal_table_handle$last_by(columns)))
        },

        #' @description
        #' Creates a new table from this table, grouped by columns, with the "median" aggregate operation
        #' applied to the remaining columns.
        #' @param columns Columns to group by.
        #' @return A TableHandle referencing the new table.
        median_by = function(columns) {
            verify_string_vector("columns", columns)
            return(TableHandle$new(self$internal_table_handle$median_by(columns)))
        },

        #' @description
        #' Creates a new table from this table, grouped by columns, with the "percentile" aggregate operation
        #' applied to the remaining columns.
        #' @param percentile The designated percentile
        #' @param columns Columns to group by.
        #' @return A TableHandle referencing the new table.
        percentile_by = function(percentile, columns) {
            verify_proportion("percentile", percentile)
            verify_string_vector("columns", columns)
            return(TableHandle$new(self$internal_table_handle$percentile_by(percentile, columns)))
        },

        #' @description
        #' Creates a new table from this table, grouped by columns, having a new column named by
        #' `countByColumn` containing the size of each group.
        #' @param count_by_column Name of the output column.
        #' @param columns Columns to group by.
        #' @return A TableHandle referencing the new table.
        count_by = function(count_by_column, columns) {
            verify_string("count_by_column", count_by_column)
            verify_string_vector("columns", columns)
            return(TableHandle$new(self$internal_table_handle$count_by(count_by_column, columns)))
        },

        #' @description
        #' Creates a new table from this table, grouped by columns, containing the first `n` rows of
        #' each group.
        #' @param n Number of rows
        #' @param columns Columns to group by.
        #' @return A TableHandle referencing the new table.
        head_by = function(n, columns) {
            verify_int("n", n)
            verify_string_vector("columns", columns)
            return(TableHandle$new(self$internal_table_handle$head_by(n, columns)))
        },

        #' @description
        #' Creates a new table from this table, grouped by columns, containing the last `n` rows of
        #' each group.
        #' @param n Number of rows
        #' @param columns Columns to group by.
        #' @return A TableHandle referencing the new table.
        tail_by = function(n, columns) {
            verify_int("n", n)
            verify_string_vector("columns", columns)
            return(TableHandle$new(self$internal_table_handle$tail_by(n, columns)))
        },

        # JOIN OPERATIONS

        #' @description
        #' Creates a new table by cross joining this table with `rightSide`. The tables are joined by
        #' the columns in `columnsToMatch`, and columns from `rightSide` are brought in and optionally
        #' renamed by `columnsToAdd`. Example:
        #' @param right_side The table to join with this table
        #' @param columns_to_match The columns to join on
        #' @param columns_to_add The columns from the right side to add, and possibly rename.
        #' @return A TableHandle referencing the new table.
        cross_join = function(right_side, columns_to_match, columns_to_add) {
            verify_string_vector("columns_to_match", columns_to_match)
            verify_string_vector("columns_to_add", columns_to_add)
            return(TableHandle$new(self$internal_table_handle$cross_join(right_side$internal_table_handle,
                                                                            columns_to_match, columns_to_add)))
        },

        #' @description
        #' Creates a new table by natural joining this table with `rightSide`. The tables are joined by
        #' the columns in `columnsToMatch`, and columns from `rightSide` are brought in and optionally
        #' renamed by `columnsToAdd`. Example:
        #' @param right_side The table to join with this table
        #' @param columns_to_match The columns to join on
        #' @param columns_to_add The columns from the right side to add, and possibly rename.
        #' @return A TableHandle referencing the new table.
        natural_join = function(right_side, columns_to_match, columns_to_add) {
            verify_string_vector("columns_to_match", columns_to_match)
            verify_string_vector("columns_to_add", columns_to_add)
            return(TableHandle$new(self$internal_table_handle$natural_join(right_side$internal_table_handle,
                                                                              columns_to_match, columns_to_add)))
        },

        #' @description
        #' Creates a new table by exact joining this table with `rightSide`. The tables are joined by
        #' the columns in `columnsToMatch`, and columns from `rightSide` are brought in and optionally
        #' renamed by `columnsToAdd`. Example:
        #' @param right_side The table to join with this table
        #' @param columns_to_match The columns to join on
        #' @param columns_to_add The columns from the right side to add, and possibly rename.
        #' @return A TableHandle referencing the new table.
        exact_join = function(right_side, columns_to_match, columns_to_add) {
            verify_string_vector("columns_to_match", columns_to_match)
            verify_string_vector("columns_to_add", columns_to_add)
            return(TableHandle$new(self$internal_table_handle$exact_join(right_side$internal_table_handle,
                                                                            columns_to_match, columns_to_add)))
        },

        # MISC OPERATIONS

        #' @description
        #' Creates a new table from this table containing the first `n` rows of this table.
        #' @param n Number of rows
        #' @return A TableHandle referencing the new table.
        head = function(n) {
            verify_int("n", n)
            return(TableHandle$new(self$internal_table_handle$head(n)))
        },

        #' @description
        #' Creates a new table from this table containing the last `n` rows of this table.
        #' @param n Number of rows
        #' @return A TableHandle referencing the new table.
        tail = function(n) {
            verify_int("n", n)
            return(TableHandle$new(self$internal_table_handle$tail(n)))
        },

        #' @description
        #' Creates a new table from this table with the column array data ungrouped. This is the inverse
        #' of the by() const operation.
        #' @param group_by_columns Columns to ungroup.
        #' @return A TableHandle referencing the new table.
        ungroup = function(null_fill, group_by_columns) {
            verify_string_vector(group_by_columns, "group_by_columns")
            return(TableHandle$new(self$internal_table_handle$ungroup(null_fill, group_by_columns)))
        },

        #' @description
        #' Creates a new table from this table, sorted by sortPairs.
        #' @param sorters A vector of Deephaven Sorter objects (either sort.asc or sort.desc) describing the sort.
        #' Each Sorter accepts a column to sort, and whether the sort should consider to the value's regular or
        #' absolute value when doing comparisons.
        #' @return A TableHandle referencing the new table.
        sort = function(sorters) {
            verify_internal_type("Sorter", "sorters", sorters)
            if ((first_class(sorters) != "list") && (first_class(sorters) == "Sorter")) {
                sorters = c(sorters)
            }
            unwrapped_sorters = lapply(sorters, private$strip_r6_wrapping_from_sorter)
            return(TableHandle$new(self$internal_table_handle$sort(unwrapped_sorters)))
        },

        #' @description
        #' Creates a new table by merging `sources` together. The tables are essentially stacked on top
        #' of each other.
        #TODO: Document keyColumn
        #' @param sources The tables to merge.
        #' @return A TableHandle referencing the new table.
        merge = function(key_column, sources) {
            verify_string("key_column", key_column)
            return(TableHandle$new(self$internal_table_handle$merge(key_column, sources)))
        },
        
        internal_table_handle = NULL
    ),
    private = list(

        strip_r6_wrapping_from_aggregation = function(r6_aggregation) {
            return(r6_aggregation$internal_aggregation)
        },

        strip_r6_wrapping_from_sorter = function(r6_sorter) {
            return(r6_sorter$internal_sorter)
        },

        is_static_field = NULL
    )
)
