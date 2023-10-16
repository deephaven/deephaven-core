#' @title Deephaven TableHandles
#' @md
#' @description
#' A TableHandle holds a reference to a Deephaven Table on the server, and provides methods for operating on that table.
#' Note that TableHandles should not be instantiated directly by user code, but rather by server calls accessible from
#' the [`Client`][Client] class. See `?Client` for more information.
#'
#' @section
#' Naming tables on the server:
#' When a TableHandle is created, it is not automatically bound to a variable name on the server. This means that the
#' TableHandle that gets created is _the only_ reference to the table that's been created. Importantly, the variable
#' name given to the TableHandle is purely a _local_ variable, and has no relationship to that table's name on the server.
#' For this reason, code like the following:
#' ```r
#' client <- Client$new(...)
#' df1 <- data.frame(x = 1:10, y = 11:20)
#' t1 <- client$import_table(df1)
#' client$run_script("t2 = t1.update('z = x + y')")
#' ```
#' will not run, because the table referenced by the local variable `t1` is not named on the server at all. To
#' make the table referenced by `t1` accessible by name on the server (e.g., from within query strings), you must
#' _bind it to a variable_ with the method `bind_to_variable()`. We adopt the convention of calling _local TableHandles_
#' `th1`, `th2`, etc., and _server-side tables_ `t1`, `t2`, etc., to help distinguish between the two. So, the above
#' code should be written as:
#' ```r
#' client <- Client$new(...)
#' df1 <- data.frame(x = 1:10, y = 11:20)
#' th1 <- client$import_table(df1)
#' th1$bind_to_variable("t1")
#' client$run_script("t2 = t1.update('z = x + y')")
#' ```
#' You can then create a local TableHandle to reference `t2` as follows:
#' ```r
#' th2 <- client$open_table("t2")
#' ```
#' The above code is not best practice; calling `update()` directly on `t1` would be preferred to running a script.
#' It is, however, more illustrative of the relationship between local TableHandles and server-side tables. The best
#' way to accomplish the above would be the following:
#' ```r
#' client <- Client$new(...)
#' df1 <- data.frame(x = 1:10, y = 11:20)
#' th1 <- client$import_table(df1)
#' th2 <- th1$update("z = x + y")
#'
#' # this is necessary to access the tables from within query strings
#' th1$bind_to_variable("t1")
#' th2$bind_to_variable("t2")
#' ```
#'
#' @usage NULL
#' @format NULL
#' @docType class
#'
#' @examples
#' \dontrun{
#' library(rdeephaven)
#'
#' # connecting to Deephaven server
#' client <- Client$new("localhost:10000", auth_type="psk", auth_token="my_secret_token")
#'
#' # create a data frame, push it to the server, and retrieve a TableHandle referencing the new table
#' df <- data.frame(
#'   timeCol = seq.POSIXt(as.POSIXct(Sys.Date()), as.POSIXct(Sys.Date() + 0.01), by = "1 sec")[1:50],
#'   boolCol = sample(c(TRUE,FALSE), 50, TRUE),
#'   col1 = sample(1000, size = 50, replace = TRUE),
#'   col2 = sample(1000, size = 50, replace = TRUE),
#'   col3 = 1:50
#' )
#' th <- client$import_table(df)
#'
#' # get the dimension of the table
#' dim(th)
#'
#' # get the last 10 rows of the table
#' th2 <- tail(th, 10)
#' as.data.frame(th2)
#'
#' # create several new columns
#' th3 <- th$update(c("col4 = col1 + col2", "charCol = col3 % 2 == 0 ? `A` : `B`"))
#' as.data.frame(th3)
#'
#' # filter based on parity of col3
#' th4 <- th3$where("charCol == `A`")
#' as.data.frame(th4)
#'
#' # select a subset of columns
#' th5 <- th3$select(c("timeCol", "col1", "col4"))
#' as.data.frame(th5)
#'
#' # drop timestamp column and get sum of remaining columns grouped by boolCol and charCol
#' th6 <- th3$
#'   drop_columns("timeCol")$
#'   sum_by(c("boolCol", "charCol"))
#' as.data.frame(th6)
#'
#' client$close()
#' }
#'
#' @export
TableHandle <- R6Class("TableHandle",
  cloneable = FALSE,
  public = list(
    .internal_rcpp_object = NULL,

    #' @description
    #' Initializes a new TableHandle from an internal Deephaven TableHandle.
    #' @param table_handle Internal Deephaven TableHandle.
    initialize = function(table_handle) {
      if (class(table_handle)[[1]] != "Rcpp_INTERNAL_TableHandle") {
        stop("'table_handle' should be an internal Deephaven TableHandle. If you're seeing this,
                you are trying to call the constructor of TableHandle directly, which is not advised.")
      }
      self$.internal_rcpp_object <- table_handle
    },

    #' @description
    #' Determines whether the table referenced by this TableHandle is static or not.
    #' @return TRUE if the table is static, or FALSE if the table is ticking.
    is_static = function() {
      return(self$.internal_rcpp_object$is_static())
    },

    #' @description
    #' Binds the table referenced by this TableHandle to a variable on the server, so that it can be referenced by that name.
    #' @param name Name for this table on the server.
    bind_to_variable = function(name) {
      verify_string("name", name, TRUE)
      self$.internal_rcpp_object$bind_to_variable(name)
    },

    ### BASE R METHODS, ALSO IMPLEMENTED FUNCTIONALLY

    #' @description
    #' Creates a new table containing the first `n` rows of this table.
    #' @param n Positive integer specifying the number of rows to return.
    #' @return A TableHandle referencing the new table.
    head = function(n) {
      verify_positive_int("n", n, TRUE)
      return(TableHandle$new(self$.internal_rcpp_object$head(n)))
    },

    #' @description
    #' Creates a new table containing the last `n` rows of this table.
    #' @param n Positive integer specifying the number of rows to return.
    #' @return A TableHandle referencing the new table consisting of the last n rows of the parent table.
    tail = function(n) {
      verify_positive_int("n", n, TRUE)
      return(TableHandle$new(self$.internal_rcpp_object$tail(n)))
    },

    #' @description
    #' Gets the number of rows in the table referenced by this TableHandle.
    #' @return The number of rows in the table.
    nrow = function() {
      return(self$.internal_rcpp_object$num_rows())
    },

    #' @description
    #' Gets the number of columns in the table referenced by this TableHandle.
    #' @return The number of columns in the table.
    ncol = function() {
      return(self$.internal_rcpp_object$num_cols())
    },

    #' @description
    #' Gets the dimensions of the table referenced by this TableHandle. Equivalent to `c(nrow, ncol)`.
    #' @return A vector of length 2, where the first element is the number of rows in the table and the second
    #' element is the number of columns in the table.
    dim = function() {
      return(c(self$nrow(), self$ncol()))
    },

    #' @description
    #' Merges several tables into one table on the server. All tables must have the same schema as this table, and can
    #' be supplied as a list of TableHandles, any number of TableHandles, or a mix of both.
    #' @param ... Arbitrary number of TableHandles or vectors of TableHandles with a schema matching this table.
    #' @return A TableHandle referencing the new table.
    merge = function(...) {
      table_list <- unlist(c(...))
      if (length(table_list) == 0) {
        return(self)
      }
      verify_type("table_list", table_list, FALSE, "TableHandle", "a Deephaven TableHandle")
      unwrapped_table_list <- lapply(table_list, strip_r6_wrapping)
      return(TableHandle$new(self$.internal_rcpp_object$merge(unwrapped_table_list)))
    },

    ### CONVERSION METHODS, ALSO IMPLEMENTED FUNCTIONALLY

    #' @description
    #' Converts the table referenced by this TableHandle to an Arrow RecordBatchStreamReader.
    #' @return An Arrow RecordBatchStreamReader constructed from the data of this TableHandle.
    as_record_batch_reader = function() {
      ptr <- self$.internal_rcpp_object$get_arrow_array_stream_ptr()
      rbsr <- arrow::RecordBatchStreamReader$import_from_c(ptr)
      return(rbsr)
    },

    #' @description
    #' Converts the table referenced by this TableHandle to an Arrow Table.
    #' @return An Arrow Table constructed from the data of this TableHandle.
    as_arrow_table = function() {
      rbsr <- self$as_record_batch_reader()
      arrow_tbl <- rbsr$read_table()
      return(arrow_tbl)
    },

    #' @description
    #' Converts the table referenced by this TableHandle to a dplyr tibble.
    #' @return A dplyr tibble constructed from the data of this TableHandle.
    as_tibble = function() {
      rbsr <- self$as_record_batch_reader()
      arrow_tbl <- rbsr$read_table()
      return(as_tibble(arrow_tbl))
    },

    #' @description
    #' Converts the table referenced by this TableHandle to an R data frame.
    #' @return An R data frame constructed from the data of this TableHandle.
    as_data_frame = function() {
      arrow_tbl <- self$as_arrow_table()
      return(as.data.frame(as.data.frame(arrow_tbl))) # TODO: for some reason as.data.frame on arrow table returns a tibble, not a data frame
    },

    ### DEEPHAVEN TABLE OPERATIONS

    #' @description
    #' Creates a new in-memory table that includes one column for each formula.
    #' If no formula is specified, all columns will be included.
    #' @param formulas String or list of strings denoting the column formulas.
    #' @return A TableHandle referencing the new table.
    select = function(formulas = character()) {
      verify_string("formulas", formulas, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$select(formulas)))
    },

    #' @description
    #' Creates a new formula table that includes one column for each formula.
    #' @param formulas String or list of strings denoting the column formulas.
    #' @return A TableHandle referencing the new table.
    view = function(formulas = character()) {
      verify_string("formulas", formulas, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$view(formulas)))
    },

    #' @description
    #' Creates a new table containing a new, in-memory column for each formula.
    #' @param formulas String or list of strings denoting the column formulas.
    #' @return A TableHandle referencing the new table.
    update = function(formulas = character()) {
      verify_string("formulas", formulas, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$update(formulas)))
    },

    #' @description
    #' Creates a new table containing a new formula column for each formula.
    #' @param formulas String or list of strings denoting the column formulas.
    #' @return A TableHandle referencing the new table.
    update_view = function(formulas = character()) {
      verify_string("formulas", formulas, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$update_view(formulas)))
    },

    #' @description
    #' Creates a new table that has the same number of rows as this table,
    #' but omits the columns specified in `cols`.
    #' @param cols String or list of strings denoting the names of the columns to drop.
    #' @return A TableHandle referencing the new table.
    drop_columns = function(cols = character()) {
      verify_string("cols", cols, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$drop_columns(cols)))
    },

    #' @description
    #' Creates a new table containing only the rows meeting the filter condition.
    #' @param filter String denoting the filter condition.
    #' @return A TableHandle referencing the new table.
    where = function(filter) {
      verify_string("filter", filter, TRUE)
      return(TableHandle$new(self$.internal_rcpp_object$where(filter)))
    },

    #' @description
    #' Creates a new table containing grouping columns and grouped data, with column content is grouped into arrays.
    #' If no group-by column is given, the content of each column is grouped into its own array.
    #' @param by String or list of strings denoting the names of the columns to group by.
    #' @return A TableHandle referencing the new table.
    group_by = function(by = character()) {
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$group_by(by)))
    },

    #' @description
    #' Creates a new table in which array columns from the source table are unwrapped into separate rows.
    #' The ungroup columns should be of array types.
    #' @param by String or list of strings denoting the names of the columns to ungroup.
    #' @return A TableHandle referencing the new table.
    ungroup = function(by = character()) {
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$ungroup(by)))
    },

    #' @description
    #' Creates a table with additional columns calculated from window-based aggregations of columns in this table.
    #' The aggregations are defined by the provided operations, which support incremental aggregations over the
    #' corresponding rows in the table. The aggregations will apply position or time-based windowing and compute the
    #' results over the entire table or each row group as identified by the provided key columns.
    #' See more detailed documentation [here][UpdateBy] or run `?UpdateBy`.
    #' @param ops `UpdateByOp` or list of `UpdateByOp`s to perform on non-grouping columns.
    #' @param by String or list of strings denoting the names of the columns to group by.
    #' @return A TableHandle referencing the new table.
    update_by = function(ops, by = character()) {
      verify_type("ops", ops, FALSE, "UpdateByOp", "a Deephaven UpdateByOp")
      verify_string("by", by, FALSE)
      ops <- c(ops)
      unwrapped_ops <- lapply(ops, strip_r6_wrapping)
      return(TableHandle$new(self$.internal_rcpp_object$update_by(unwrapped_ops, by)))
    },

    #' @description
    #' Creates a new table containing grouping columns and grouped data. The resulting grouped data is defined by the
    #' aggregation(s) specified. See more detailed documentation [here][AggBy] or run `?AggBy`.
    #' @param aggs `AggOp` or list of `AggOp`s to perform on non-grouping columns.
    #' @param by String or list of strings denoting the names of the columns to group by.
    #' @return A TableHandle referencing the new table.
    agg_by = function(aggs, by = character()) {
      verify_type("aggs", aggs, FALSE, "AggOp", "a Deephaven AggOp")
      verify_string("by", by, FALSE)
      aggs <- c(aggs)
      for (i in 1:length(aggs)) {
        if (!is.null(aggs[[i]]$.internal_num_cols) && aggs[[i]]$.internal_num_cols == 0) {
          stop(paste0("Aggregations with no columns cannot be used in 'agg_by'. Got '", aggs[[i]]$.internal_agg_name, "' at index ", i, " with an empty 'cols' argument."))
        }
      }
      unwrapped_aggs <- lapply(aggs, strip_r6_wrapping)
      return(TableHandle$new(self$.internal_rcpp_object$agg_by(unwrapped_aggs, by)))
    },

    #' @description
    #' Creates a new table containing grouping columns and grouped data. The resulting grouped data is defined by the
    #' aggregation(s) specified. See more detailed documentation [here][AggBy] or run `?AggBy`.
    #' This method applies the aggregation to all non-grouping columns of the table, so it can only
    #' accept one aggregation at a time.
    #' @param agg `AggOp` to perform on non-grouping columns.
    #' @param by String or list of strings denoting the names of the columns to group by.
    #' @return A TableHandle referencing the new table.
    agg_all_by = function(agg, by = character()) {
      verify_type("agg", agg, TRUE, "AggOp", "a Deephaven AggOp")
      return(TableHandle$new(self$.internal_rcpp_object$agg_all_by(agg$.internal_rcpp_object, by)))
    },

    #' @description
    #' Creates a new table containing the first row of each group.
    #' @param by String or list of strings denoting the names of the columns to group by.
    #' @return A TableHandle referencing the new table.
    first_by = function(by = character()) {
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$first_by(by)))
    },

    #' @description
    #' Creates a new table containing the last row of each group.
    #' @param by String or list of strings denoting the names of the columns to group by.
    #' @return A TableHandle referencing the new table.
    last_by = function(by = character()) {
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$last_by(by)))
    },

    #' @description
    #' Creates a new table containing the first `num_rows` rows of each group.
    #' @param num_rows Positive integer specifying the number of rows to return.
    #' @param by String or list of strings denoting the names of the columns to group by.
    #' @return A TableHandle referencing the new table.
    head_by = function(num_rows, by = character()) {
      verify_positive_int("num_rows", num_rows, TRUE)
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$head_by(num_rows, by)))
    },

    #' @description
    #' Creates a new table containing the last `num_rows` rows of each group.
    #' @param num_rows Positive integer specifying the number of rows to return.
    #' @param by String or list of strings denoting the names of the columns to group by.
    #' @return A TableHandle referencing the new table.
    tail_by = function(num_rows, by = character()) {
      verify_positive_int("num_rows", num_rows, TRUE)
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$tail_by(num_rows, by)))
    },

    #' @description
    #' Creates a new table containing the column-wise minimum of each group.
    #' @param by String or list of strings denoting the names of the columns to group by.
    #' @return A TableHandle referencing the new table.
    min_by = function(by = character()) {
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$min_by(by)))
    },

    #' @description
    #' Creates a new table containing the column-wise maximum of each group.
    #' @param by String or list of strings denoting the names of the columns to group by.
    #' @return A TableHandle referencing the new table.
    max_by = function(by = character()) {
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$max_by(by)))
    },

    #' @description
    #' Creates a new table containing the column-wise sum of each group.
    #' @param by String or list of strings denoting the names of the columns to group by.
    #' @return A TableHandle referencing the new table.
    sum_by = function(by = character()) {
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$sum_by(by)))
    },

    #' @description
    #' Creates a new table containing the column-wise absolute sum of each group.
    #' @param by String or list of strings denoting the names of the columns to group by.
    #' @return A TableHandle referencing the new table.
    abs_sum_by = function(by = character()) {
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$abs_sum_by(by)))
    },

    #' @description
    #' Creates a new table containing the column-wise average of each group.
    #' @param by String or list of strings denoting the names of the columns to group by.
    #' @return A TableHandle referencing the new table.
    avg_by = function(by = character()) {
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$avg_by(by)))
    },

    #' @description
    #' Creates a new table containing the column-wise weighted average of each group.
    #' @param wcol String denoting the name of the column to use as weights.
    #' @param by String or list of strings denoting the names of the columns to group by.
    #' @return A TableHandle referencing the new table.
    w_avg_by = function(wcol, by = character()) {
      verify_string("wcol", wcol, TRUE)
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$w_avg_by(wcol, by)))
    },

    #' @description
    #' Creates a new table containing the column-wise median of each group.
    #' @param by String or list of strings denoting the names of the columns to group by.
    #' @return A TableHandle referencing the new table.
    median_by = function(by = character()) {
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$median_by(by)))
    },

    #' @description
    #' Creates a new table containing the column-wise variance of each group.
    #' @param by String or list of strings denoting the names of the columns to group by.
    #' @return A TableHandle referencing the new table.
    var_by = function(by = character()) {
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$var_by(by)))
    },

    #' @description
    #' Creates a new table containing the column-wise standard deviation of each group.
    #' @param by String or list of strings denoting the names of the columns to group by.
    #' @return A TableHandle referencing the new table.
    std_by = function(by = character()) {
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$std_by(by)))
    },

    #' @description
    #' Creates a new table containing the column-wise percentile of each group.
    #' @param percentile Numeric scalar between 0 and 1 denoting the percentile to compute.
    #' @param by String or list of strings denoting the names of the columns to group by.
    #' @return A TableHandle referencing the new table.
    percentile_by = function(percentile, by = character()) {
      verify_in_unit_interval("percentile", percentile, TRUE)
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$percentile_by(percentile, by)))
    },

    #' @description
    #' Creates a new table containing the number of rows in each group.
    #' @param col String denoting the name of the new column to hold the counts of each group.
    #' @param by String or list of strings denoting the names of the columns to group by.
    #' @return A TableHandle referencing the new table.
    count_by = function(col, by = character()) {
      verify_string("col", col, TRUE)
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$count_by(col, by)))
    },

    #' @description
    #' Creates a new table containing rows that have matching values in both tables. Rows that do not have matching
    #' criteria will not be included in the result. If there are multiple matches between a row from the left table
    #' and rows from the right table, all matching combinations will be included. If no columns to match (on) are
    #' specified, every combination of left and right table rows is included.
    #' @param table TableHandle referencing the table to join with.
    #' @param on String or list of strings denoting the names of the columns to join on.
    #' @param joins String or list of strings denoting the names of the columns to add from `table`.
    #' @return A TableHandle referencing the new table.
    join = function(table, on = character(), joins = character()) {
      verify_string("on", on, FALSE)
      verify_string("joins", joins, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$join(
        table$.internal_rcpp_object,
        on, joins
      )))
    },

    #' @description
    #' Creates a new table containing all the rows and columns of this table, plus additional columns containing data
    #' from the right table. For columns appended to the left table (joins), row values equal the row values from the
    #' right table where the key values in the left and right tables are equal.
    #' If there is no matching key in the right table, appended row values are NULL.
    #' @param table TableHandle referencing the table to join with.
    #' @param on String or list of strings denoting the names of the columns to join on.
    #' @param joins String or list of strings denoting the names of the columns to add from `table`.
    #' @return A TableHandle referencing the new table.
    natural_join = function(table, on = character(), joins = character()) {
      verify_string("on", on, FALSE)
      verify_string("joins", joins, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$natural_join(
        table$.internal_rcpp_object,
        on, joins
      )))
    },

    #' @description
    #' Creates a new table containing all the rows and columns of this table, plus additional columns containing data
    #' from the right table. For columns appended to the left table (joins), row values equal the row values from the
    #' right table where the key values in the left and right tables are equal.
    #' @param table TableHandle referencing the table to join with.
    #' @param on String or list of strings denoting the names of the columns to join on.
    #' @param joins String or list of strings denoting the names of the columns to add from `table`.
    #' @return A TableHandle referencing the new table.
    exact_join = function(table, on = character(), joins = character()) {
      verify_string("on", on, FALSE)
      verify_string("joins", joins, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$exact_join(
        table$.internal_rcpp_object,
        on, joins
      )))
    },

    #' @description
    #' Creates a new table containing all the rows and columns of this table, sorted by the specified columns.
    #' @param order_by String or list of strings denoting the names of the columns to sort by.
    #' @param descending Boolean or list of booleans denoting whether to sort in descending order.
    #' If a list is supplied, it must be the same length as `order_by`.
    #' @param abs_sort Boolean or list of booleans denoting whether to sort by absolute value.
    #' If a list is supplied, it must be the same length as `order_by`.
    #' @return A TableHandle referencing the new table.
    sort = function(order_by, descending = FALSE, abs_sort = FALSE) {
      verify_string("order_by", order_by, FALSE)
      verify_bool("descending", descending, FALSE)
      verify_bool("abs_sort", abs_sort, FALSE)
      if ((length(descending) > 1) && length(descending) != length(order_by)) {
        stop(paste0("'descending' must be the same length as 'order_by' if more than one entry is supplied. Got 'order_by' with length ", length(order_by), " and 'descending' with length ", length(descending), "."))
      }
      if ((length(abs_sort) > 1) && length(abs_sort) != length(by)) {
        stop(paste0("'abs_sort' must be the same length as 'order_by' if more than one entry is supplied. Got 'order_by' with length ", length(order_by), " and 'abs_sort' with length ", length(abs_sort), "."))
      }
      return(TableHandle$new(self$.internal_rcpp_object$sort(order_by, descending, abs_sort)))
    }
  )
)

#' @export
head.TableHandle <- function(x, n = 1, ...) {
  return(x$head(n))
}

#' @export
tail.TableHandle <- function(x, n = 1, ...) {
  return(x$tail(n))
}

#' @export
nrow.TableHandle <- function(x) {
  return(x$nrow())
}

#' @export
ncol.TableHandle <- function(x) {
  return(x$ncol())
}

#' @export
dim.TableHandle <- function(x) {
  return(x$dim())
}


#' @name
#' merge_tables
#' @title
#' Merge tables with the same schema
#' @md
#'
#' @description
#' Merges several tables into one table on the server. All tables must have the same schema, and can
#' be supplied as a list of TableHandles, any number of TableHandles, or a mix of both.
#'
#' @param ... Arbitrary number of TableHandles or vectors of TableHandles with a schema matching this table.
#' @return A TableHandle referencing the new table.
#'
#' @examples
#' print("hello!")
#'
#' @export
merge_tables <- function(...) {
  table_list <- unlist(c(...))
  if (length(table_list) == 0) {
    return(NULL)
  }
  verify_type("table_list", table_list, FALSE, "TableHandle", "Deephaven TableHandle")
  if (length(table_list) == 1) {
    return(table_list[[1]])
  }
  return(table_list[[1]]$merge(table_list[2:length(table_list)]))
}

#' @export
as_record_batch_reader.TableHandle <- function(x, ...) {
  return(x$as_record_batch_reader())
}

#' @export
as_arrow_table.TableHandle <- function(x, ...) {
  return(x$as_arrow_table())
}

#' @export
as_tibble.TableHandle <- function(x, ...) {
  return(x$as_tibble())
}

#' @export
as.data.frame.TableHandle <- function(x, row.names = NULL, optional = FALSE, ...) {
  return(x$as_data_frame())
}
