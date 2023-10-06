#' @name
#' Aggregations
#' @title
#' Aggregations
#' @md
#' @usage NULL
#' @format NULL
#' @methods NULL
#' @docType class
#'
#' @description
#' Table aggregations are a quintessential feature of Deephaven. You can apply as many aggregations as
#' needed to static tables _or_ streaming tables, and if the parent tables are streaming, the resulting aggregated
#' tables will update alongside their parent tables. It is also very easy to perform _grouped_ aggregations, which
#' allow you to aggregate tables on a per-group basis.
#'
#' @section
#' Apply aggregations to a table:
#' There are two methods for performing aggregations on a table, `agg_by()` and `agg_all_by()`. `agg_by()` allows you to
#' perform many aggregations on specified columns, while `agg_all_by()` allows you to perform a single aggregation to
#' every column in the table. Both methods have an optional `by` parameter that is used to specify grouping columns.
#' Here are some details on each method:
#'
#' - `TableHandle$agg_by(aggs, by)`: Creates a new table containing grouping columns and grouped data.
#'   The resulting grouped data is defined by the aggregation(s) specified.
#' - `TableHandle$agg_all_by(agg, by)`: Creates a new table containing grouping columns and grouped data.
#'   The resulting grouped data is defined by the aggregation specified. This method applies the aggregation to all
#'   columns of the table, so it can only accept one aggregation at a time.
#'
#' For more details, see the reference documentation on each method by running `?agg_by`, or `?agg_all_by`.
#'
#' @section
#' Aggregation functions:
#' Aggregation functions are used to define the grouped data in an aggregated table by passing them to `agg_by()`
#' or `agg_all_by()`. These functions are _generators_, meaning they return _functions_ that the Deephaven engine knows
#' how to interpret. Here is a list of all aggregation functions available in Deephaven:
#'
#' - [`agg_first(cols)`][agg_first]: Creates a First aggregation that computes the first value of each column in `cols` for each aggregation group.
#' - [`agg_last(cols)`][agg_last]: Creates a Last aggregation that computes the last value of each column in `cols` for each aggregation group.
#' - [`agg_min(cols)`][agg_min]: Creates a Minimum aggregation that computes the minimum of each column in `cols` for each aggregation group.
#' - [`agg_max(cols)`][agg_max]: Creates a Maximum aggregation that computes the maximum of each column in `cols` for each aggregation group.
#' - [`agg_sum(cols)`][agg_sum]: Creates a Sum aggregation that computes the sum of each column in `cols` for each aggregation group.
#' - [`agg_abs_sum(cols)`][agg_abs_sum]: Creates an Absolute Sum aggregation that computes the absolute sum of each column in `cols` for each aggregation group.
#' - [`agg_avg(cols)`][agg_avg]: Creates an Average aggregation that computes the average of each column in `cols` for each aggregation group.
#' - [`agg_w_avg(wcol, cols)`][agg_w_avg]: Creates a Weighted Average aggregation that computes the weighted average of each column in `cols` for each aggregation group.
#' - [`agg_median(cols)`][agg_median]: Creates a Median aggregation that computes the median of each column in `cols` for each aggregation group.
#' - [`agg_var(cols)`][agg_var]: Creates a Variance aggregation that computes the variance of each column in `cols` for each aggregation group.
#' - [`agg_std(cols)`][agg_std]: Creates a Standard Deviation aggregation that computes the standard deviation of each column in `cols`, for each aggregation group.
#' - [`agg_percentile(percentile, cols)`][agg_percentile]: Creates a Percentile aggregation that computes the given percentile of each column in `cols` for each aggregation group.
#' - [`agg_count(col)`][agg_count]: Creates a Count aggregation that computes the number of rows in each aggregation group.
#'
#' For more details on each aggregation function, click on one of the methods above or see the reference documentation
#' by running `?agg_first`, `?agg_last`, etc.
#'
#' @section
#' Usage:
#'
#' ```
#' NULL
#' ```
#'
NULL

AggOp <- R6Class("AggOp",
  cloneable = FALSE,
  public = list(
    .internal_rcpp_object = NULL,
    .internal_num_cols = NULL,
    .internal_agg_name = NULL,
    initialize = function(aggregation, agg_name, ...) {
      self$.internal_agg_name <- agg_name
      args <- list(...)
      if (any(names(args) == "cols")) {
        self$.internal_num_cols <- length(args$cols)
      }
      self$.internal_rcpp_object <- do.call(aggregation, args)
    }
  )
)


#' @name
#' agg_first
#' @title
#' First element of specified columns by group
#' @md
#'
#' @description
#' Creates a First aggregation that computes the first value of each column in `cols` for each aggregation group.
#'
#' @details
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `agg_by()` or
#' `agg_all_by()` caller function. The aggregation groups are defined by the unique combinations of values in the `by`
#' columns. For example, if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of
#' values in the `A` and `B` columns.
#'
#' This function, like the other Deephaven `agg_*` functions, is a generator function. That is, its output is another
#' function that is intended to be used in a call to `agg_by()` or `agg_all_by()`. This detail is typically hidden from
#' the user by the `agg_by()` and `agg_all_by()` functions, which call the generated functions internally. However, it is
#' important to understand this detail for debugging purposes, as the output of an `agg_*` function can otherwise seem
#' unexpected.
#'
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to aggregate all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return Aggregation function to be used in `agg_by()` or `agg_all_by()`.
#'
#' @examples
#' print("hello!")
#'
#' @export
agg_first <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(AggOp$new(INTERNAL_agg_first, "agg_first", cols=cols))
}

#' @name
#' agg_last
#' @title
#' Last element of specified columns by group
#' @md
#'
#' @description
#' Creates a Last aggregation that computes the last value of each column in `cols` for each aggregation group.
#'
#' @details
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `agg_by()` or
#' `agg_all_by()` caller function. The aggregation groups are defined by the unique combinations of values in the `by`
#' columns. For example, if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of
#' values in the `A` and `B` columns.
#'
#' This function, like the other Deephaven `agg_*` functions, is a generator function. That is, its output is another
#' function that is intended to be used in a call to `agg_by()` or `agg_all_by()`. This detail is typically hidden from
#' the user by the `agg_by()` and `agg_all_by()` functions, which call the generated functions internally. However, it is
#' important to understand this detail for debugging purposes, as the output of an `agg_*` function can otherwise seem
#' unexpected.
#'
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to aggregate all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return Aggregation function to be used in `agg_by()` or `agg_all_by()`.
#'
#' @examples
#' print("hello!")
#'
#' @export
agg_last <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(AggOp$new(INTERNAL_agg_last, "agg_last", cols=cols))
}

#' @name
#' agg_min
#' @title
#' Minimum of specified columns by group
#' @md
#'
#' @description
#' Creates a Minimum aggregation that computes the minimum of each column in `cols` for each aggregation group.
#'
#' @details
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `agg_by()` or
#' `agg_all_by()` caller function. The aggregation groups are defined by the unique combinations of values in the `by`
#' columns. For example, if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of
#' values in the `A` and `B` columns.
#'
#' This function, like the other Deephaven `agg_*` functions, is a generator function. That is, its output is another
#' function that is intended to be used in a call to `agg_by()` or `agg_all_by()`. This detail is typically hidden from
#' the user by the `agg_by()` and `agg_all_by()` functions, which call the generated functions internally. However, it is
#' important to understand this detail for debugging purposes, as the output of an `agg_*` function can otherwise seem
#' unexpected.
#'
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to aggregate all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return Aggregation function to be used in `agg_by()` or `agg_all_by()`.
#'
#' @examples
#' print("hello!")
#'
#' @export
agg_min <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(AggOp$new(INTERNAL_agg_min, "agg_min", cols=cols))
}

#' @name
#' agg_max
#' @title
#' Maximum of specified columns by group
#' @md
#'
#' @description
#' Creates a Maximum aggregation that computes the maximum of each column in `cols` for each aggregation group.
#'
#' @details
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `agg_by()` or
#' `agg_all_by()` caller function. The aggregation groups are defined by the unique combinations of values in the `by`
#' columns. For example, if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of
#' values in the `A` and `B` columns.
#'
#' This function, like the other Deephaven `agg_*` functions, is a generator function. That is, its output is another
#' function that is intended to be used in a call to `agg_by()` or `agg_all_by()`. This detail is typically hidden from
#' the user by the `agg_by()` and `agg_all_by()` functions, which call the generated functions internally. However, it is
#' important to understand this detail for debugging purposes, as the output of an `agg_*` function can otherwise seem
#' unexpected.
#'
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to aggregate all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return Aggregation function to be used in `agg_by()` or `agg_all_by()`.
#'
#' @examples
#' print("hello!")
#'
#' @export
agg_max <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(AggOp$new(INTERNAL_agg_max, "agg_max", cols=cols))
}

#' @name
#' agg_sum
#' @title
#' Sum element of specified columns by group
#' @md
#'
#' @description
#' Creates a Sum aggregation that computes the sum of each column in `cols` for each aggregation group.
#'
#' @details
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `agg_by()` or
#' `agg_all_by()` caller function. The aggregation groups are defined by the unique combinations of values in the `by`
#' columns. For example, if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of
#' values in the `A` and `B` columns.
#'
#' This function, like the other Deephaven `agg_*` functions, is a generator function. That is, its output is another
#' function that is intended to be used in a call to `agg_by()` or `agg_all_by()`. This detail is typically hidden from
#' the user by the `agg_by()` and `agg_all_by()` functions, which call the generated functions internally. However, it is
#' important to understand this detail for debugging purposes, as the output of an `agg_*` function can otherwise seem
#' unexpected.
#'
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to aggregate all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return Aggregation function to be used in `agg_by()` or `agg_all_by()`.
#'
#' @examples
#' print("hello!")
#'
#' @export
agg_sum <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(AggOp$new(INTERNAL_agg_sum, "agg_sum", cols=cols))
}

#' @name
#' agg_abs_sum
#' @title
#' Absolute sum of specified columns by group
#' @md
#'
#' @description
#' Creates an Absolute Sum aggregation that computes the absolute sum of each column in `cols` for each aggregation group.
#'
#' @details
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `agg_by()` or
#' `agg_all_by()` caller function. The aggregation groups are defined by the unique combinations of values in the `by`
#' columns. For example, if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of
#' values in the `A` and `B` columns.
#'
#' This function, like the other Deephaven `agg_*` functions, is a generator function. That is, its output is another
#' function that is intended to be used in a call to `agg_by()` or `agg_all_by()`. This detail is typically hidden from
#' the user by the `agg_by()` and `agg_all_by()` functions, which call the generated functions internally. However, it is
#' important to understand this detail for debugging purposes, as the output of an `agg_*` function can otherwise seem
#' unexpected.
#'
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to aggregate all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return Aggregation function to be used in `agg_by()` or `agg_all_by()`.
#'
#' @examples
#' print("hello!")
#'
#' @export
agg_abs_sum <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(AggOp$new(INTERNAL_agg_abs_sum, "agg_abs_sum", cols=cols))
}

#' @name
#' agg_avg
#' @title
#' Average of specified columns by group
#' @md
#'
#' @description
#' Creates an Average aggregation that computes the average of each column in `cols` for each aggregation group.
#'
#' @details
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `agg_by()` or
#' `agg_all_by()` caller function. The aggregation groups are defined by the unique combinations of values in the `by`
#' columns. For example, if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of
#' values in the `A` and `B` columns.
#'
#' This function, like the other Deephaven `agg_*` functions, is a generator function. That is, its output is another
#' function that is intended to be used in a call to `agg_by()` or `agg_all_by()`. This detail is typically hidden from
#' the user by the `agg_by()` and `agg_all_by()` functions, which call the generated functions internally. However, it is
#' important to understand this detail for debugging purposes, as the output of an `agg_*` function can otherwise seem
#' unexpected.
#'
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to aggregate all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return Aggregation function to be used in `agg_by()` or `agg_all_by()`.
#'
#' @examples
#' print("hello!")
#'
#' @export
agg_avg <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(AggOp$new(INTERNAL_agg_avg, "agg_avg", cols=cols))
}

#' @name
#' agg_w_avg
#' @title
#' Weighted average of specified columns by group
#' @md
#'
#' @description
#' Creates a Weighted Average aggregation that computes the weighted average of each column in `cols` for each aggregation group.
#'
#' @details
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `agg_by()` or
#' `agg_all_by()` caller function. The aggregation groups are defined by the unique combinations of values in the `by`
#' columns. For example, if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of
#' values in the `A` and `B` columns.
#'
#' This function, like the other Deephaven `agg_*` functions, is a generator function. That is, its output is another
#' function that is intended to be used in a call to `agg_by()` or `agg_all_by()`. This detail is typically hidden from
#' the user by the `agg_by()` and `agg_all_by()` functions, which call the generated functions internally. However, it is
#' important to understand this detail for debugging purposes, as the output of an `agg_*` function can otherwise seem
#' unexpected.
#'
#' @param wcol String denoting the column to use for weights. This must be a numeric column.
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to aggregate all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return Aggregation function to be used in `agg_by()` or `agg_all_by()`.
#'
#' @examples
#' print("hello!")
#'
#' @export
agg_w_avg <- function(wcol, cols = character()) {
  verify_string("wcol", wcol, TRUE)
  verify_string("cols", cols, FALSE)
  return(AggOp$new(INTERNAL_agg_w_avg, "agg_w_avg", wcol=wcol, cols=cols))
}

#' @name
#' agg_median
#' @title
#' Median of specified columns by group
#' @md
#'
#' @description
#' Creates a Median aggregation that computes the median of each column in `cols` for each aggregation group.
#'
#' @details
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `agg_by()` or
#' `agg_all_by()` caller function. The aggregation groups are defined by the unique combinations of values in the `by`
#' columns. For example, if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of
#' values in the `A` and `B` columns.
#'
#' This function, like the other Deephaven `agg_*` functions, is a generator function. That is, its output is another
#' function that is intended to be used in a call to `agg_by()` or `agg_all_by()`. This detail is typically hidden from
#' the user by the `agg_by()` and `agg_all_by()` functions, which call the generated functions internally. However, it is
#' important to understand this detail for debugging purposes, as the output of an `agg_*` function can otherwise seem
#' unexpected.
#'
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to aggregate all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return Aggregation function to be used in `agg_by()` or `agg_all_by()`.
#'
#' @examples
#' print("hello!")
#'
#' @export
agg_median <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(AggOp$new(INTERNAL_agg_median, "agg_median", cols=cols))
}

#' @name
#' agg_var
#' @title
#' Variance of specified columns by group
#' @md
#'
#' @description
#' Creates a Variance aggregation that computes the variance of each column in `cols` for each aggregation group.
#'
#' @details
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `agg_by()` or
#' `agg_all_by()` caller function. The aggregation groups are defined by the unique combinations of values in the `by`
#' columns. For example, if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of
#' values in the `A` and `B` columns.
#'
#' This function, like the other Deephaven `agg_*` functions, is a generator function. That is, its output is another
#' function that is intended to be used in a call to `agg_by()` or `agg_all_by()`. This detail is typically hidden from
#' the user by the `agg_by()` and `agg_all_by()` functions, which call the generated functions internally. However, it is
#' important to understand this detail for debugging purposes, as the output of an `agg_*` function can otherwise seem
#' unexpected.
#'
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to aggregate all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return Aggregation function to be used in `agg_by()` or `agg_all_by()`.
#'
#' @examples
#' print("hello!")
#'
#' @export
agg_var <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(AggOp$new(INTERNAL_agg_var, "agg_var", cols=cols))
}

#' @name
#' agg_std
#' @title
#' Standard deviation of specified columns by group
#' @md
#'
#' @description
#' Creates a Standard Deviation aggregation that computes the standard deviation of each column in `cols`, for each aggregation group.
#'
#' @details
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `agg_by()` or
#' `agg_all_by()` caller function. The aggregation groups are defined by the unique combinations of values in the `by`
#' columns. For example, if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of
#' values in the `A` and `B` columns.
#'
#' This function, like the other Deephaven `agg_*` functions, is a generator function. That is, its output is another
#' function that is intended to be used in a call to `agg_by()` or `agg_all_by()`. This detail is typically hidden from
#' the user by the `agg_by()` and `agg_all_by()` functions, which call the generated functions internally. However, it is
#' important to understand this detail for debugging purposes, as the output of an `agg_*` function can otherwise seem
#' unexpected.
#'
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to aggregate all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return Aggregation function to be used in `agg_by()` or `agg_all_by()`.
#'
#' @examples
#' print("hello!")
#'
#' @export
agg_std <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(AggOp$new(INTERNAL_agg_std, "agg_std", cols=cols))
}

#' @name
#' agg_percentile
#' @title
#' p-th percentile of specified columns by group
#' @md
#'
#' @description
#' Creates a Percentile aggregation that computes the given percentile of each column in `cols` for each aggregation group.
#'
#' @details
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `agg_by()` or
#' `agg_all_by()` caller function. The aggregation groups are defined by the unique combinations of values in the `by`
#' columns. For example, if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of
#' values in the `A` and `B` columns.
#'
#' This function, like the other Deephaven `agg_*` functions, is a generator function. That is, its output is another
#' function that is intended to be used in a call to `agg_by()` or `agg_all_by()`. This detail is typically hidden from
#' the user by the `agg_by()` and `agg_all_by()` functions, which call the generated functions internally. However, it is
#' important to understand this detail for debugging purposes, as the output of an `agg_*` function can otherwise seem
#' unexpected.
#'
#' @param percentile Numeric scalar between 0 and 1 denoting the percentile to compute.
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to aggregate all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return Aggregation function to be used in `agg_by()` or `agg_all_by()`.
#'
#' @examples
#' print("hello!")
#'
#' @export
agg_percentile <- function(percentile, cols = character()) {
  verify_in_unit_interval("percentile", percentile, TRUE)
  verify_string("cols", cols, FALSE)
  return(AggOp$new(INTERNAL_agg_percentile, "agg_percentile", percentile=percentile, cols=cols))
}

#' @name
#' agg_count
#' @title
#' Number of observations by group
#' @md
#'
#' @description
#' Creates a Count aggregation that counts the number of rows in each aggregation group.
#'
#' @details
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `agg_by()` caller
#' function. The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the `A` and
#' `B` columns.
#'
#' This function, like the other Deephaven `agg_*` functions, is a generator function. That is, its output is another
#' function that is intended to be used in a call to `agg_by()` or `agg_all_by()`. This detail is typically hidden from
#' the user by the `agg_by()` and `agg_all_by()` functions, which call the generated functions internally. However, it is
#' important to understand this detail for debugging purposes, as the output of an `agg_*` function can otherwise seem
#' unexpected.
#'
#' Note that this operation is not supported in `agg_all_by()`.
#'
#' @param col String denoting the name of the new column to hold the counts of each aggregation group.
#' @return Aggregation function to be used in `agg_by()`.
#'
#' @examples
#' print("hello!")
#'
#' @export
agg_count <- function(col) {
  verify_string("col", col, TRUE)
  return(AggOp$new(INTERNAL_agg_count, "agg_count", col=col))
}