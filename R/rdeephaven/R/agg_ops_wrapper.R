#' @description
#' An AggOp represents an aggregation operation that can be passed to `agg_by()` or `agg_all_by()`.
#' Note that AggOps should not be instantiated directly by user code, but rather by provided agg_* functions.
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

### All of the functions below return an instance of an 'AggOp' object

#' @description
#' Creates a First aggregation that computes the first value of each column in `cols` for each aggregation group.
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to aggregate all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return Aggregation function to be used in `agg_by()` or `agg_all_by()`.
#' @export
agg_first <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(AggOp$new(INTERNAL_agg_first, "agg_first", cols=cols))
}

#' @description
#' Creates a Last aggregation that computes the last value of each column in `cols` for each aggregation group.
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to aggregate all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return Aggregation function to be used in `agg_by()` or `agg_all_by()`.
#' @export
agg_last <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(AggOp$new(INTERNAL_agg_last, "agg_last", cols=cols))
}

#' @description
#' Creates a Minimum aggregation that computes the minimum of each column in `cols` for each aggregation group.
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to aggregate all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return Aggregation function to be used in `agg_by()` or `agg_all_by()`.
#' @export
agg_min <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(AggOp$new(INTERNAL_agg_min, "agg_min", cols=cols))
}

#' @description
#' Creates a Maximum aggregation that computes the maximum of each column in `cols` for each aggregation group.
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to aggregate all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return Aggregation function to be used in `agg_by()` or `agg_all_by()`.
#' @export
agg_max <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(AggOp$new(INTERNAL_agg_max, "agg_max", cols=cols))
}

#' @description
#' Creates a Sum aggregation that computes the sum of each column in `cols` for each aggregation group.
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to aggregate all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return Aggregation function to be used in `agg_by()` or `agg_all_by()`.
#' @export
agg_sum <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(AggOp$new(INTERNAL_agg_sum, "agg_sum", cols=cols))
}

#' @description
#' Creates an Absolute Sum aggregation that computes the absolute sum of each column in `cols` for each aggregation group.
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to aggregate all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return Aggregation function to be used in `agg_by()` or `agg_all_by()`.
#' @export
agg_abs_sum <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(AggOp$new(INTERNAL_agg_abs_sum, "agg_abs_sum", cols=cols))
}

#' @description
#' Creates an Average aggregation that computes the average of each column in `cols` for each aggregation group.
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to aggregate all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return Aggregation function to be used in `agg_by()` or `agg_all_by()`.
#' @export
agg_avg <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(AggOp$new(INTERNAL_agg_avg, "agg_avg", cols=cols))
}

#' @description
#' Creates a Weighted Average aggregation that computes the weighted average of each column in `cols` for each aggregation group.
#' @param wcol String denoting the column to use for weights. This must be a numeric column.
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to aggregate all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return Aggregation function to be used in `agg_by()` or `agg_all_by()`.
#' @export
agg_w_avg <- function(wcol, cols = character()) {
  verify_string("wcol", wcol, TRUE)
  verify_string("cols", cols, FALSE)
  return(AggOp$new(INTERNAL_agg_w_avg, "agg_w_avg", wcol=wcol, cols=cols))
}

#' @description
#' Creates a Median aggregation that computes the median of each column in `cols` for each aggregation group.
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to aggregate all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return Aggregation function to be used in `agg_by()` or `agg_all_by()`.
#' @export
agg_median <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(AggOp$new(INTERNAL_agg_median, "agg_median", cols=cols))
}

#' @description
#' Creates a Variance aggregation that computes the variance of each column in `cols` for each aggregation group.
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to aggregate all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return Aggregation function to be used in `agg_by()` or `agg_all_by()`.
#' @export
agg_var <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(AggOp$new(INTERNAL_agg_var, "agg_var", cols=cols))
}

#' @description
#' Creates a Standard Deviation aggregation that computes the standard deviation of each column in `cols`, for each aggregation group.
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to aggregate all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return Aggregation function to be used in `agg_by()` or `agg_all_by()`.
#' @export
agg_std <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(AggOp$new(INTERNAL_agg_std, "agg_std", cols=cols))
}

#' @description
#' Creates a Percentile aggregation that computes the given percentile of each column in `cols` for each aggregation group.
#' @param percentile Numeric scalar between 0 and 1 denoting the percentile to compute.
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to aggregate all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return Aggregation function to be used in `agg_by()` or `agg_all_by()`.
#' @export
agg_percentile <- function(percentile, cols = character()) {
  verify_in_unit_interval("percentile", percentile, TRUE)
  verify_string("cols", cols, FALSE)
  return(AggOp$new(INTERNAL_agg_percentile, "agg_percentile", percentile=percentile, cols=cols))
}

#' @description
#' Creates a Count aggregation that counts the number of rows in each aggregation group.
#' Note that this operation is not supported in `agg_all_by()`.
#' @param col String denoting the name of the new column to hold the counts of each aggregation group.
#' @return Aggregation function to be used in `agg_by()`.
#' @export
agg_count <- function(col) {
  verify_string("col", col, TRUE)
  return(AggOp$new(INTERNAL_agg_count, "agg_count", col=col))
}