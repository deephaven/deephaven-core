Aggregation <- R6Class("Aggregation",
  cloneable = FALSE,
  public = list(
    .internal_rcpp_object = NULL,
    .internal_num_cols = NULL,
    initialize = function(aggregation, ...) {
      args <- list(...)
      if (any(names(args) == "cols")) {
        self$.internal_num_cols <- length(args$cols)
      }
      self$.internal_rcpp_object <- do.call(aggregation, args)
    }
  )
)

### All of the functions below return an instance of the above class

#' @description
#' Create a First aggregation that computes the group-wise first value
#' of each column in `cols`, for each aggregation group.
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to use all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return Aggregation function to be used in `agg_by()` or `agg_all_by()`.
#' @export
agg_first <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(Aggregation$new(INTERNAL_agg_first, cols=cols))
}

#' @description
#' Create a Last aggregation that computes the group-wise last value
#' of each column in `cols`, for each aggregation group.
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to use all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return Aggregation function to be used in `agg_by()` or `agg_all_by()`.
#' @export
agg_last <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(Aggregation$new(INTERNAL_agg_last, cols=cols))
}

#' @description
#' Create a Minimum aggregation that computes the group-wise minimum
#' of each column in `cols`, for each aggregation group.
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to use all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return Aggregation function to be used in `agg_by()` or `agg_all_by()`.
#' @export
agg_min <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(Aggregation$new(INTERNAL_agg_min, cols=cols))
}

#' @description
#' Create a Maximum aggregation that computes the group-wise maximum
#' of each column in `cols`, for each aggregation group.
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to use all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return Aggregation function to be used in `agg_by()` or `agg_all_by()`.
#' @export
agg_max <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(Aggregation$new(INTERNAL_agg_max, cols=cols))
}

#' @description
#' Create a Sum aggregation that computes the group-wise sum
#' of each column in `cols`, for each aggregation group.
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to use all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return Aggregation function to be used in `agg_by()` or `agg_all_by()`.
#' @export
agg_sum <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(Aggregation$new(INTERNAL_agg_sum, cols=cols))
}

#' @description
#' Create a Absolute Sum aggregation that computes the group-wise absolute sum
#' of each column in `cols`, for each aggregation group.
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to use all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return Aggregation function to be used in `agg_by()` or `agg_all_by()`.
#' @export
agg_abs_sum <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(Aggregation$new(INTERNAL_agg_abs_sum, cols=cols))
}

#' @description
#' Create a Average aggregation that computes the group-wise average
#' of each column in `cols`, for each aggregation group.
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to use all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return Aggregation function to be used in `agg_by()` or `agg_all_by()`.
#' @export
agg_avg <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(Aggregation$new(INTERNAL_agg_avg, cols=cols))
}

#' @description
#' Create a Weighted Average aggregation that computes the group-wise weighted average
#' of each column in `cols`, for each aggregation group.
#' @param wcol String denoting the column to use for weights. This must be a numeric column.
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to use all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return Aggregation function to be used in `agg_by()` or `agg_all_by()`.
#' @export
agg_w_avg <- function(wcol, cols = character()) {
  verify_string("wcol", wcol, TRUE)
  verify_string("cols", cols, FALSE)
  return(Aggregation$new(INTERNAL_agg_w_avg, wcol=wcol, cols=cols))
}

#' @description
#' Create a Median aggregation that computes the group-wise median
#' of each column in `cols`, for each aggregation group.
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to use all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return Aggregation function to be used in `agg_by()` or `agg_all_by()`.
#' @export
agg_median <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(Aggregation$new(INTERNAL_agg_median, cols=cols))
}

#' @description
#' Create a Variance aggregation that computes the group-wise variance
#' of each column in `cols`, for each aggregation group.
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to use all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return Aggregation function to be used in `agg_by()` or `agg_all_by()`.
#' @export
agg_var <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(Aggregation$new(INTERNAL_agg_var, cols=cols))
}

#' @description
#' Create a Standard Deviation aggregation that computes the group-wise standard deviation
#' of each column in `cols`, for each aggregation group.
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to use all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return Aggregation function to be used in `agg_by()` or `agg_all_by()`.
#' @export
agg_std <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(Aggregation$new(INTERNAL_agg_std, cols=cols))
}

#' @description
#' Create a Percentile aggregation that computes the group-wise percentile
#' of each column in `cols`, for each aggregation group.
#' @param percentile Numeric scalar between 0 and 1 denoting the percentile to compute.
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to use all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return Aggregation function to be used in `agg_by()` or `agg_all_by()`.
#' @export
agg_percentile <- function(percentile, cols = character()) {
  verify_in_unit_interval("percentile", percentile, TRUE)
  verify_string("cols", cols, FALSE)
  return(Aggregation$new(INTERNAL_agg_percentile, percentile=percentile, cols=cols))
}

#' @description
#' Create a Count aggregation that counts the number of rows in each aggregation group.
#' Note that this operation is not supported in `agg_all_by()`.
#' @param col String denoting the name of the new column to hold the counts of each aggregation group.
#' Defaults to "n".
#' @return Aggregation function to be used in `agg_by()`.
#' @export
agg_count <- function(col = "n") {
  verify_string("col", col, TRUE)
  return(Aggregation$new(INTERNAL_agg_count, col=col))
}
