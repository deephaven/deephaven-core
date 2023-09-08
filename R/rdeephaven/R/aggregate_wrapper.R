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
#' Aggregation function that computes the first value of each column in `cols`, for each aggregation group.
#' Intended to be used in a call to `TableHandle$agg_by()`.
#' @param cols String or list of strings denoting the names of the columns to aggregate.
#' @return Result of the aggregation.
#' @export
agg_first <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(Aggregation$new(INTERNAL_agg_first, cols=cols))
}

#' @description
#' Aggregation function that computes the last value of each column in `cols`, for each aggregation group.
#' @param cols String or list of strings denoting the names of the columns to aggregate.
#' @return Result of the aggregation.
#' @export
agg_last <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(Aggregation$new(INTERNAL_agg_last, cols=cols))
}

#' @description
#' Aggregation function that computes the minimum value of each column in `cols`, for each aggregation group.
#' @param cols String or list of strings denoting the names of the columns to aggregate.
#' @return Result of the aggregation.
#' @export
agg_min <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(Aggregation$new(INTERNAL_agg_min, cols=cols))
}

#' @description
#' Aggregation function that computes the maximum value of each column in `cols`, for each aggregation group.
#' @param cols String or list of strings denoting the names of the columns to aggregate.
#' @return Result of the aggregation.
#' @export
agg_max <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(Aggregation$new(INTERNAL_agg_max, cols=cols))
}

#' @description
#' Aggregation function that computes the sum of each column in `cols`, for each aggregation group.
#' @param cols String or list of strings denoting the names of the columns to aggregate.
#' @return Result of the aggregation.
#' @export
agg_sum <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(Aggregation$new(INTERNAL_agg_sum, cols=cols))
}

#' @description
#' Aggregation function that computes the absolute sum of each column in `cols`, for each aggregation group.
#' @param cols String or list of strings denoting the names of the columns to aggregate.
#' @return Result of the aggregation.
#' @export
agg_abs_sum <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(Aggregation$new(INTERNAL_agg_abs_sum, cols=cols))
}

#' @description
#' Aggregation function that computes the average of each column in `cols`, for each aggregation group.
#' @param cols String or list of strings denoting the names of the columns to aggregate.
#' @return Result of the aggregation.
#' @export
agg_avg <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(Aggregation$new(INTERNAL_agg_avg, cols=cols))
}

#' @description
#' Aggregation function that computes the weighted average of each column in `cols`, for each aggregation group.
#' @param wcol String denoting the name of the column to use as weights.
#' @param cols String or list of strings denoting the names of the columns to aggregate.
#' @return Result of the aggregation.
#' @export
agg_w_avg <- function(wcol, cols = character()) {
  verify_string("wcol", wcol, TRUE)
  verify_string("cols", cols, FALSE)
  return(Aggregation$new(INTERNAL_agg_w_avg, wcol=wcol, cols=cols))
}

#' @description
#' Aggregation function that computes the median of each column in `cols`, for each aggregation group.
#' @param cols String or list of strings denoting the names of the columns to aggregate.
#' @return Result of the aggregation.
#' @export
agg_median <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(Aggregation$new(INTERNAL_agg_median, cols=cols))
}

#' @description
#' Aggregation function that computes the variance of each column in `cols`, for each aggregation group.
#' @param cols String or list of strings denoting the names of the columns to aggregate.
#' @return Result of the aggregation.
#' @export
agg_var <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(Aggregation$new(INTERNAL_agg_var, cols=cols))
}

#' @description
#' Aggregation function that computes the standard deviation of each column in `cols`, for each aggregation group.
#' @param cols String or list of strings denoting the names of the columns to aggregate.
#' @return Result of the aggregation.
#' @export
agg_std <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(Aggregation$new(INTERNAL_agg_std, cols=cols))
}

#' @description
#' Aggregation function that computes the p-th percentile of each column in `cols`, for each aggregation group.
#' @param percentile Numeric scalar between 0 and 1 denoting the percentile to compute.
#' @param cols String or list of strings denoting the names of the columns to aggregate.
#' @return Result of the aggregation.
#' @export
agg_percentile <- function(percentile, cols = character()) {
  verify_in_unit_interval("percentile", percentile, TRUE)
  verify_string("cols", cols, FALSE)
  return(Aggregation$new(INTERNAL_agg_percentile, percentile=percentile, cols=cols))
}

#' @description
#' Aggregation function that counts the number of rows for each aggregation group.
#' @param col String denoting the name of the new column to be created by counting entries in each group.
#' @return Result of the aggregation.
#' @export
agg_count <- function(col) {
  verify_string("col", col, TRUE)
  return(Aggregation$new(INTERNAL_agg_count, col=col))
}
