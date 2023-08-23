#' @export
Aggregation <- R6Class("Aggregation",
  cloneable = FALSE,
  public = list(
    .internal_rcpp_object = NULL,
    initialize = function(aggregation) {
      if (class(aggregation) != "Rcpp_INTERNAL_Aggregate") {
        stop("'aggregation' should be an internal Deephaven Aggregation. If you're seeing this,\n  you are trying to call the constructor of an Aggregation directly, which is not advised.\n  Please use one of the provided aggregation functions instead.")
      }
      self$.internal_rcpp_object <- aggregation
    }
  )
)

### All of the functions below return an instance of the above class

#' @export
agg_first <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(Aggregation$new(INTERNAL_agg_first(cols)))
}

#' @export
agg_last <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(Aggregation$new(INTERNAL_agg_last(cols)))
}

#' @export
agg_min <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(Aggregation$new(INTERNAL_agg_min(cols)))
}

#' @export
agg_max <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(Aggregation$new(INTERNAL_agg_max(cols)))
}

#' @export
agg_sum <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(Aggregation$new(INTERNAL_agg_sum(cols)))
}

#' @export
agg_abs_sum <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(Aggregation$new(INTERNAL_agg_abs_sum(cols)))
}

#' @export
agg_avg <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(Aggregation$new(INTERNAL_agg_avg(cols)))
}

#' @export
agg_w_avg <- function(wcol, cols = character()) {
  verify_string("wcol", wcol, TRUE)
  verify_string("cols", cols, FALSE)
  return(Aggregation$new(INTERNAL_agg_w_avg(wcol, cols)))
}

#' @export
agg_median <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(Aggregation$new(INTERNAL_agg_median(cols)))
}

#' @export
agg_var <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(Aggregation$new(INTERNAL_agg_var(cols)))
}

#' @export
agg_std <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(Aggregation$new(INTERNAL_agg_std(cols)))
}

#' @export
agg_percentile <- function(percentile, cols = character()) {
  verify_in_unit_interval("percentile", percentile, TRUE)
  verify_string("cols", cols, FALSE)
  return(Aggregation$new(INTERNAL_agg_percentile(percentile, cols)))
}

#' @export
agg_count <- function(col) {
  verify_string("col", col, TRUE)
  return(Aggregation$new(INTERNAL_agg_count(col)))
}
