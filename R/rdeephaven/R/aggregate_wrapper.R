#' @export
Aggregation <- R6Class("Aggregation",
  cloneable = FALSE,
  public = list(

    #' @description
    #' Create an Aggregation instance.
    initialize = function(aggregation) {
      if (class(aggregation) != "Rcpp_INTERNAL_Aggregate") {
        stop("'aggregation' should be an internal Deephaven Aggregation. If you're seeing this,\n  you are trying to call the constructor of an Aggregation directly, which is not advised.\n  Please use one of the provided aggregation functions instead.")
      }
      self$internal_aggregation <- aggregation
    },
    internal_aggregation = NULL
  )
)

### All of the functions below return an instance of the above class

#' @export
agg_first <- function(columns = character()) {
  verify_string_vector("columns", columns)
  return(Aggregation$new(INTERNAL_first(columns)))
}

#' @export
agg_last <- function(columns = character()) {
  verify_string_vector("columns", columns)
  return(Aggregation$new(INTERNAL_last(columns)))
}

#' @export
agg_min <- function(columns = character()) {
  verify_string_vector("columns", columns)
  return(Aggregation$new(INTERNAL_min(columns)))
}

#' @export
agg_max <- function(columns = character()) {
  verify_string_vector("columns", columns)
  return(Aggregation$new(INTERNAL_max(columns)))
}

#' @export
agg_sum <- function(columns = character()) {
  verify_string_vector("columns", columns)
  return(Aggregation$new(INTERNAL_sum(columns)))
}

#' @export
agg_abs_sum <- function(columns = character()) {
  verify_string_vector("columns", columns)
  return(Aggregation$new(INTERNAL_abs_sum(columns)))
}

#' @export
agg_avg <- function(columns = character()) {
  verify_string_vector("columns", columns)
  return(Aggregation$new(INTERNAL_avg(columns)))
}

#' @export
agg_w_avg <- function(weight_column, columns = character()) {
  verify_string("weight_column", weight_column)
  verify_string_vector("columns", columns)
  return(Aggregation$new(INTERNAL_w_avg(weight_column, columns)))
}

#' @export
agg_median <- function(columns = character()) {
  verify_string_vector("columns", columns)
  return(Aggregation$new(INTERNAL_median(columns)))
}

#' @export
agg_var <- function(columns = character()) {
  verify_string_vector("columns", columns)
  return(Aggregation$new(INTERNAL_var(columns)))
}

#' @export
agg_std <- function(columns = character()) {
  verify_string_vector("columns", columns)
  return(Aggregation$new(INTERNAL_std(columns)))
}

#' @export
agg_percentile <- function(percentile, columns = character()) {
  verify_proportion("percentile", percentile)
  verify_string_vector("columns", columns)
  return(Aggregation$new(INTERNAL_percentile(percentile, columns)))
}

#' @export
agg_count <- function(count_column) {
  verify_string("count_column", count_column)
  return(Aggregation$new(INTERNAL_count(count_column)))
}
