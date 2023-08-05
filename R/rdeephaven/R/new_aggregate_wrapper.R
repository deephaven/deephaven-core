#' @export
setClass(
  "Aggregation",
  representation(
    .internal_rcpp_object = "Rcpp_INTERNAL_Aggregate"
  )
)

### All of the functions below return an instance of the above class

#' @export
agg_first <- function(columns = character()) {
  verify_string("columns", columns, FALSE)
  return(new("Aggregation", .internal_rcpp_object = INTERNAL_agg_first(columns)))
}

#' @export
agg_last <- function(columns = character()) {
  verify_string("columns", columns, FALSE)
  return(new("Aggregation", .internal_rcpp_object = INTERNAL_agg_last(columns)))
}

#' @export
agg_min <- function(columns = character()) {
  verify_string("columns", columns, FALSE)
  return(new("Aggregation", .internal_rcpp_object = INTERNAL_agg_min(columns)))
}

#' @export
agg_max <- function(columns = character()) {
  verify_string("columns", columns, FALSE)
  return(new("Aggregation", .internal_rcpp_object = INTERNAL_agg_max(columns)))
}

#' @export
agg_sum <- function(columns = character()) {
  verify_string("columns", columns, FALSE)
  return(new("Aggregation", .internal_rcpp_object = INTERNAL_agg_sum(columns)))
}

#' @export
agg_abs_sum <- function(columns = character()) {
  verify_string("columns", columns, FALSE)
  return(new("Aggregation", .internal_rcpp_object = INTERNAL_agg_abs_sum(columns)))
}

#' @export
agg_avg <- function(columns = character()) {
  verify_string("columns", columns, FALSE)
  return(new("Aggregation", .internal_rcpp_object = INTERNAL_agg_avg(columns)))
}

#' @export
agg_w_avg <- function(weight_column, columns = character()) {
  verify_string("weight_column", weight_column, TRUE)
  verify_string("columns", columns, FALSE)
  return(new("Aggregation", .internal_rcpp_object = INTERNAL_agg_w_avg(weight_column, columns)))
}

#' @export
agg_median <- function(columns = character()) {
  verify_string("columns", columns, FALSE)
  return(new("Aggregation", .internal_rcpp_object = INTERNAL_agg_median(columns)))
}

#' @export
agg_var <- function(columns = character()) {
  verify_string("columns", columns, FALSE)
  return(new("Aggregation", .internal_rcpp_object = INTERNAL_agg_var(columns)))
}

#' @export
agg_std <- function(columns = character()) {
  verify_string("columns", columns, FALSE)
  return(new("Aggregation", .internal_rcpp_object = INTERNAL_agg_std(columns)))
}

#' @export
agg_percentile <- function(percentile, columns = character()) {
  verify_in_unit_interval("percentile", percentile, TRUE)
  verify_string("columns", columns, FALSE)
  return(new("Aggregation", .internal_rcpp_object = INTERNAL_agg_percentile(percentile, columns)))
}

#' @export
agg_count <- function(count_column) {
  verify_string("count_column", count_column, TRUE)
  return(new("Aggregation", .internal_rcpp_object = INTERNAL_agg_count(count_column)))
}