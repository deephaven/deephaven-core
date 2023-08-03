#' @export
setClass(
  "S4Aggregation",
  representation(
    .internal_rcpp_object = "Rcpp_INTERNAL_Aggregate"
  )
)

### All of the functions below return an instance of the above class

#' @export
s4_agg_first <- function(columns = character()) {
  verify_string("columns", columns, FALSE)
  return(new("S4Aggregation", .internal_rcpp_object = INTERNAL_first(columns)))
}

#' @export
s4_agg_last <- function(columns = character()) {
  verify_string("columns", columns, FALSE)
  return(new("S4Aggregation", .internal_rcpp_object = INTERNAL_last(columns)))
}

#' @export
s4_agg_min <- function(columns = character()) {
  verify_string("columns", columns, FALSE)
  return(new("S4Aggregation", .internal_rcpp_object = INTERNAL_min(columns)))
}

#' @export
s4_agg_max <- function(columns = character()) {
  verify_string("columns", columns, FALSE)
  return(new("S4Aggregation", .internal_rcpp_object = INTERNAL_max(columns)))
}

#' @export
s4_agg_sum <- function(columns = character()) {
  verify_string("columns", columns, FALSE)
  return(new("S4Aggregation", .internal_rcpp_object = INTERNAL_sum(columns)))
}

#' @export
s4_agg_abs_sum <- function(columns = character()) {
  verify_string("columns", columns, FALSE)
  return(new("S4Aggregation", .internal_rcpp_object = INTERNAL_abs_sum(columns)))
}

#' @export
s4_agg_avg <- function(columns = character()) {
  verify_string("columns", columns, FALSE)
  return(new("S4Aggregation", .internal_rcpp_object = INTERNAL_avg(columns)))
}

#' @export
s4_agg_w_avg <- function(weight_column, columns = character()) {
  verify_string("weight_column", weight_column, TRUE)
  verify_string("columns", columns, FALSE)
  return(new("S4Aggregation", .internal_rcpp_object = INTERNAL_w_avg(weight_column, columns)))
}

#' @export
s4_agg_median <- function(columns = character()) {
  verify_string("columns", columns, FALSE)
  return(new("S4Aggregation", .internal_rcpp_object = INTERNAL_median(columns)))
}

#' @export
s4_agg_var <- function(columns = character()) {
  verify_string("columns", columns, FALSE)
  return(new("S4Aggregation", .internal_rcpp_object = INTERNAL_var(columns)))
}

#' @export
s4_agg_std <- function(columns = character()) {
  verify_string("columns", columns, FALSE)
  return(new("S4Aggregation", .internal_rcpp_object = INTERNAL_std(columns)))
}

#' @export
s4_agg_percentile <- function(percentile, columns = character()) {
  verify_in_unit_interval("percentile", percentile, TRUE)
  verify_string("columns", columns, FALSE)
  return(new("S4Aggregation", .internal_rcpp_object = INTERNAL_percentile(percentile, columns)))
}

#' @export
s4_agg_count <- function(count_column) {
  verify_string("count_column", count_column, TRUE)
  return(new("S4Aggregation", .internal_rcpp_object = INTERNAL_count(count_column)))
}