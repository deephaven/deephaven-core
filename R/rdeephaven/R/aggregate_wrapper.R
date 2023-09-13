Aggregation <- R6Class("Aggregation",
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

### All of the functions below return an instance of the above class

agg_first <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(Aggregation$new(INTERNAL_agg_first, "agg_first", cols=cols))
}

agg_last <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(Aggregation$new(INTERNAL_agg_last, "agg_last", cols=cols))
}

agg_min <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(Aggregation$new(INTERNAL_agg_min, "agg_min", cols=cols))
}

agg_max <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(Aggregation$new(INTERNAL_agg_max, "agg_max", cols=cols))
}

agg_sum <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(Aggregation$new(INTERNAL_agg_sum, "agg_sum", cols=cols))
}

agg_abs_sum <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(Aggregation$new(INTERNAL_agg_abs_sum, "agg_abs_sum", cols=cols))
}

agg_avg <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(Aggregation$new(INTERNAL_agg_avg, "agg_avg", cols=cols))
}

agg_w_avg <- function(wcol, cols = character()) {
  verify_string("wcol", wcol, TRUE)
  verify_string("cols", cols, FALSE)
  return(Aggregation$new(INTERNAL_agg_w_avg, "agg_w_avg", wcol=wcol, cols=cols))
}

agg_median <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(Aggregation$new(INTERNAL_agg_median, "agg_median", cols=cols))
}

agg_var <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(Aggregation$new(INTERNAL_agg_var, "agg_var", cols=cols))
}

agg_std <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(Aggregation$new(INTERNAL_agg_std, "agg_std", cols=cols))
}

agg_percentile <- function(percentile, cols = character()) {
  verify_in_unit_interval("percentile", percentile, TRUE)
  verify_string("cols", cols, FALSE)
  return(Aggregation$new(INTERNAL_agg_percentile, "agg_percentile", percentile=percentile, cols=cols))
}

agg_count <- function(col) {
  verify_string("col", col, TRUE)
  return(Aggregation$new(INTERNAL_agg_count, "agg_count", col=col))
}