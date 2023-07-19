#' @export
agg.min = function(columns) {
    return(Aggregation$new(INTERNAL_min(columns)))
}

#' @export
agg.max = function(columns) {
    return(Aggregation$new(INTERNAL_max(columns)))
}

#' @export
agg.sum = function(columns) {
    return(Aggregation$new(INTERNAL_sum(columns)))
}

#' @export
agg.abs_sum = function(columns) {
    return(Aggregation$new(INTERNAL_abs_sum(columns)))
}

#' @export
agg.avg = function(columns) {
    return(Aggregation$new(INTERNAL_avg(columns)))
}

#' @export
agg.w_avg = function(weight_column, columns) {
    return(Aggregation$new(INTERNAL_w_avg(weight_column, columns)))
}

#' @export
agg.var = function(columns) {
    return(Aggregation$new(INTERNAL_var(columns)))
}

#' @export
agg.std = function(columns) {
    return(Aggregation$new(INTERNAL_std(columns)))
}

#' @export
agg.first = function(columns) {
    return(Aggregation$new(INTERNAL_first(columns)))
}

#' @export
agg.last = function(columns) {
    return(Aggregation$new(INTERNAL_last(columns)))
}

#' @export
agg.median = function(columns) {
    return(Aggregation$new(INTERNAL_median(columns)))
}

#' @export
agg.percentile = function(percentile, columns) {
    return(Aggregation$new(INTERNAL_percentile(percentile, columns)))
}

#' @export
agg.count = function(count_column) {
    return(Aggregation$new(INTERNAL_count(count_column)))
}
