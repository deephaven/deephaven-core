#' @export
agg.min = function(columns) {
    return(INTERNAL_min(columns))
}

#' @export
agg.max = function(columns) {
    return(INTERNAL_max(columns))
}

#' @export
agg.sum = function(columns) {
    return(INTERNAL_sum(columns))
}

#' @export
agg.abs_sum = function(columns) {
    return(INTERNAL_abs_sum(columns))
}

#' @export
agg.avg = function(columns) {
    return(INTERNAL_avg(columns))
}

#' @export
agg.w_avg = function(weight_column, columns) {
    return(INTERNAL_w_avg(weight_column, columns))
}

#' @export
agg.var = function(columns) {
    return(INTERNAL_var(columns))
}

#' @export
agg.std = function(columns) {
    return(INTERNAL_std(columns))
}

#' @export
agg.first = function(columns) {
    return(INTERNAL_first(columns))
}

#' @export
agg.last = function(columns) {
    return(INTERNAL_last(columns))
}

#' @export
agg.median = function(columns) {
    return(INTERNAL_median(columns))
}

#' @export
agg.percentile = function(percentile, columns) {
    return(INTERNAL_percentile(percentile, columns))
}

#' @export
agg.count = function(count_column) {
    return(INTERNAL_count(count_column))
}
