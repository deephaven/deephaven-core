#' @export
agg.min = function(columns) {
    verify_string_vector(columns, "columns")
    return(Aggregation$new(INTERNAL_min(columns)))
}

#' @export
agg.max = function(columns) {
    verify_string_vector(columns, "columns")
    return(Aggregation$new(INTERNAL_max(columns)))
}

#' @export
agg.sum = function(columns) {
    verify_string_vector(columns, "columns")
    return(Aggregation$new(INTERNAL_sum(columns)))
}

#' @export
agg.abs_sum = function(columns) {
    verify_string_vector(columns, "columns")
    return(Aggregation$new(INTERNAL_abs_sum(columns)))
}

#' @export
agg.avg = function(columns) {
    verify_string_vector(columns, "columns")
    return(Aggregation$new(INTERNAL_avg(columns)))
}

#' @export
agg.w_avg = function(weight_column, columns) {
    verify_string(weight_column, "weight_column")
    verify_string_vector(columns, "columns")
    return(Aggregation$new(INTERNAL_w_avg(weight_column, columns)))
}

#' @export
agg.var = function(columns) {
    verify_string_vector(columns, "columns")
    return(Aggregation$new(INTERNAL_var(columns)))
}

#' @export
agg.std = function(columns) {
    verify_string_vector(columns, "columns")
    return(Aggregation$new(INTERNAL_std(columns)))
}

#' @export
agg.first = function(columns) {
    verify_string_vector(columns, "columns")
    return(Aggregation$new(INTERNAL_first(columns)))
}

#' @export
agg.last = function(columns) {
    verify_string_vector(columns, "columns")
    return(Aggregation$new(INTERNAL_last(columns)))
}

#' @export
agg.median = function(columns) {
    verify_string_vector(columns, "columns")
    return(Aggregation$new(INTERNAL_median(columns)))
}

#' @export
agg.percentile = function(percentile, columns) {
    # TODO: type check percentile
    verify_string_vector(columns, "columns")
    return(Aggregation$new(INTERNAL_percentile(percentile, columns)))
}

#' @export
agg.count = function(count_column) {
    verify_string(count_column, "count_column")
    return(Aggregation$new(INTERNAL_count(count_column)))
}
