### S3 GENERICS
# The following generics do not already exist in the dplyr suite, so we must define them in order to implement the
# corresponding methods for TableHandles, so that the Deephaven query language feels identical to the dplyr experience.

#' @export
view <- function(x, ...) {UseMethod("view", x)}
#' @export
update_view <- function(x, ...) {UseMethod("update_view", x)}
#' @export
drop_columns <- function(x, ...) {UseMethod("drop_columns", x)}
#' @export
where <- function(x, ...) {UseMethod("where", x)}

#' @export
abs_sum_by <- function(x, ...) {UseMethod("abs_sum_by", x)}
#' @export
agg_by <- function(x, ...) {UseMethod("agg_by", x)}
#' @export
avg_by <- function(x, ...) {UseMethod("avg_by", x)}
#' @export
count_by <- function(x, ...) {UseMethod("count_by", x)}
#' @export
first_by <- function(x, ...) {UseMethod("first_by", x)}
#' @export
head_by <- function(x, ...) {UseMethod("head_by", x)}
#' @export
last_by <- function(x, ...) {UseMethod("last_by", x)}
#' @export
max_by <- function(x, ...) {UseMethod("max_by", x)}
#' @export
median_by <- function(x, ...) {UseMethod("median_by", x)}
#' @export
min_by <- function(x, ...) {UseMethod("min_by", x)}
#' @export
percentile_by <- function(x, ...) {UseMethod("percentile_by", x)}
#' @export
std_by <- function(x, ...) {UseMethod("std_by", x)}
#' @export
sum_by <- function(x, ...) {UseMethod("sum_by", x)}
#' @export
tail_by <- function(x, ...) {UseMethod("tail_by", x)}
#' @export
var_by <- function(x, ...) {UseMethod("var_by", x)}
#' @export
w_avg_by <- function(x, ...) {UseMethod("w_avg_by", x)}

#' @export
cross_join <- function(x, ...) {UseMethod("cross_join", x)}
#' @export
natural_join <- function(x, ...) {UseMethod("natural_join", x)}
#' @export
exact_join <- function(x, ...) {UseMethod("exact_join", x)}

# TODO: figure this shit out
#' @export
dh_ungroup <- function(x, ...) {UseMethod("dh_ungroup", x)}
#' @export
dh_sort <- function(x, ...) {UseMethod("dh_sort", x)}
#' @export
dh_merge <- function(x, ...) {UseMethod("dh_merge", x)}
#' @export
dh_cross_join <- function(x, ...) {UseMethod("dh_cross_join", x)}


### S3 METHODS (implemented generics)

#' @export
select.TableHandle <- function(th, columns) {
    verify_string_vector("columns", columns)
    TableHandle$new(th$internal_table_handle$select(columns))
}

#' @export
view.TableHandle <- function(th, columns) {
    verify_string_vector("columns", columns)
    return(TableHandle$new(th$internal_table_handle$view(columns)))
}

#' @export
update.TableHandle <- function(th, columns) {
    verify_string_vector("columns", columns)
    return(TableHandle$new(th$internal_table_handle$update(columns)))
}

#' @export
update_view.TableHandle <- function(th, columns) {
    verify_string_vector("columns", columns)
    return(TableHandle$new(th$internal_table_handle$update_view(columns)))
}

#' @export
drop_columns.TableHandle <- function(th, columns) {
    verify_string_vector("columns", columns)
    return(TableHandle$new(th$internal_table_handle$drop_columns(columns)))
}

#' @export
where.TableHandle = function(th, condition) {
    verify_string("condition", condition)
    return(TableHandle$new(th$internal_table_handle$where(condition)))
}

# AGGREGATION OPERATIONS

#' @export
agg_by.TableHandle = function(th, aggregations) {
    verify_internal_type("Aggregation", "aggregations", aggregations)
    if ((first_class(aggregations) != "list") && (first_class(aggregations) == "Aggregation")) {
        aggregations = c(aggregations)
    }
    unwrapped_aggregations = lapply(aggregations, strip_r6_wrapping_from_aggregation)

    return(TableHandle$new(th$internal_table_handle$agg_by(unwrapped_aggregations)))
}

#' @export
min_by.TableHandle = function(th, columns) {
    verify_string_vector("columns", columns)
    return(TableHandle$new(th$internal_table_handle$min_by(columns)))
}

#' @export
max_by.TableHandle = function(th, columns) {
    verify_string_vector("columns", columns)
    return(TableHandle$new(th$internal_table_handle$max_by(columns)))
}

#' @export
sum_by.TableHandle = function(th, columns) {
    verify_string_vector("columns", columns)
    return(TableHandle$new(th$internal_table_handle$sum_by(columns)))
}

#' @export
abs_sum_by.TableHandle = function(th, columns) {
    verify_string_vector("columns", columns)
    return(TableHandle$new(th$internal_table_handle$abs_sum_by(columns)))
}

#' @export
avg_by.TableHandle = function(th, columns) {
    verify_string_vector("columns", columns)
    return(TableHandle$new(th$internal_table_handle$avg_by(columns)))
}

#' @export
w_avg_by.TableHandle = function(th, weight_column, columns) {
    verify_string("weight_column", weight_column)
    verify_string_vector("columns", columns)
    return(TableHandle$new(th$internal_table_handle$w_avg_by(weight_column, columns)))
}

#' @export
var_by.TableHandle = function(th, columns) {
    verify_string_vector("columns", columns)
    return(TableHandle$new(th$internal_table_handle$var_by(columns)))
}

#' @export
std_by.TableHandle = function(th, columns) {
    verify_string_vector("columns", columns)
    return(TableHandle$new(th$internal_table_handle$std_by(columns)))
}

#' @export
first_by.TableHandle = function(th, columns) {
    verify_string_vector("columns", columns)
    return(TableHandle$new(th$internal_table_handle$first_by(columns)))
}

#' @export
last_by.TableHandle = function(th, columns) {
    verify_string_vector("columns", columns)
    return(TableHandle$new(th$internal_table_handle$last_by(columns)))
}

#' @export
median_by.TableHandle = function(th, columns) {
    verify_string_vector("columns", columns)
    return(TableHandle$new(th$internal_table_handle$median_by(columns)))
}

#' @export
percentile_by.TableHandle = function(th, columns, percentile) {
    verify_string_vector("columns", columns)
    verify_proportion("percentile", percentile)
    return(TableHandle$new(th$internal_table_handle$percentile_by(columns, percentile)))
}

#' @export
count_by.TableHandle = function(th, columns, count_by_column) {
    verify_string("count_by_column", count_by_column)
    verify_string_vector("columns", columns)
    return(TableHandle$new(th$internal_table_handle$count_by(count_by_column, columns)))
}

#' @export
head_by.TableHandle = function(th, columns, n) {
    verify_int("n", n)
    verify_string_vector("columns", columns)
    return(TableHandle$new(th$internal_table_handle$head_by(n, columns)))
}

#' @export
tail_by.TableHandle = function(th, columns, n) {
    verify_int("n", n)
    verify_string_vector("columns", columns)
    return(TableHandle$new(th$internal_table_handle$tail_by(n, columns)))
}

# JOIN OPERATIONS

#' @export
dh_cross_join.TableHandle = function(th, right_side, columns_to_match, columns_to_add) {
    verify_string_vector("columns_to_match", columns_to_match)
    verify_string_vector("columns_to_add", columns_to_add)
    return(TableHandle$new(th$internal_table_handle$cross_join(right_side$internal_table_handle,
                                                                    columns_to_match, columns_to_add)))
}

#' @export
natural_join.TableHandle = function(th, right_side, columns_to_match, columns_to_add) {
    verify_string_vector("columns_to_match", columns_to_match)
    verify_string_vector("columns_to_add", columns_to_add)
    return(TableHandle$new(th$internal_table_handle$natural_join(right_side$internal_table_handle,
                                                                      columns_to_match, columns_to_add)))
}

#' @export
exact_join.TableHandle = function(th, right_side, columns_to_match, columns_to_add) {
    verify_string_vector("columns_to_match", columns_to_match)
    verify_string_vector("columns_to_add", columns_to_add)
    return(TableHandle$new(th$internal_table_handle$exact_join(right_side$internal_table_handle,
                                                                    columns_to_match, columns_to_add)))
}

# MISC OPERATIONS

#' @export
head.TableHandle = function(th, n) {
    verify_int("n", n)
    return(TableHandle$new(th$internal_table_handle$head(n)))
}

#' @export
tail.TableHandle = function(th, n) {
    verify_int("n", n)
    return(TableHandle$new(th$internal_table_handle$tail(n)))
}

#' @export
dh_ungroup.TableHandle = function(th, null_fill, group_by_columns) {
    verify_string_vector(group_by_columns, "group_by_columns")
    return(TableHandle$new(th$internal_table_handle$ungroup(null_fill, group_by_columns)))
}

#' @export
dh_sort.TableHandle = function(th, sorters) {
    verify_internal_type("Sorter", "sorters", sorters)
    if ((first_class(sorters) != "list") && (first_class(sorters) == "Sorter")) {
        sorters = c(sorters)
    }
    unwrapped_sorters = lapply(sorters, strip_r6_wrapping_from_sorter)
    return(TableHandle$new(th$internal_table_handle$sort(unwrapped_sorters)))
}

#' @export
dh_merge.TableHandle = function(th, key_column, sources) {
    verify_string("key_column", key_column)
    return(TableHandle$new(th$internal_table_handle$merge(key_column, sources)))
}