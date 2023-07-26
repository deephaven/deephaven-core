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

# TODO: figure this shit out
#' @export
dh_ungroup <- function(x, ...) {UseMethod("dh_ungroup", x)}
#' @export
dh_merge <- function(x, ...) {UseMethod("dh_merge", x)}

### S3 METHODS (implemented generics)

#' @export
select.TableHandle <- function(th, columns = character()) {
    verify_string_vector("columns", columns)
    TableHandle$new(th$internal_table_handle$select(columns))
}

#' @export
view.TableHandle <- function(th, columns = character()) {
    verify_string_vector("columns", columns)
    return(TableHandle$new(th$internal_table_handle$view(columns)))
}

#' @export
update.TableHandle <- function(th, columns = character()) {
    verify_string_vector("columns", columns)
    return(TableHandle$new(th$internal_table_handle$update(columns)))
}

#' @export
update_view.TableHandle <- function(th, columns = character()) {
    verify_string_vector("columns", columns)
    return(TableHandle$new(th$internal_table_handle$update_view(columns)))
}

#' @export
drop_columns.TableHandle <- function(th, columns = character()) {
    verify_string_vector("columns", columns)
    return(TableHandle$new(th$internal_table_handle$drop_columns(columns)))
}

#' @export
where.TableHandle <- function(th, condition) {
    verify_string("condition", condition)
    return(TableHandle$new(th$internal_table_handle$where(condition)))
}

# AGGREGATION OPERATIONS

#' @export
group_by <- function(th, group_by_columns = character()) {
    verify_string_vector("group_by_columns", group_by_columns)
    return(TableHandle$new(th$internal_table_handle$group_by(group_by_columns)))
}

#' @export
ungroup <- function(th, group_by_columns = character()) {
    verify_string_vector("group_by_columns", group_by_columns)
    return(TableHandle$new(th$internal_table_handle$ungroup(group_by_columns)))
}

#' @export
agg_by <- function(th, aggregations, group_by_columns = character()) {
    verify_internal_type("Aggregation", "aggregations", aggregations)
    verify_string_vector("group_by_columns", group_by_columns)
    aggregations = c(aggregations)
    unwrapped_aggregations = lapply(aggregations, strip_r6_wrapping_from_aggregation)
    return(TableHandle$new(th$internal_table_handle$agg_by(unwrapped_aggregations, group_by_columns)))
}

#' @export
first_by <- function(th, columns = character()) {
    verify_string_vector("columns", columns)
    return(TableHandle$new(th$internal_table_handle$first_by(columns)))
}

#' @export
last_by <- function(th, columns = character()) {
    verify_string_vector("columns", columns)
    return(TableHandle$new(th$internal_table_handle$last_by(columns)))
}

#' @export
head_by <- function(th, n, columns = character()) {
    verify_int("n", n)
    verify_string_vector("columns", columns)
    return(TableHandle$new(th$internal_table_handle$head_by(n, columns)))
}

#' @export
tail_by <- function(th, n, columns = character()) {
    verify_int("n", n)
    verify_string_vector("columns", columns)
    return(TableHandle$new(th$internal_table_handle$tail_by(n, columns)))
}

#' @export
min_by <- function(th, columns = character()) {
    verify_string_vector("columns", columns)
    return(TableHandle$new(th$internal_table_handle$min_by(columns)))
}

#' @export
max_by <- function(th, columns = character()) {
    verify_string_vector("columns", columns)
    return(TableHandle$new(th$internal_table_handle$max_by(columns)))
}

#' @export
sum_by <- function(th, columns = character()) {
    verify_string_vector("columns", columns)
    return(TableHandle$new(th$internal_table_handle$sum_by(columns)))
}

#' @export
abs_sum_by <- function(th, columns = character()) {
    verify_string_vector("columns", columns)
    return(TableHandle$new(th$internal_table_handle$abs_sum_by(columns)))
}

#' @export
avg_by <- function(th, columns = character()) {
    verify_string_vector("columns", columns)
    return(TableHandle$new(th$internal_table_handle$avg_by(columns)))
}

#' @export
w_avg_by <- function(th, weight_column, columns = character()) {
    verify_string("weight_column", weight_column)
    verify_string_vector("columns", columns)
    return(TableHandle$new(th$internal_table_handle$w_avg_by(weight_column, columns)))
}

#' @export
median_by <- function(th, columns = character()) {
    verify_string_vector("columns", columns)
    return(TableHandle$new(th$internal_table_handle$median_by(columns)))
}

#' @export
var_by <- function(th, columns = character()) {
    verify_string_vector("columns", columns)
    return(TableHandle$new(th$internal_table_handle$var_by(columns)))
}

#' @export
std_by <- function(th, columns = character()) {
    verify_string_vector("columns", columns)
    return(TableHandle$new(th$internal_table_handle$std_by(columns)))
}

#' @export
percentile_by <- function(th, percentile, columns = character()) {
    verify_proportion("percentile", percentile)
    verify_string_vector("columns", columns)
    return(TableHandle$new(th$internal_table_handle$percentile_by(percentile, columns)))
}

#' @export
count_by <- function(th, count_by_column, columns = character()) {
    verify_string("count_by_column", count_by_column)
    verify_string_vector("columns", columns)
    return(TableHandle$new(th$internal_table_handle$count_by(count_by_column, columns)))
}

#' @export
sort_by <- function(th, columns, descending = FALSE) {
    verify_string_vector("columns", columns)
    verify_bool_vector("descending", descending)
    if ((length(descending) > 1) && length(descending) != length(columns)) {
        stop(paste0("'descending' must be the same length as 'columns' if more than one entry is supplied. Got 'columns' with length ", length(columns), " and 'descending' with length", length(descending), " instead."))
    }
    return(TableHandle$new(th$internal_table_handle$sort(columns, descending)))
}

# JOIN OPERATIONS

#' @export
cross_join <- function(th, right_side, columns_to_match, columns_to_add) {
    verify_string_vector("columns_to_match", columns_to_match)
    verify_string_vector("columns_to_add", columns_to_add)
    return(TableHandle$new(th$internal_table_handle$cross_join(right_side$internal_table_handle,
                                                                    columns_to_match, columns_to_add)))
}

#' @export
natural_join <- function(th, right_side, columns_to_match, columns_to_add) {
    verify_string_vector("columns_to_match", columns_to_match)
    verify_string_vector("columns_to_add", columns_to_add)
    return(TableHandle$new(th$internal_table_handle$natural_join(right_side$internal_table_handle,
                                                                      columns_to_match, columns_to_add)))
}

#' @export
exact_join <- function(th, right_side, columns_to_match, columns_to_add) {
    verify_string_vector("columns_to_match", columns_to_match)
    verify_string_vector("columns_to_add", columns_to_add)
    return(TableHandle$new(th$internal_table_handle$exact_join(right_side$internal_table_handle,
                                                                    columns_to_match, columns_to_add)))
}

# MISC OPERATIONS

#' @export
head.TableHandle <- function(th, n) {
    verify_int("n", n)
    return(TableHandle$new(th$internal_table_handle$head(n)))
}

#' @export
tail.TableHandle <- function(th, n) {
    verify_int("n", n)
    return(TableHandle$new(th$internal_table_handle$tail(n)))
}

#' @export
dh_merge.TableHandle <- function(th, key_column, sources) {
    verify_string("key_column", key_column)
    return(TableHandle$new(th$internal_table_handle$merge(key_column, sources)))
}