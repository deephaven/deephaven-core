#' @name TableOps
#' @title Deephaven TableHandle operations
#' @description These TableHandle operations provide a dplyr-like interface for filtering, aggregating, and summarizing
#' data in Deephaven Tables living on the server. For large datasets, constructing queries with these operations before
#' pulling results into an R data frame will be more performant than first pulling a Deephaven Table into an R data frame
#' and using existing dplyr methods. Additionally, these operations seamlessly support real-time Deephaven Tables, and
#' the resulting downstream tables will be automatically updated on the server when the parent tables update.
NULL

# FILTERING OPERATIONS

#' @description
#' Creates a new in-memory table that includes one column for each argument.
#' @param by A string or list of strings specifying the columns to view.
#' @export
select <- function(th, columns = character()) {
  verify_string_vector("columns", columns)
  TableHandle$new(th$internal_table_handle$select(columns))
}

#' @description
#' Creates a new formula table that includes one column for each argument.
#' @param by A string or list of strings specifying the columns to view.
#' @return TableHandle reference to the new table.
#' @export
view <- function(th, columns = character()) {
  verify_string_vector("columns", columns)
  return(TableHandle$new(th$internal_table_handle$view(columns)))
}

#' @description
#' Creates a new table containing a new in-memory column for each argument.
#' @param by A string or list of strings specifying the formulas to create new columns.
#' @return TableHandle reference to the new table.
#' @export
update <- function(th, columns = character()) {
  verify_string_vector("columns", columns)
  return(TableHandle$new(th$internal_table_handle$update(columns)))
}

#' @description
#' Creates a new formula table containing a new formula for each argument.
#' @param by A string or list of strings specifying the formulas to create new columns.
#' @return TableHandle reference to the new table.
#' @export
update_view <- function(th, columns = character()) {
  verify_string_vector("columns", columns)
  return(TableHandle$new(th$internal_table_handle$update_view(columns)))
}

#' @description
#' Creates a new table with the same size as this table, but omits any of the specified columns.
#' @param by A string or list of strings specifying the column names to drop.
#' @return TableHandle reference to the new table.
#' @export
drop_columns <- function(th, columns = character()) {
  verify_string_vector("columns", columns)
  return(TableHandle$new(th$internal_table_handle$drop_columns(columns)))
}

#' @description
#' Creates a new table only containing the rows that meet the specified condition.
#' @param condition A string specifying a conditional expression.
#' @return TableHandle reference to the new table.
#' @export
where <- function(th, condition) {
  verify_string("condition", condition)
  return(TableHandle$new(th$internal_table_handle$where(condition)))
}

# AGGREGATION OPERATIONS

#' @description
#' Creates a new table containing grouping columns and grouped data, with column content grouped into arrays.
#' @param group_by_columns A string or list of strings specifying the columns to group by.
#' @return TableHandle reference to the new table.
#' @export
group_by <- function(th, columns = character()) {
  verify_string_vector("columns", columns)
  return(TableHandle$new(th$internal_table_handle$group_by(columns)))
}

#' @description
#' Creates a new table in which array columns from the source table are unwrapped into separate rows.
#' This is the inverse of a `group_by()` aggregation.
#' @param group_by_columns A string or list of strings specifying the columns to ungroup by.
#' @return TableHandle reference to the new table.
#' @export
ungroup <- function(th, columns = character()) {
  verify_string_vector("columns", columns)
  return(TableHandle$new(th$internal_table_handle$ungroup(columns)))
}

#' @description
#' Creates a new table containing grouping columns and grouped data, defined by the specified aggregation(s).
#' @param aggregations A Deephaven Aggregation or a list of Aggregations specifying the aggregations to perform on the data.
#' TODO: Link agg_* docs for more info
#' @param group_by_columns #' @param group_by_columns A string or list of strings specifying the columns to group by.
#' @return TableHandle reference to the new table.
#' @export
agg_by <- function(th, aggregations, columns = character()) {
  verify_internal_type("Aggregation", "aggregations", aggregations)
  verify_string_vector("columns", columns)
  aggregations <- c(aggregations)
  unwrapped_aggregations <- lapply(aggregations, strip_r6_wrapping_from_aggregation)
  return(TableHandle$new(th$internal_table_handle$agg_by(unwrapped_aggregations, columns)))
}

#' @description
#' Creates a new table which contains the first row of each distinct group.
#' @param by A string or list of strings specifying the column names to group by.
#' @return TableHandle reference to the new table.
#' @export
first_by <- function(th, columns = character()) {
  verify_string_vector("columns", columns)
  return(TableHandle$new(th$internal_table_handle$first_by(columns)))
}

#' @description
#' Creates a new table which contains the last row of each distinct group.
#' @param by A string or list of strings specifying the column names to group by.
#' @return TableHandle reference to the new table.
#' @export
last_by <- function(th, columns = character()) {
  verify_string_vector("columns", columns)
  return(TableHandle$new(th$internal_table_handle$last_by(columns)))
}

#' @description
#' Creates a new table which contains the first n rows of each distinct group.
#' @param n An integer specifying the number of rows to include for each group.
#' @param by A string or list of strings specifying the column names to group by.
#' @return TableHandle reference to the new table.
#' @export
head_by <- function(th, n, columns = character()) {
  verify_int("n", n)
  verify_string_vector("columns", columns)
  return(TableHandle$new(th$internal_table_handle$head_by(n, columns)))
}

#' @description
#' Creates a new table which contains the last n rows of each distinct group.
#' @param n An integer specifying the number of rows to include for each group.
#' @param by A string or list of strings specifying the column names to group by.
#' @return TableHandle reference to the new table.
#' @export
tail_by <- function(th, n, columns = character()) {
  verify_int("n", n)
  verify_string_vector("columns", columns)
  return(TableHandle$new(th$internal_table_handle$tail_by(n, columns)))
}

#' @description
#' Creates a new table which contains the columnwise minimum of each distinct group, only defined for numeric columns.
#' @param by A string or list of strings specifying the column names to group by.
#' @return TableHandle reference to the new table.
#' @export
min_by <- function(th, columns = character()) {
  verify_string_vector("columns", columns)
  return(TableHandle$new(th$internal_table_handle$min_by(columns)))
}

#' @description
#' Creates a new table which contains the columnwise maximum of each distinct group, only defined for numeric columns.
#' @param by A string or list of strings specifying the column names to group by.
#' @return TableHandle reference to the new table.
#' @export
max_by <- function(th, columns = character()) {
  verify_string_vector("columns", columns)
  return(TableHandle$new(th$internal_table_handle$max_by(columns)))
}

#' @description
#' Creates a new table which contains the columnwise sum of each distinct group, only defined for numeric columns.
#' @param by A string or list of strings specifying the column names to group by.
#' @return TableHandle reference to the new table.
#' @export
sum_by <- function(th, columns = character()) {
  verify_string_vector("columns", columns)
  return(TableHandle$new(th$internal_table_handle$sum_by(columns)))
}

#' @description
#' Creates a new table which contains the columnwise sum of absolute values of each distinct group, only defined for numeric columns.
#' @param by A string or list of strings specifying the column names to group by.
#' @return TableHandle reference to the new table.
#' @export
abs_sum_by <- function(th, columns = character()) {
  verify_string_vector("columns", columns)
  return(TableHandle$new(th$internal_table_handle$abs_sum_by(columns)))
}

#' @description
#' Creates a new table which contains the columnwise average of each distinct group, only defined for numeric columns.
#' @param by A string or list of strings specifying the column names to group by.
#' @return TableHandle reference to the new table.
#' @export
avg_by <- function(th, columns = character()) {
  verify_string_vector("columns", columns)
  return(TableHandle$new(th$internal_table_handle$avg_by(columns)))
}

#' @description
#' Creates a new table which contains the columnwise weighted average of each distinct group, only defined for numeric columns.
#' @param weight_column A numeric column to use for the weights.
#' @param by A string or list of strings specifying the column names to group by.
#' @return TableHandle reference to the new table.
#' @export
w_avg_by <- function(th, weight_column, columns = character()) {
  verify_string("weight_column", weight_column)
  verify_string_vector("columns", columns)
  return(TableHandle$new(th$internal_table_handle$w_avg_by(weight_column, columns)))
}

#' @description
#' Creates a new table which contains the columnwise median of each distinct group, only defined for numeric columns.
#' @param by A string or list of strings specifying the column names to group by.
#' @return TableHandle reference to the new table.
#' @export
median_by <- function(th, columns = character()) {
  verify_string_vector("columns", columns)
  return(TableHandle$new(th$internal_table_handle$median_by(columns)))
}

#' @description
#' Creates a new table which contains the columnwise variance of each distinct group, only defined for numeric columns.
#' @param by A string or list of strings specifying the column names to group by.
#' @return TableHandle reference to the new table.
#' @export
var_by <- function(th, columns = character()) {
  verify_string_vector("columns", columns)
  return(TableHandle$new(th$internal_table_handle$var_by(columns)))
}

#' @description
#' Creates a new table which contains the columnwise standard deviation of each distinct group, only defined for numeric columns.
#' @param by A string or list of strings specifying the column names to group by.
#' @return TableHandle reference to the new table.
#' @export
std_by <- function(th, columns = character()) {
  verify_string_vector("columns", columns)
  return(TableHandle$new(th$internal_table_handle$std_by(columns)))
}

#' @description
#' Creates a new table which contains the columnwise pth percentile of each distinct group, only defined for numeric columns.
#' @param percentile A float in [0, 1] specifying the percentile. The results will be the values in the source table
#' nearest to the pth percentile, and no interpolation or estimation will be performed.
#' @param by A string or list of strings specifying the column names to group by.
#' @return TableHandle reference to the new table.
#' @export
percentile_by <- function(th, percentile, columns = character()) {
  verify_proportion("percentile", percentile)
  verify_string_vector("columns", columns)
  return(TableHandle$new(th$internal_table_handle$percentile_by(percentile, columns)))
}

#' @description
#' Creates a new table which contains the number of rows in each distinct group.
#' @param count_by_column A string specifying the name of the new count column.
#' @param by A string or list of strings specifying the column names to group by.
#' @return TableHandle reference to the new table.
#' @export
count_by <- function(th, count_by_column = "n", columns = character()) {
  verify_string("count_by_column", count_by_column)
  verify_string_vector("columns", columns)
  return(TableHandle$new(th$internal_table_handle$count_by(count_by_column, columns)))
}

# JOIN OPERATIONS

# TODO: Figure out the right defaults here to make interpretation simple
#' @export
cross_join <- function(th, right_side, columns_to_match, columns_to_add) {
  verify_string_vector("columns_to_match", columns_to_match)
  verify_string_vector("columns_to_add", columns_to_add)
  return(TableHandle$new(th$internal_table_handle$cross_join(
    right_side$internal_table_handle,
    columns_to_match, columns_to_add
  )))
}

# TODO: Document this well
#' @export
natural_join <- function(th, right_side, columns_to_match, columns_to_add) {
  verify_string_vector("columns_to_match", columns_to_match)
  verify_string_vector("columns_to_add", columns_to_add)
  return(TableHandle$new(th$internal_table_handle$natural_join(
    right_side$internal_table_handle,
    columns_to_match, columns_to_add
  )))
}

# TODO: Document this well
#' @export
exact_join <- function(th, right_side, columns_to_match, columns_to_add) {
  verify_string_vector("columns_to_match", columns_to_match)
  verify_string_vector("columns_to_add", columns_to_add)
  return(TableHandle$new(th$internal_table_handle$exact_join(
    right_side$internal_table_handle,
    columns_to_match, columns_to_add
  )))
}

# MISC OPERATIONS

#' @description
#' Creates a new table with rows sorted by the values in each distinct group.
#' @param by A string or list of strings specifying the column names to group by.
#' @param descending A boolean or list of booleans the same size as columns, specifying
#' whether the sort should be descending or not. Defaults to ascending sort.
#' @return TableHandle reference to the new table.
#' @export
sort <- function(th, columns, descending = FALSE) {
  verify_string_vector("columns", columns)
  verify_bool_vector("descending", descending)
  if ((length(descending) > 1) && length(descending) != length(columns)) {
    stop(paste0("'descending' must be the same length as 'columns' if more than one entry is supplied. Got 'columns' with length ", length(columns), " and 'descending' with length", length(descending), " instead."))
  }
  return(TableHandle$new(th$internal_table_handle$sort(columns, descending)))
}
