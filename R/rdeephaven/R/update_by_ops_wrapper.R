#' @description
#' An UpdateByOp represents a window-based operator that can be passed to `update_by()`.
#' Note that UpdateByOps should not be instantiated directly by user code, but rather by provided udb_* functions.
UpdateByOp <- R6Class("UpdateByOp",
  cloneable = FALSE,
  public = list(
    .internal_rcpp_object = NULL,
    initialize = function(update_by_op) {
      if (class(update_by_op) != "Rcpp_INTERNAL_UpdateByOp") {
        stop("'update_by_op' should be an internal Deephaven UpdateByOp. If you're seeing this,\n  you are trying to call the constructor of an UpdateByOp directly, which is not advised.\n  Please use one of the provided udb_op functions instead.")
      }
      self$.internal_rcpp_object <- update_by_op
    }
  )
)

### All of the functions below return an instance of an 'UpdateByOp' object

#' @description
#' Creates a Cumulative Sum UpdateByOp that computes the cumulative sum of each column in `cols` for each aggregation group.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the cumulative sum for all non-grouping columns.
#' @return UpdateByOp to be used in `update_by()`.
#' @export
udb_cum_sum <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(UpdateByOp$new(INTERNAL_cum_sum(cols)))
}

#' @description
#' Creates a Cumulative Product UpdateByOp that computes the cumulative product of each column in `cols` for each aggregation group.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the cumulative product for all non-grouping columns.
#' @return UpdateByOp to be used in `update_by()`.
#' @export
udb_cum_prod <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(UpdateByOp$new(INTERNAL_cum_prod(cols)))
}

#' @description
#' Creates a Cumulative Minimum UpdateByOp that computes the cumulative minimum of each column in `cols` for each aggregation group.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the cumulative minimum for all non-grouping columns.
#' @return UpdateByOp to be used in `update_by()`.
#' @export
udb_cum_min <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(UpdateByOp$new(INTERNAL_cum_min(cols)))
}

#' @description
#' Creates a Cumulative Maximum UpdateByOp that computes the cumulative maximum of each column in `cols` for each aggregation group.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the cumulative maximum for all non-grouping columns.
#' @return UpdateByOp to be used in `update_by()`.
#' @export
udb_cum_max <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(UpdateByOp$new(INTERNAL_cum_max(cols)))
}

#' @description
#' Creates a Forward Fill UpdateByOp that replaces null values in `cols` with the last known non-null values.
#' This operation is forward only.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to perform a forward fill on all non-grouping columns.
#' @return UpdateByOp to be used in `update_by()`.
#' @export
udb_forward_fill <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(UpdateByOp$new(INTERNAL_forward_fill(cols)))
}

#' @description
#' Creates a Delta UpdateByOp for each column in `cols`. The Delta operation computes the difference between the
#' current value and the previous value. When the current value is null, this operation will output null.
#' When the current value is valid, the output will depend on the `delta_control` provided.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to apply the delta operation to all non-grouping columns.
#' @param delta_control Defines how the delta operation handles null values. Can be one of the following:
#'   'null_dominates': A valid value following a null value returns null.
#'   'value_dominates': A valid value following a null value returns the valid value.
#'   'zero_dominates': A valid value following a null value returns zero.
#' Defaults to `null_dominates`.
#' @return UpdateByOp to be used in `update_by()`.
#' @export
udb_delta <- function(cols = character(), delta_control = "null_dominates") {
  verify_string("cols", cols, FALSE)
  if (!(delta_control %in% c("null_dominates", "value_dominates", "zero_dominates"))) {
    stop(paste0("'delta_control' must be one of 'null_dominates', 'value_dominates', or 'zero_dominates'. Got '", delta_control, "'."))
  }
  return(UpdateByOp$new(INTERNAL_delta(cols, delta_control)))
}

#' @description
#' Creates an exponential moving average UpdateByOp for each column in `cols`, using ticks as the decay unit.
#' The formula used is
#'   `a = e^(-1 / decay_ticks)`
#'   `ema_next = a * ema_last + (1 - a) * value`
#' @param decay_ticks Numeric scalar denoting the decay rate in ticks.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the exponential moving average for all non-grouping columns.
#' @param operation_control OperationControl that defines how special cases will behave. See `?OperationControl` for more information.
#' @export
udb_ema_tick <- function(decay_ticks, cols = character(), operation_control = op_control()) {
  verify_real("decay_ticks", decay_ticks, TRUE)
  verify_string("cols", cols, FALSE)
  verify_type("operation_control", operation_control, TRUE, "OperationControl", "a Deephaven OperationControl")
  return(UpdateByOp$new(INTERNAL_ema_tick(decay_ticks, cols, operation_control$.internal_rcpp_object)))
}

#' @description
#' Creates an exponential moving average UpdateByOp for each column in `cols`, using time as the decay unit.
#' The formula used is
#'   `a = e^(-dt / decay_time)`
#'   `ema_next = a * ema_last + (1 - a) * value`
#' @param ts_col String denoting the column to use as the timestamp.
#' @param decay_time ISO-8601-formatted string specifying the decay rate.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the exponential moving average for all non-grouping columns.
#' @param operation_control OperationControl that defines how special cases will behave. See `?OperationControl` for more information.
#' @export
udb_ema_time <- function(ts_col, decay_time, cols = character(), operation_control = op_control()) {
  verify_string("ts_col", ts_col, TRUE)
  verify_string("decay_time", decay_time, TRUE)
  verify_string("cols", cols, FALSE)
  verify_type("operation_control", operation_control, TRUE, "OperationControl", "a Deephaven OperationControl")
  return(UpdateByOp$new(INTERNAL_ema_time(ts_col, decay_time, cols, operation_control$.internal_rcpp_object)))
}

#' @description
#' Creates an exponential moving sum UpdateByOp for each column in `cols`, using ticks as the decay unit.
#' The formula used is
#'   `a = e^(-1 / decay_ticks)`
#'   `ems_next = a * ems_last + value`
#' @param decay_ticks Numeric scalar denoting the decay rate in ticks.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the exponential moving sum for all non-grouping columns.
#' @param operation_control OperationControl that defines how special cases will behave. See `?OperationControl` for more information.
#' @export
udb_ems_tick <- function(decay_ticks, cols = character(), operation_control = op_control()) {
  verify_real("decay_ticks", decay_ticks, TRUE)
  verify_string("cols", cols, FALSE)
  verify_type("operation_control", operation_control, TRUE, "OperationControl", "a Deephaven OperationControl")
  return(UpdateByOp$new(INTERNAL_ems_tick(decay_ticks, cols, operation_control$.internal_rcpp_object)))
}

#' @description
#' Creates an exponential moving sum UpdateByOp for each column in `cols`, using time as the decay unit.
#' The formula used is
#'   `a = e^(-dt / decay_time)`
#'   `ems_next = a * ems_last + value`
#' @param ts_col String denoting the column to use as the timestamp.
#' @param decay_time ISO-8601-formatted string specifying the decay rate.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the exponential moving sum for all non-grouping columns.
#' @param operation_control OperationControl that defines how special cases will behave. See `?OperationControl` for more information.
#' @export
udb_ems_time <- function(ts_col, decay_time, cols = character(), operation_control = op_control()) {
  verify_string("ts_col", ts_col, TRUE)
  verify_string("decay_time", decay_time, TRUE)
  verify_string("cols", cols, FALSE)
  verify_type("operation_control", operation_control, TRUE, "OperationControl", "a Deephaven OperationControl")
  return(UpdateByOp$new(INTERNAL_ems_time(ts_col, decay_time, cols, operation_control$.internal_rcpp_object)))
}

#' @description
#' Creates an exponential moving minimum UpdateByOp for each column in `cols`, using ticks as the decay unit.
#' The formula used is
#'   `a = e^(-1 / decay_ticks)`
#'   `emmin_next = min(a * emmin_last, value)`
#' @param decay_ticks Numeric scalar denoting the decay rate in ticks.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the exponential moving minimum for all non-grouping columns.
#' @param operation_control OperationControl that defines how special cases will behave. See `?OperationControl` for more information.
#' @export
udb_emmin_tick <- function(decay_ticks, cols = character(), operation_control = op_control()) {
  verify_real("decay_ticks", decay_ticks, TRUE)
  verify_string("cols", cols, FALSE)
  verify_type("operation_control", operation_control, TRUE, "OperationControl", "a Deephaven OperationControl")
  return(UpdateByOp$new(INTERNAL_emmin_tick(decay_ticks, cols, operation_control$.internal_rcpp_object)))
}

#' @description
#' Creates an exponential moving minimum UpdateByOp for each column in `cols`, using time as the decay unit.
#' The formula used is
#'   `a = e^(-dt / decay_time)`
#'   `emmin_next = min(a * emmin_last, value)`
#' @param ts_col String denoting the column to use as the timestamp.
#' @param decay_time ISO-8601-formatted string specifying the decay rate.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the exponential moving minimum for all non-grouping columns.
#' @param operation_control OperationControl that defines how special cases will behave. See `?OperationControl` for more information.
#' @export
udb_emmin_time <- function(ts_col, decay_time, cols = character(), operation_control = op_control()) {
  verify_string("ts_col", ts_col, TRUE)
  verify_string("decay_time", decay_time, TRUE)
  verify_string("cols", cols, FALSE)
  verify_type("operation_control", operation_control, TRUE, "OperationControl", "a Deephaven OperationControl")
  return(UpdateByOp$new(INTERNAL_emmin_time(ts_col, decay_time, cols, operation_control$.internal_rcpp_object)))
}

#' @description
#' Creates an exponential moving maximum UpdateByOp for each column in `cols`, using ticks as the decay unit.
#' The formula used is
#'   `a = e^(-1 / decay_ticks)`
#'   `emmax_next = max(a * emmax_last, value)`
#' @param decay_ticks Numeric scalar denoting the decay rate in ticks.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the exponential moving maximum for all non-grouping columns.
#' @param operation_control OperationControl that defines how special cases will behave. See `?OperationControl` for more information.
#' @export
udb_emmax_tick <- function(decay_ticks, cols = character(), operation_control = op_control()) {
  verify_real("decay_ticks", decay_ticks, TRUE)
  verify_string("cols", cols, FALSE)
  verify_type("operation_control", operation_control, TRUE, "OperationControl", "a Deephaven OperationControl")
  return(UpdateByOp$new(INTERNAL_emmax_tick(decay_ticks, cols, operation_control$.internal_rcpp_object)))
}

#' @description
#' Creates an exponential moving maximum UpdateByOp for each column in `cols`, using time as the decay unit.
#' The formula used is
#'   `a = e^(-dt / decay_time)`
#'   `emmax_next = max(a * emmax_last, value)`
#' @param ts_col String denoting the column to use as the timestamp.
#' @param decay_time ISO-8601-formatted string specifying the decay rate.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the exponential moving maximum for all non-grouping columns.
#' @param operation_control OperationControl that defines how special cases will behave. See `?OperationControl` for more information.
#' @export
udb_emmax_time <- function(ts_col, decay_time, cols = character(), operation_control = op_control()) {
  verify_string("ts_col", ts_col, TRUE)
  verify_string("decay_time", decay_time, TRUE)
  verify_string("cols", cols, FALSE)
  verify_type("operation_control", operation_control, TRUE, "OperationControl", "a Deephaven OperationControl")
  return(UpdateByOp$new(INTERNAL_emmax_time(ts_col, decay_time, cols, operation_control$.internal_rcpp_object)))
}

#' @description
#' Creates an exponential moving standard deviation UpdateByOp for each column in `cols`, using ticks as the decay unit.
#' The formula used is
#'   `a = e^(-1 / decay_ticks)`
#'   `ema_next = a * ema_last + (1 - a) * value`
#'   `em_variance_next = a * (em_variance_last + (1 − a) * (value − ema_last)^2)`
#'   `emstd_next = sqrt(em_variance_next)`
#' @param decay_ticks Numeric scalar denoting the decay rate in ticks.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the exponential moving standard deviation for all non-grouping columns.
#' @param operation_control OperationControl that defines how special cases will behave. See `?OperationControl` for more information.
#' @export
udb_emstd_tick <- function(decay_ticks, cols = character(), operation_control = op_control()) {
  verify_real("decay_ticks", decay_ticks, TRUE)
  verify_string("cols", cols, FALSE)
  verify_type("operation_control", operation_control, TRUE, "OperationControl", "a Deephaven OperationControl")
  return(UpdateByOp$new(INTERNAL_emstd_tick(decay_ticks, cols, operation_control$.internal_rcpp_object)))
}

#' @description
#' Creates an exponential moving standard deviation UpdateByOp for each column in `cols`, using time as the decay unit.
#' The formula used is
#'   `a = e^(-dt / decay_time)`
#'   `ema_next = a * ema_last + (1 - a) * value`
#'   `em_variance_next = a * (em_variance_last + (1 − a) * (value − ema_last)^2)`
#'   `emstd_next = sqrt(em_variance_next)`
#' @param ts_col String denoting the column to use as the timestamp.
#' @param decay_time ISO-8601-formatted string specifying the decay rate.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the exponential moving standard deviation for all non-grouping columns.
#' @param operation_control OperationControl that defines how special cases will behave. See `?OperationControl` for more information.
#' @export
udb_emstd_time <- function(ts_col, decay_time, cols = character(), operation_control = op_control()) {
  verify_string("ts_col", ts_col, TRUE)
  verify_string("decay_time", decay_time, TRUE)
  verify_string("cols", cols, FALSE)
  verify_type("operation_control", operation_control, TRUE, "OperationControl", "a Deephaven OperationControl")
  return(UpdateByOp$new(INTERNAL_emstd_time(ts_col, decay_time, cols, operation_control$.internal_rcpp_object)))
}

#' @description
#' Creates a rolling sum UpdateByOp for each column in `cols`, using ticks as the windowing unit.
#' Ticks are row counts, and you may specify the reverse and forward window in number of rows to include.
#' The current row is considered to belong to the reverse window but not the forward window.
#' Also, negative values are allowed and can be used to generate completely forward or completely reverse windows.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling sum for all non-grouping columns.
#' @param rev_ticks Integer scalar denoting the look-behind window size in number of rows.
#' @param fwd_ticks Integer scalar denoting the look-ahead window size in number of rows. Default is 0.
#' @export
udb_roll_sum_tick <- function(cols, rev_ticks, fwd_ticks = 0) {
  verify_string("cols", cols, FALSE)
  verify_any_int("rev_ticks", rev_ticks, TRUE)
  verify_any_int("fwd_ticks", fwd_ticks, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_sum_tick(cols, rev_ticks, fwd_ticks)))
}

#' @description
#' Creates a rolling sum UpdateByOp for each column in `cols`, using time as the windowing unit.
#' This uses ISO-8601 time strings as the reverse and forward window parameters.
#' Negative values are allowed and can be used to generate completely forward or completely reverse windows.
#' A row containing a null value in the timestamp column belongs to no window and will not be considered
#' in the windows of other rows; its output will be null.
#' @param ts_col String denoting the column to use as the timestamp.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling sum for all non-grouping columns.
#' @param rev_time ISO-8601-formatted string specifying the look-behind window size.
#' @param fwd_time ISO-8601-formatted string specifying the look-ahead window size. Default is 0 seconds.
#' @export
udb_roll_sum_time <- function(ts_col, cols, rev_time, fwd_time = "PT0s") {
  verify_string("ts_col", ts_col, TRUE)
  verify_string("cols", cols, FALSE)
  verify_string("rev_time", rev_time, TRUE)
  verify_string("fwd_time", fwd_time, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_sum_time(ts_col, cols, rev_time, fwd_time)))
}

#' @description
#' Creates a rolling group UpdateByOp for each column in `cols`, using ticks as the windowing unit.
#' Ticks are row counts, and you may specify the reverse and forward window in number of rows to include.
#' The current row is considered to belong to the reverse window but not the forward window.
#' Also, negative values are allowed and can be used to generate completely forward or completely reverse windows.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling group for all non-grouping columns.
#' @param rev_ticks Integer scalar denoting the look-behind window size in number of rows.
#' @param fwd_ticks Integer scalar denoting the look-ahead window size in number of rows. Default is 0.
#' @export
udb_roll_group_tick <- function(cols, rev_ticks, fwd_ticks = 0) {
  verify_string("cols", cols, FALSE)
  verify_any_int("rev_ticks", rev_ticks, TRUE)
  verify_any_int("fwd_ticks", fwd_ticks, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_group_tick(cols, rev_ticks, fwd_ticks)))
}

#' @description
#' Creates a rolling group UpdateByOp for each column in `cols`, using time as the windowing unit.
#' This uses ISO-8601 time strings as the reverse and forward window parameters.
#' Negative values are allowed and can be used to generate completely forward or completely reverse windows.
#' A row containing a null value in the timestamp column belongs to no window and will not be considered
#' in the windows of other rows; its output will be null.
#' @param ts_col String denoting the column to use as the timestamp.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling group for all non-grouping columns.
#' @param rev_time ISO-8601-formatted string specifying the look-behind window size.
#' @param fwd_time ISO-8601-formatted string specifying the look-ahead window size. Default is 0 seconds.
#' @export
udb_roll_group_time <- function(ts_col, cols, rev_time, fwd_time = "PT0s") {
  verify_string("ts_col", ts_col, TRUE)
  verify_string("cols", cols, FALSE)
  verify_string("rev_time", rev_time, TRUE)
  verify_string("fwd_time", fwd_time, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_group_time(ts_col, cols, rev_time, fwd_time)))
}

#' @description
#' Creates a rolling average UpdateByOp for each column in `cols`, using ticks as the windowing unit.
#' Ticks are row counts, and you may specify the reverse and forward window in number of rows to include.
#' The current row is considered to belong to the reverse window but not the forward window.
#' Also, negative values are allowed and can be used to generate completely forward or completely reverse windows.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling average for all non-grouping columns.
#' @param rev_ticks Integer scalar denoting the look-behind window size in number of rows.
#' @param fwd_ticks Integer scalar denoting the look-ahead window size in number of rows. Default is 0.
#' @export
udb_roll_avg_tick <- function(cols, rev_ticks, fwd_ticks = 0) {
  verify_string("cols", cols, FALSE)
  verify_any_int("rev_ticks", rev_ticks, TRUE)
  verify_any_int("fwd_ticks", fwd_ticks, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_avg_tick(cols, rev_ticks, fwd_ticks)))
}

#' @description
#' Creates a rolling average UpdateByOp for each column in `cols`, using time as the windowing unit.
#' This uses ISO-8601 time strings as the reverse and forward window parameters.
#' Negative values are allowed and can be used to generate completely forward or completely reverse windows.
#' A row containing a null value in the timestamp column belongs to no window and will not be considered
#' in the windows of other rows; its output will be null.
#' @param ts_col String denoting the column to use as the timestamp.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling average for all non-grouping columns.
#' @param rev_time ISO-8601-formatted string specifying the look-behind window size.
#' @param fwd_time ISO-8601-formatted string specifying the look-ahead window size. Default is 0 seconds.
#' @export
udb_roll_avg_time <- function(ts_col, cols, rev_time, fwd_time = "PT0s") {
  verify_string("ts_col", ts_col, TRUE)
  verify_string("cols", cols, FALSE)
  verify_string("rev_time", rev_time, TRUE)
  verify_string("fwd_time", fwd_time, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_avg_time(ts_col, cols, rev_time, fwd_time)))
}

#' @description
#' Creates a rolling minimum UpdateByOp for each column in `cols`, using ticks as the windowing unit.
#' Ticks are row counts, and you may specify the reverse and forward window in number of rows to include.
#' The current row is considered to belong to the reverse window but not the forward window.
#' Also, negative values are allowed and can be used to generate completely forward or completely reverse windows.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling minimum for all non-grouping columns.
#' @param rev_ticks Integer scalar denoting the look-behind window size in number of rows.
#' @param fwd_ticks Integer scalar denoting the look-ahead window size in number of rows. Default is 0.
#' @export
udb_roll_min_tick <- function(cols, rev_ticks, fwd_ticks = 0) {
  verify_string("cols", cols, FALSE)
  verify_any_int("rev_ticks", rev_ticks, TRUE)
  verify_any_int("fwd_ticks", fwd_ticks, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_min_tick(cols, rev_ticks, fwd_ticks)))
}

#' @description
#' Creates a rolling minimum UpdateByOp for each column in `cols`, using time as the windowing unit.
#' This uses ISO-8601 time strings as the reverse and forward window parameters.
#' Negative values are allowed and can be used to generate completely forward or completely reverse windows.
#' A row containing a null value in the timestamp column belongs to no window and will not be considered
#' in the windows of other rows; its output will be null.
#' @param ts_col String denoting the column to use as the timestamp.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling minimum for all non-grouping columns.
#' @param rev_time ISO-8601-formatted string specifying the look-behind window size.
#' @param fwd_time ISO-8601-formatted string specifying the look-ahead window size. Default is 0 seconds.
#' @export
udb_roll_min_time <- function(ts_col, cols, rev_time, fwd_time = "PT0s") {
  verify_string("ts_col", ts_col, TRUE)
  verify_string("cols", cols, FALSE)
  verify_string("rev_time", rev_time, TRUE)
  verify_string("fwd_time", fwd_time, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_min_time(ts_col, cols, rev_time, fwd_time)))
}

#' @description
#' Creates a rolling maximum UpdateByOp for each column in `cols`, using ticks as the windowing unit.
#' Ticks are row counts, and you may specify the reverse and forward window in number of rows to include.
#' The current row is considered to belong to the reverse window but not the forward window.
#' Also, negative values are allowed and can be used to generate completely forward or completely reverse windows.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling maximum for all non-grouping columns.
#' @param rev_ticks Integer scalar denoting the look-behind window size in number of rows.
#' @param fwd_ticks Integer scalar denoting the look-ahead window size in number of rows. Default is 0.
#' @export
udb_roll_max_tick <- function(cols, rev_ticks, fwd_ticks = 0) {
  verify_string("cols", cols, FALSE)
  verify_any_int("rev_ticks", rev_ticks, TRUE)
  verify_any_int("fwd_ticks", fwd_ticks, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_max_tick(cols, rev_ticks, fwd_ticks)))
}

#' @description
#' Creates a rolling maximum UpdateByOp for each column in `cols`, using time as the windowing unit.
#' This uses ISO-8601 time strings as the reverse and forward window parameters.
#' Negative values are allowed and can be used to generate completely forward or completely reverse windows.
#' A row containing a null value in the timestamp column belongs to no window and will not be considered
#' in the windows of other rows; its output will be null.
#' @param ts_col String denoting the column to use as the timestamp.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling maximum for all non-grouping columns.
#' @param rev_time ISO-8601-formatted string specifying the look-behind window size.
#' @param fwd_time ISO-8601-formatted string specifying the look-ahead window size. Default is 0 seconds.
#' @export
udb_roll_max_time <- function(ts_col, cols, rev_time, fwd_time = "PT0s") {
  verify_string("ts_col", ts_col, TRUE)
  verify_string("cols", cols, FALSE)
  verify_string("rev_time", rev_time, TRUE)
  verify_string("fwd_time", fwd_time, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_max_time(ts_col, cols, rev_time, fwd_time)))
}

#' @description
#' Creates a rolling product UpdateByOp for each column in `cols`, using ticks as the windowing unit.
#' Ticks are row counts, and you may specify the reverse and forward window in number of rows to include.
#' The current row is considered to belong to the reverse window but not the forward window.
#' Also, negative values are allowed and can be used to generate completely forward or completely reverse windows.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling product for all non-grouping columns.
#' @param rev_ticks Integer scalar denoting the look-behind window size in number of rows.
#' @param fwd_ticks Integer scalar denoting the look-ahead window size in number of rows. Default is 0.
#' @export
udb_roll_prod_tick <- function(cols, rev_ticks, fwd_ticks = 0) {
  verify_string("cols", cols, FALSE)
  verify_any_int("rev_ticks", rev_ticks, TRUE)
  verify_any_int("fwd_ticks", fwd_ticks, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_prod_tick(cols, rev_ticks, fwd_ticks)))
}

#' @description
#' Creates a rolling product UpdateByOp for each column in `cols`, using time as the windowing unit.
#' This uses ISO-8601 time strings as the reverse and forward window parameters.
#' Negative values are allowed and can be used to generate completely forward or completely reverse windows.
#' A row containing a null value in the timestamp column belongs to no window and will not be considered
#' in the windows of other rows; its output will be null.
#' @param ts_col String denoting the column to use as the timestamp.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling product for all non-grouping columns.
#' @param rev_time ISO-8601-formatted string specifying the look-behind window size.
#' @param fwd_time ISO-8601-formatted string specifying the look-ahead window size. Default is 0 seconds.
#' @export
udb_roll_prod_time <- function(ts_col, cols, rev_time, fwd_time = "PT0s") {
  verify_string("ts_col", ts_col, TRUE)
  verify_string("cols", cols, FALSE)
  verify_string("rev_time", rev_time, TRUE)
  verify_string("fwd_time", fwd_time, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_prod_time(ts_col, cols, rev_time, fwd_time)))
}

#' @description
#' Creates a rolling count UpdateByOp for each column in `cols`, using ticks as the windowing unit.
#' Ticks are row counts, and you may specify the reverse and forward window in number of rows to include.
#' The current row is considered to belong to the reverse window but not the forward window.
#' Also, negative values are allowed and can be used to generate completely forward or completely reverse windows.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling count for all non-grouping columns.
#' @param rev_ticks Integer scalar denoting the look-behind window size in number of rows.
#' @param fwd_ticks Integer scalar denoting the look-ahead window size in number of rows. Default is 0.
#' @export
udb_roll_count_tick <- function(cols, rev_ticks, fwd_ticks = 0) {
  verify_string("cols", cols, FALSE)
  verify_any_int("rev_ticks", rev_ticks, TRUE)
  verify_any_int("fwd_ticks", fwd_ticks, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_count_tick(cols, rev_ticks, fwd_ticks)))
}

#' @description
#' Creates a rolling count UpdateByOp for each column in `cols`, using time as the windowing unit.
#' This uses ISO-8601 time strings as the reverse and forward window parameters.
#' Negative values are allowed and can be used to generate completely forward or completely reverse windows.
#' A row containing a null value in the timestamp column belongs to no window and will not be considered
#' in the windows of other rows; its output will be null.
#' @param ts_col String denoting the column to use as the timestamp.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling count for all non-grouping columns.
#' @param rev_time ISO-8601-formatted string specifying the look-behind window size.
#' @param fwd_time ISO-8601-formatted string specifying the look-ahead window size. Default is 0 seconds.
#' @export
udb_roll_count_time <- function(ts_col, cols, rev_time, fwd_time = "PT0s") {
  verify_string("ts_col", ts_col, TRUE)
  verify_string("cols", cols, FALSE)
  verify_string("rev_time", rev_time, TRUE)
  verify_string("fwd_time", fwd_time, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_count_time(ts_col, cols, rev_time, fwd_time)))
}

#' @description
#' Creates a rolling standard deviation UpdateByOp for each column in `cols`, using ticks as the windowing unit.
#' Ticks are row counts, and you may specify the reverse and forward window in number of rows to include.
#' The current row is considered to belong to the reverse window but not the forward window.
#' Also, negative values are allowed and can be used to generate completely forward or completely reverse windows.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling standard deviation for all non-grouping columns.
#' @param rev_ticks Integer scalar denoting the look-behind window size in number of rows.
#' @param fwd_ticks Integer scalar denoting the look-ahead window size in number of rows. Default is 0.
#' @export
udb_roll_std_tick <- function(cols, rev_ticks, fwd_ticks = 0) {
  verify_string("cols", cols, FALSE)
  verify_any_int("rev_ticks", rev_ticks, TRUE)
  verify_any_int("fwd_ticks", fwd_ticks, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_std_tick(cols, rev_ticks, fwd_ticks)))
}

#' @description
#' Creates a rolling standard deviation UpdateByOp for each column in `cols`, using time as the windowing unit.
#' This uses ISO-8601 time strings as the reverse and forward window parameters.
#' Negative values are allowed and can be used to generate completely forward or completely reverse windows.
#' A row containing a null value in the timestamp column belongs to no window and will not be considered
#' in the windows of other rows; its output will be null.
#' @param ts_col String denoting the column to use as the timestamp.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling standard deviation for all non-grouping columns.
#' @param rev_time ISO-8601-formatted string specifying the look-behind window size.
#' @param fwd_time ISO-8601-formatted string specifying the look-ahead window size. Default is 0 seconds.
#' @export
udb_roll_std_time <- function(ts_col, cols, rev_time, fwd_time = "PT0s") {
  verify_string("ts_col", ts_col, TRUE)
  verify_string("cols", cols, FALSE)
  verify_string("rev_time", rev_time, TRUE)
  verify_string("fwd_time", fwd_time, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_std_time(ts_col, cols, rev_time, fwd_time)))
}

#' @description
#' Creates a rolling weighted average UpdateByOp for each column in `cols`, using ticks as the windowing unit.
#' Ticks are row counts, and you may specify the reverse and forward window in number of rows to include.
#' The current row is considered to belong to the reverse window but not the forward window.
#' Also, negative values are allowed and can be used to generate completely forward or completely reverse windows.
#' @param wcol String denoting the column to use for weights. This must be a numeric column.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling weighted average for all non-grouping columns.
#' @param rev_ticks Integer scalar denoting the look-behind window size in number of rows.
#' @param fwd_ticks Integer scalar denoting the look-ahead window size in number of rows. Default is 0.
#' @export
udb_roll_wavg_tick <- function(wcol, cols, rev_ticks, fwd_ticks = 0) {
  verify_string("wcol", wcol, TRUE)
  verify_string("cols", cols, FALSE)
  verify_any_int("rev_ticks", rev_ticks, TRUE)
  verify_any_int("fwd_ticks", fwd_ticks, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_wavg_tick(wcol, cols, rev_ticks, fwd_ticks)))
}

#' @description
#' Creates a rolling weighted average UpdateByOp for each column in `cols`, using time as the windowing unit.
#' This uses ISO-8601 time strings as the reverse and forward window parameters.
#' Negative values are allowed and can be used to generate completely forward or completely reverse windows.
#' A row containing a null value in the timestamp column belongs to no window and will not be considered
#' in the windows of other rows; its output will be null.
#' @param ts_col String denoting the column to use as the timestamp.
#' @param wcol String denoting the column to use for weights. This must be a numeric column.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling weighted average for all non-grouping columns.
#' @param rev_time ISO-8601-formatted string specifying the look-behind window size.
#' @param fwd_time ISO-8601-formatted string specifying the look-ahead window size. Default is 0 seconds.
#' @export
udb_roll_wavg_time <- function(ts_col, wcol, cols, rev_time, fwd_time = "PT0s") {
  verify_string("ts_col", ts_col, TRUE)
  verify_string("wcol", wcol, TRUE)
  verify_string("cols", cols, FALSE)
  verify_string("rev_time", rev_time, TRUE)
  verify_string("fwd_time", fwd_time, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_wavg_time(ts_col, wcol, cols, rev_time, fwd_time)))
}
