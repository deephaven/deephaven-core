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

#' @export
udb_cum_sum <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(UpdateByOp$new(INTERNAL_cum_sum(cols)))
}

#' @export
udb_cum_prod <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(UpdateByOp$new(INTERNAL_cum_prod(cols)))
}

#' @export
udb_cum_min <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(UpdateByOp$new(INTERNAL_cum_min(cols)))
}

#' @export
udb_cum_max <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(UpdateByOp$new(INTERNAL_cum_max(cols)))
}

#' @export
udb_forward_fill <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(UpdateByOp$new(INTERNAL_forward_fill(cols)))
}

#' @export
udb_delta <- function(cols = character(), delta_control = "null_dominates") {
  verify_string("cols", cols, FALSE)
  if (!(delta_control %in% c("null_dominates", "value_dominates", "zero_dominates"))) {
    stop(paste0("'delta_control' must be one of 'null_dominates', 'value_dominates', or 'zero_dominates'. Got '", delta_control, "'."))
  }
  return(UpdateByOp$new(INTERNAL_delta(cols, delta_control)))
}

#' @export
udb_ema_tick <- function(decay_ticks, cols = character(), operation_control = op_control()) {
  verify_numeric("decay_ticks", decay_ticks, TRUE)
  verify_string("cols", cols, FALSE)
  verify_type("operation_control", operation_control, "OperationControl", "Deephaven OperationControl", TRUE)
  return(UpdateByOp$new(INTERNAL_ema_tick(decay_ticks, cols, operation_control$.internal_rcpp_object)))
}

#' @export
udb_ema_time <- function(ts_col, decay_time, cols = character(), operation_control = op_control()) {
  verify_string("ts_col", ts_col, TRUE)
  verify_string("decay_time", decay_time, TRUE)
  verify_string("cols", cols, FALSE)
  verify_type("operation_control", operation_control, "OperationControl", "Deephaven OperationControl", TRUE)
  return(UpdateByOp$new(INTERNAL_ema_time(ts_col, decay_time, cols, operation_control$.internal_rcpp_object)))
}

#' @export
udb_ems_tick <- function(decay_ticks, cols = character(), operation_control = op_control()) {
  verify_numeric("decay_ticks", decay_ticks, TRUE)
  verify_string("cols", cols, FALSE)
  verify_type("operation_control", operation_control, "OperationControl", "Deephaven OperationControl", TRUE)
  return(UpdateByOp$new(INTERNAL_ems_tick(decay_ticks, cols, operation_control$.internal_rcpp_object)))
}

#' @export
udb_ems_time <- function(ts_col, decay_time, cols = character(), operation_control = op_control()) {
  verify_string("ts_col", ts_col, TRUE)
  verify_string("decay_time", decay_time, TRUE)
  verify_string("cols", cols, FALSE)
  verify_type("operation_control", operation_control, "OperationControl", "Deephaven OperationControl", TRUE)
  return(UpdateByOp$new(INTERNAL_ems_time(ts_col, decay_time, cols, operation_control$.internal_rcpp_object)))
}

#' @export
udb_emmin_tick <- function(decay_ticks, cols = character(), operation_control = op_control()) {
  verify_numeric("decay_ticks", decay_ticks, TRUE)
  verify_string("cols", cols, FALSE)
  verify_type("operation_control", operation_control, "OperationControl", "Deephaven OperationControl", TRUE)
  return(UpdateByOp$new(INTERNAL_emmin_tick(decay_ticks, cols, operation_control$.internal_rcpp_object)))
}

#' @export
udb_emmin_time <- function(ts_col, decay_time, cols = character(), operation_control = op_control()) {
  verify_string("ts_col", ts_col, TRUE)
  verify_string("decay_time", decay_time, TRUE)
  verify_string("cols", cols, FALSE)
  verify_type("operation_control", operation_control, "OperationControl", "Deephaven OperationControl", TRUE)
  return(UpdateByOp$new(INTERNAL_emmin_time(ts_col, decay_time, cols, operation_control$.internal_rcpp_object)))
}

#' @export
udb_emmax_tick <- function(decay_ticks, cols = character(), operation_control = op_control()) {
  verify_numeric("decay_ticks", decay_ticks, TRUE)
  verify_string("cols", cols, FALSE)
  verify_type("operation_control", operation_control, "OperationControl", "Deephaven OperationControl", TRUE)
  return(UpdateByOp$new(INTERNAL_emmax_tick(decay_ticks, cols, operation_control$.internal_rcpp_object)))
}

#' @export
udb_emmax_time <- function(ts_col, decay_time, cols = character(), operation_control = op_control()) {
  verify_string("ts_col", ts_col, TRUE)
  verify_string("decay_time", decay_time, TRUE)
  verify_string("cols", cols, FALSE)
  verify_type("operation_control", operation_control, "OperationControl", "Deephaven OperationControl", TRUE)
  return(UpdateByOp$new(INTERNAL_emmax_time(ts_col, decay_time, cols, operation_control$.internal_rcpp_object)))
}

#' @export
udb_emstd_tick <- function(decay_ticks, cols = character(), operation_control = op_control()) {
  verify_numeric("decay_ticks", decay_ticks, TRUE)
  verify_string("cols", cols, FALSE)
  verify_type("operation_control", operation_control, "OperationControl", "Deephaven OperationControl", TRUE)
  return(UpdateByOp$new(INTERNAL_emstd_tick(decay_ticks, cols, operation_control$.internal_rcpp_object)))
}

#' @export
udb_emstd_time <- function(ts_col, decay_time, cols = character(), operation_control = op_control()) {
  verify_string("ts_col", ts_col, TRUE)
  verify_string("decay_time", decay_time, TRUE)
  verify_string("cols", cols, FALSE)
  verify_type("operation_control", operation_control, "OperationControl", "Deephaven OperationControl", TRUE)
  return(UpdateByOp$new(INTERNAL_emstd_time(ts_col, decay_time, cols, operation_control$.internal_rcpp_object)))
}

#' @export
udb_roll_sum_tick <- function(cols, rev_ticks, fwd_ticks = 0) {
  verify_string("cols", cols, FALSE)
  verify_numeric("rev_ticks", rev_ticks, TRUE)
  verify_numeric("fwd_ticks", fwd_ticks, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_sum_tick(cols, rev_ticks, fwd_ticks)))
}

#' @export
udb_roll_sum_time <- function(ts_col, cols, rev_time, fwd_time = "PT0s") {
  verify_string("ts_col", ts_col, TRUE)
  verify_string("cols", cols, FALSE)
  verify_string("rev_time", rev_time, TRUE)
  verify_string("fwd_time", fwd_time, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_sum_time(ts_col, cols, rev_time, fwd_time))
}

#' @export
udb_roll_group_tick <- function(cols, rev_ticks, fwd_ticks = 0) {
  verify_string("cols", cols, FALSE)
  verify_numeric("rev_ticks", rev_ticks, TRUE)
  verify_numeric("fwd_ticks", fwd_ticks, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_group_tick(cols, rev_ticks, fwd_ticks)))
}

#' @export
udb_roll_group_time <- function(ts_col, cols, rev_time, fwd_time = "PT0s") {
  verify_string("ts_col", ts_col, TRUE)
  verify_string("cols", cols, FALSE)
  verify_string("rev_time", rev_time, TRUE)
  verify_string("fwd_time", fwd_time, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_group_time(ts_col, cols, rev_time, fwd_time)))
}

#' @export
udb_roll_avg_tick <- function(cols, rev_ticks, fwd_ticks = 0) {
  verify_string("cols", cols, FALSE)
  verify_numeric("rev_ticks", rev_ticks, TRUE)
  verify_numeric("fwd_ticks", fwd_ticks, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_avg_tick(cols, rev_ticks, fwd_ticks)))
}

#' @export
udb_roll_avg_time <- function(ts_col, cols, rev_time, fwd_time = "PT0s") {
  verify_string("ts_col", ts_col, TRUE)
  verify_string("cols", cols, FALSE)
  verify_string("rev_time", rev_time, TRUE)
  verify_string("fwd_time", fwd_time, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_avg_time(ts_col, cols, rev_time, fwd_time)))
}

#' @export
udb_roll_min_tick <- function(cols, rev_ticks, fwd_ticks = 0) {
  verify_string("cols", cols, FALSE)
  verify_numeric("rev_ticks", rev_ticks, TRUE)
  verify_numeric("fwd_ticks", fwd_ticks, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_min_tick(cols, rev_ticks, fwd_ticks)))
}

#' @export
udb_roll_min_time <- function(ts_col, cols, rev_time, fwd_time = "PT0s") {
  verify_string("ts_col", ts_col, TRUE)
  verify_string("cols", cols, FALSE)
  verify_string("rev_time", rev_time, TRUE)
  verify_string("fwd_time", fwd_time, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_min_time(ts_col, cols, rev_time, fwd_time)))
}

#' @export
udb_roll_max_tick <- function(cols, rev_ticks, fwd_ticks = 0) {
  verify_string("cols", cols, FALSE)
  verify_numeric("rev_ticks", rev_ticks, TRUE)
  verify_numeric("fwd_ticks", fwd_ticks, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_max_tick(cols, rev_ticks, fwd_ticks)))
}

#' @export
udb_roll_max_time <- function(ts_col, cols, rev_time, fwd_time = "PT0s") {
  verify_string("ts_col", ts_col, TRUE)
  verify_string("cols", cols, FALSE)
  verify_string("rev_time", rev_time, TRUE)
  verify_string("fwd_time", fwd_time, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_max_time(ts_col, cols, rev_time, fwd_time)))
}

#' @export
udb_roll_prod_tick <- function(cols, rev_ticks, fwd_ticks = 0) {
  verify_string("cols", cols, FALSE)
  verify_numeric("rev_ticks", rev_ticks, TRUE)
  verify_numeric("fwd_ticks", fwd_ticks, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_prod_tick(cols, rev_ticks, fwd_ticks)))
}

#' @export
udb_roll_prod_time <- function(ts_col, cols, rev_time, fwd_time = "PT0s") {
  verify_string("ts_col", ts_col, TRUE)
  verify_string("cols", cols, FALSE)
  verify_string("rev_time", rev_time, TRUE)
  verify_string("fwd_time", fwd_time, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_prod_time(ts_col, cols, rev_time, fwd_time)))
}

#' @export
udb_roll_count_tick <- function(cols, rev_ticks, fwd_ticks = 0) {
  verify_string("cols", cols, FALSE)
  verify_numeric("rev_ticks", rev_ticks, TRUE)
  verify_numeric("fwd_ticks", fwd_ticks, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_count_tick(cols, rev_ticks, fwd_ticks)))
}

#' @export
udb_roll_count_time <- function(ts_col, cols, rev_time, fwd_time = "PT0s") {
  verify_string("ts_col", ts_col, TRUE)
  verify_string("cols", cols, FALSE)
  verify_string("rev_time", rev_time, TRUE)
  verify_string("fwd_time", fwd_time, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_count_time(ts_col, cols, rev_time, fwd_time)))
}

#' @export
udb_roll_std_tick <- function(cols, rev_ticks, fwd_ticks = 0) {
  verify_string("cols", cols, FALSE)
  verify_numeric("rev_ticks", rev_ticks, TRUE)
  verify_numeric("fwd_ticks", fwd_ticks, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_std_tick(cols, rev_ticks, fwd_ticks)))
}

#' @export
udb_roll_std_time <- function(ts_col, cols, rev_time, fwd_time = "PT0s") {
  verify_string("ts_col", ts_col, TRUE)
  verify_string("cols", cols, FALSE)
  verify_string("rev_time", rev_time, TRUE)
  verify_string("fwd_time", fwd_time, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_std_time(ts_col, cols, rev_time, fwd_time)))
}

#' @export
udb_roll_wavg_tick <- function(wcol, cols, rev_ticks, fwd_ticks = 0) {
  verify_string("wcol", wcol, TRUE)
  verify_string("cols", cols, FALSE)
  verify_numeric("rev_ticks", rev_ticks, TRUE)
  verify_numeric("fwd_ticks", fwd_ticks, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_wavg_tick(wcol, cols, rev_ticks, fwd_ticks)))
}

#' @export
udb_roll_wavg_time <- function(ts_col, wcol, cols, rev_time, fwd_time = "PT0s") {
  verify_string("ts_col", ts_col, TRUE)
  verify_string("wcol", wcol, TRUE)
  verify_string("cols", cols, FALSE)
  verify_string("rev_time", rev_time, TRUE)
  verify_string("fwd_time", fwd_time, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_wavg_time(ts_col, wcol, cols, rev_time, fwd_time)))
}
