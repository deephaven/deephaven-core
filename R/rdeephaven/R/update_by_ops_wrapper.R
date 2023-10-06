#' @description
#' An UpdateByOp represents a window-based operator that can be passed to `update_by()`.
#' Note that UpdateByOps should not be instantiated directly by user code, but rather by provided uby_* functions.
UpdateByOp <- R6Class("UpdateByOp",
  cloneable = FALSE,
  public = list(
    .internal_rcpp_object = NULL,
    initialize = function(update_by_op) {
      if (class(update_by_op) != "Rcpp_INTERNAL_UpdateByOp") {
        stop("'update_by_op' should be an internal Deephaven UpdateByOp. If you're seeing this,\n  you are trying to call the constructor of an UpdateByOp directly, which is not advised.\n  Please use one of the provided uby_op functions instead.")
      }
      self$.internal_rcpp_object <- update_by_op
    }
  )
)


#' @name
#' uby_cum_sum
#' @title
#' Cumulative sum of specified columns by group
#' @md
#'
#' @description
#' Creates a cumulative sum UpdateByOp that computes the cumulative sum of each column in `cols` for each aggregation group.
#'
#' @details
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `update_by()` caller
#' function. The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like the other Deephaven `uby_*` functions, is a generator function. That is, its output is another
#' function that is intended to be used in a call to `update_by()`. This detail is typically hidden from the user by
#' `update_by()`, which calls the generated functions internally. However, it is important to understand this detail
#' for debugging purposes, as the output of a `uby_*` function can otherwise seem unexpected.
#'
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the cumulative sum for all non-grouping columns.
#' @return UpdateByOp to be used in `update_by()`.
#'
#' @examples
#' print("hello!")
#'
#' @export
uby_cum_sum <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(UpdateByOp$new(INTERNAL_cum_sum(cols)))
}

#' @name
#' uby_cum_prod
#' @title
#' Cumulative product of specified columns by group
#' @md
#'
#' @description
#' Creates a cumulative product UpdateByOp that computes the cumulative product of each column in `cols` for each aggregation group.
#'
#' @details
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `update_by()` caller
#' function. The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like the other Deephaven `uby_*` functions, is a generator function. That is, its output is another
#' function that is intended to be used in a call to `update_by()`. This detail is typically hidden from the user by
#' `update_by()`, which calls the generated functions internally. However, it is important to understand this detail
#' for debugging purposes, as the output of a `uby_*` function can otherwise seem unexpected.
#'
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the cumulative product for all non-grouping columns.
#' @return UpdateByOp to be used in `update_by()`.
#'
#' @examples
#' print("hello!")
#'
#' @export
uby_cum_prod <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(UpdateByOp$new(INTERNAL_cum_prod(cols)))
}

#' @name
#' uby_cum_min
#' @title
#' Cumulative minimum of specified columns by group
#' @md
#'
#' @description
#' Creates a cumulative minimum UpdateByOp that computes the cumulative minimum of each column in `cols` for each aggregation group.
#'
#' @details
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `update_by()` caller
#' function. The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like the other Deephaven `uby_*` functions, is a generator function. That is, its output is another
#' function that is intended to be used in a call to `update_by()`. This detail is typically hidden from the user by
#' `update_by()`, which calls the generated functions internally. However, it is important to understand this detail
#' for debugging purposes, as the output of a `uby_*` function can otherwise seem unexpected.
#'
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the cumulative minimum for all non-grouping columns.
#' @return UpdateByOp to be used in `update_by()`.
#'
#' @examples
#' print("hello!")
#'
#' @export
uby_cum_min <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(UpdateByOp$new(INTERNAL_cum_min(cols)))
}

#' @name
#' uby_cum_max
#' @title
#' Cumulative maximum of specified columns by group
#' @md
#'
#' @description
#' Creates a cumulative maximum UpdateByOp that computes the cumulative maximum of each column in `cols` for each aggregation group.
#'
#' @details
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `update_by()` caller
#' function. The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like the other Deephaven `uby_*` functions, is a generator function. That is, its output is another
#' function that is intended to be used in a call to `update_by()`. This detail is typically hidden from the user by
#' `update_by()`, which calls the generated functions internally. However, it is important to understand this detail
#' for debugging purposes, as the output of a `uby_*` function can otherwise seem unexpected.
#'
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the cumulative maximum for all non-grouping columns.
#' @return UpdateByOp to be used in `update_by()`.
#'
#' @examples
#' print("hello!")
#'
#' @export
uby_cum_max <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(UpdateByOp$new(INTERNAL_cum_max(cols)))
}

#' @name
#' uby_cum_min
#' @title
#' Replace null values with the last known non-null value
#' @md
#'
#' @description
#' Creates a forward fill UpdateByOp that replaces null values in `cols` with the last known non-null values.
#' This operation is forward only.
#'
#' @details
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `update_by()` caller
#' function. The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like the other Deephaven `uby_*` functions, is a generator function. That is, its output is another
#' function that is intended to be used in a call to `update_by()`. This detail is typically hidden from the user by
#' `update_by()`, which calls the generated functions internally. However, it is important to understand this detail
#' for debugging purposes, as the output of a `uby_*` function can otherwise seem unexpected.
#'
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to perform a forward fill on all non-grouping columns.
#' @return UpdateByOp to be used in `update_by()`.
#'
#' @examples
#' print("hello!")
#'
#' @export
uby_forward_fill <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(UpdateByOp$new(INTERNAL_forward_fill(cols)))
}


#' @name
#' uby_delta
#' @title
#' Row-wise difference by group
#' @md
#'
#' @description
#' Creates a delta UpdateByOp for each column in `cols`. The Delta operation computes the difference between the
#' current value and the previous value. When the current value is null, this operation will output null.
#' When the current value is valid, the output will depend on the `delta_control` provided.
#'
#' @details
#' The `delta_control` parameter controls how this operation treats null values. It can be one of the following:
#' - `'null_dominates'`: A valid value following a null value returns null.
#' - `'value_dominates'`: A valid value following a null value returns the valid value.
#' - `'zero_dominates'`: A valid value following a null value returns zero.
#'
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `update_by()` caller
#' function. The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like the other Deephaven `uby_*` functions, is a generator function. That is, its output is another
#' function that is intended to be used in a call to `update_by()`. This detail is typically hidden from the user by
#' `update_by()`, which calls the generated functions internally. However, it is important to understand this detail
#' for debugging purposes, as the output of a `uby_*` function can otherwise seem unexpected.
#'
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to apply the delta operation to all non-grouping columns.
#' @param delta_control Defines how the delta operation handles null values. Defaults to `'null_dominates'`.
#' @return UpdateByOp to be used in `update_by()`.
#'
#' @examples
#' print("hello!")
#'
#' @export
uby_delta <- function(cols = character(), delta_control = "null_dominates") {
  verify_string("cols", cols, FALSE)
  if (!(delta_control %in% c("null_dominates", "value_dominates", "zero_dominates"))) {
    stop(paste0("'delta_control' must be one of 'null_dominates', 'value_dominates', or 'zero_dominates'. Got '", delta_control, "'."))
  }
  return(UpdateByOp$new(INTERNAL_delta(cols, delta_control)))
}

#' @name
#' uby_ema_tick
#' @title
#' Exponential moving average by group with ticks as the decay unit
#' @md
#'
#' @description
#' Creates an exponential moving average (EMA) UpdateByOp for each column in `cols`, using ticks as the decay unit.
#'
#' @details
#' The formula used is
#' \deqn{a = e^{\frac{-1}{\tau}}}
#' \deqn{\bar{x}_0 = x_0}
#' \deqn{\bar{x}_i = a*\bar{x}_{i-1} + (1-a)*x_i}
#'
#' Where:
#' - \eqn{\tau} is `decay_ticks`, an input parameter to the method.
#' - \eqn{\bar{x}_i} is the exponential moving average of column \eqn{X} at step \eqn{i}.
#' - \eqn{x_i} is the current value.
#' - \eqn{i} denotes the time step, ranging from \eqn{i=1} to \eqn{i = n-1}, where \eqn{n} is the number of elements in \eqn{X}.
#'
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `update_by()` caller
#' function. The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like the other Deephaven `uby_*` functions, is a generator function. That is, its output is another
#' function that is intended to be used in a call to `update_by()`. This detail is typically hidden from the user by
#' `update_by()`, which calls the generated functions internally. However, it is important to understand this detail
#' for debugging purposes, as the output of a `uby_*` function can otherwise seem unexpected.
#'
#' @param decay_ticks Numeric scalar denoting the decay rate in ticks.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the exponential moving average for all non-grouping columns.
#' @param operation_control OperationControl that defines how special cases will behave. See `?op_control` for more information.
#' @return UpdateByOp to be used in `update_by()`.
#'
#' @examples
#' print("hello!")
#'
#' @export
uby_ema_tick <- function(decay_ticks, cols = character(), operation_control = op_control()) {
  verify_real("decay_ticks", decay_ticks, TRUE)
  verify_string("cols", cols, FALSE)
  verify_type("operation_control", operation_control, TRUE, "OperationControl", "a Deephaven OperationControl")
  return(UpdateByOp$new(INTERNAL_ema_tick(decay_ticks, cols, operation_control$.internal_rcpp_object)))
}

#' @name
#' uby_ema_time
#' @title
#' Exponential moving average by group with time as the decay unit
#' @md
#'
#' @description
#' Creates an exponential moving average (EMA) UpdateByOp for each column in `cols`, using time as the decay unit.
#'
#' @details
#' The formula used is
#' \deqn{a_i = e^{\frac{-dt_i}{\tau}}}
#' \deqn{\bar{x}_0 = x_0}
#' \deqn{\bar{x}_i = a_i*\bar{x}_{i-1} + (1-a_i)*x_i}
#'
#' Where:
#' - \eqn{dt_i} is the difference between time \eqn{t_i} and \eqn{t_{i-1}} in nanoseconds.
#' - \eqn{\tau} is `decay_time` in nanoseconds, an input parameter to the method.
#' - \eqn{\bar{x}_i} is the exponential moving average of column \eqn{X} at time step \eqn{i}.
#' - \eqn{x_i} is the current value.
#' - \eqn{i} denotes the time step, ranging from \eqn{i=1} to \eqn{i = n-1}, where \eqn{n} is the number of elements in \eqn{X}.
#'
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `update_by()` caller
#' function. The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like the other Deephaven `uby_*` functions, is a generator function. That is, its output is another
#' function that is intended to be used in a call to `update_by()`. This detail is typically hidden from the user by
#' `update_by()`, which calls the generated functions internally. However, it is important to understand this detail
#' for debugging purposes, as the output of a `uby_*` function can otherwise seem unexpected.
#'
#' @param ts_col String denoting the column to use as the timestamp.
#' @param decay_time ISO-8601-formatted string specifying the decay rate.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the exponential moving average for all non-grouping columns.
#' @param operation_control OperationControl that defines how special cases will behave. See `?op_control` for more information.
#' @return UpdateByOp to be used in `update_by()`.
#'
#' @examples
#' print("hello!")
#'
#' @export
uby_ema_time <- function(ts_col, decay_time, cols = character(), operation_control = op_control()) {
  verify_string("ts_col", ts_col, TRUE)
  verify_string("decay_time", decay_time, TRUE)
  verify_string("cols", cols, FALSE)
  verify_type("operation_control", operation_control, TRUE, "OperationControl", "a Deephaven OperationControl")
  return(UpdateByOp$new(INTERNAL_ema_time(ts_col, decay_time, cols, operation_control$.internal_rcpp_object)))
}

#' @name
#' uby_ems_tick
#' @title
#' Exponential moving sum by group with ticks as the decay unit
#' @md
#'
#' @description
#' Creates an exponential moving sum (EMS) UpdateByOp for each column in `cols`, using ticks as the decay unit.
#'
#' @details
#' The formula used is
#' \deqn{a = e^{\frac{-1}{\tau}}}
#' \deqn{\mathcal{S}_0 = x_0}
#' \deqn{\mathcal{S}_i = a*\mathcal{S}_{i-1} + x_i}
#'
#' Where:
#' - \eqn{\tau} is `decay_ticks`, an input parameter to the method.
#' - \eqn{\mathcal{S}_i} is the exponential moving sum of column \eqn{X} at step \eqn{i}.
#' - \eqn{x_i} is the current value.
#' - \eqn{i} denotes the time step, ranging from \eqn{i=1} to \eqn{i = n-1}, where \eqn{n} is the number of elements in \eqn{X}.
#'
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `update_by()` caller
#' function. The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like the other Deephaven `uby_*` functions, is a generator function. That is, its output is another
#' function that is intended to be used in a call to `update_by()`. This detail is typically hidden from the user by
#' `update_by()`, which calls the generated functions internally. However, it is important to understand this detail
#' for debugging purposes, as the output of a `uby_*` function can otherwise seem unexpected.
#'
#' @param decay_ticks Numeric scalar denoting the decay rate in ticks.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the exponential moving sum for all non-grouping columns.
#' @param operation_control OperationControl that defines how special cases will behave. See `?op_control` for more information.
#' @return UpdateByOp to be used in `update_by()`.
#'
#' @examples
#' print("hello!")
#'
#' @export
uby_ems_tick <- function(decay_ticks, cols = character(), operation_control = op_control()) {
  verify_real("decay_ticks", decay_ticks, TRUE)
  verify_string("cols", cols, FALSE)
  verify_type("operation_control", operation_control, TRUE, "OperationControl", "a Deephaven OperationControl")
  return(UpdateByOp$new(INTERNAL_ems_tick(decay_ticks, cols, operation_control$.internal_rcpp_object)))
}

#' @name
#' uby_ems_time
#' @title
#' Exponential moving sum by group with time as the decay unit
#' @md
#'
#' @description
#' Creates an exponential moving sum (EMS) UpdateByOp for each column in `cols`, using time as the decay unit.
#'
#' @details
#' The formula used is
#' \deqn{a_i = e^{\frac{-dt_i}{\tau}}}
#' \deqn{\mathcal{S}_0 = x_0}
#' \deqn{\mathcal{S}_i = a_i*\mathcal{S}_{i-1} + x_i}
#'
#' Where:
#' - \eqn{dt_i} is the difference between time \eqn{t_i} and \eqn{t_{i-1}} in nanoseconds.
#' - \eqn{\tau} is `decay_time` in nanoseconds, an input parameter to the method.
#' - \eqn{\mathcal{S}_i} is the exponential moving sum of column \eqn{X} at time step \eqn{i}.
#' - \eqn{x_i} is the current value.
#' - \eqn{i} denotes the time step, ranging from \eqn{i=1} to \eqn{i = n-1}, where \eqn{n} is the number of elements in \eqn{X}.
#'
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `update_by()` caller
#' function. The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like the other Deephaven `uby_*` functions, is a generator function. That is, its output is another
#' function that is intended to be used in a call to `update_by()`. This detail is typically hidden from the user by
#' `update_by()`, which calls the generated functions internally. However, it is important to understand this detail
#' for debugging purposes, as the output of a `uby_*` function can otherwise seem unexpected.
#'
#' @param decay_time ISO-8601-formatted string specifying the decay rate.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the exponential moving sum for all non-grouping columns.
#' @param operation_control OperationControl that defines how special cases will behave. See `?op_control` for more information.
#' @return UpdateByOp to be used in `update_by()`.
#'
#' @examples
#' print("hello!")
#'
#' @export
uby_ems_time <- function(ts_col, decay_time, cols = character(), operation_control = op_control()) {
  verify_string("ts_col", ts_col, TRUE)
  verify_string("decay_time", decay_time, TRUE)
  verify_string("cols", cols, FALSE)
  verify_type("operation_control", operation_control, TRUE, "OperationControl", "a Deephaven OperationControl")
  return(UpdateByOp$new(INTERNAL_ems_time(ts_col, decay_time, cols, operation_control$.internal_rcpp_object)))
}

#' @name
#' uby_emmin_tick
#' @title
#' Exponential moving minimum by group with ticks as the decay unit
#' @md
#'
#' @description
#' Creates an exponential moving minimum (EMMIN) UpdateByOp for each column in `cols`, using ticks as the decay unit.
#'
#' @details
#' The formula used is
#' \deqn{a = e^{\frac{-1}{\tau}}}
#' \deqn{\min_0(X) = x_0}
#' \deqn{\min_i(X) = \min(a*\min_{i-1}(X), \; x_i)}
#'
#' Where:
#' - \eqn{\tau} is `decay_ticks`, an input parameter to the method.
#' - \eqn{\min_i(X)} is the exponential moving minimum of column \eqn{X} at step \eqn{i}.
#' - \eqn{x_i} is the current value.
#' - \eqn{i} denotes the time step, ranging from \eqn{i=1} to \eqn{i = n-1}, where \eqn{n} is the number of elements in \eqn{X}.
#'
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `update_by()` caller
#' function. The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like the other Deephaven `uby_*` functions, is a generator function. That is, its output is another
#' function that is intended to be used in a call to `update_by()`. This detail is typically hidden from the user by
#' `update_by()`, which calls the generated functions internally. However, it is important to understand this detail
#' for debugging purposes, as the output of a `uby_*` function can otherwise seem unexpected.
#'
#' @param decay_ticks Numeric scalar denoting the decay rate in ticks.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the exponential moving minimum for all non-grouping columns.
#' @param operation_control OperationControl that defines how special cases will behave. See `?op_control` for more information.
#' @return UpdateByOp to be used in `update_by()`.
#'
#' @examples
#' print("hello!")
#'
#' @export
uby_emmin_tick <- function(decay_ticks, cols = character(), operation_control = op_control()) {
  verify_real("decay_ticks", decay_ticks, TRUE)
  verify_string("cols", cols, FALSE)
  verify_type("operation_control", operation_control, TRUE, "OperationControl", "a Deephaven OperationControl")
  return(UpdateByOp$new(INTERNAL_emmin_tick(decay_ticks, cols, operation_control$.internal_rcpp_object)))
}

#' @name
#' uby_emmin_time
#' @title
#' Exponential moving minimum by group with time as the decay unit
#' @md
#'
#' @description
#' Creates an exponential moving minimum (EMMIN) UpdateByOp for each column in `cols`, using time as the decay unit.
#'
#' @details
#' The formula used is
#' \deqn{a_i = e^{\frac{-dt_i}{\tau}}}
#' \deqn{\min_0(X) = x_0}
#' \deqn{\min_i(X) = \min(a*\min_{i-1}(X), \; x_i)}
#'
#' Where:
#' - \eqn{dt_i} is the difference between time \eqn{t_i} and \eqn{t_{i-1}} in nanoseconds.
#' - \eqn{\tau} is `decay_time` in nanoseconds, an input parameter to the method.
#' - \eqn{\min_i(X)} is the exponential moving minimum of column \eqn{X} at step \eqn{i}.
#' - \eqn{x_i} is the current value.
#' - \eqn{i} denotes the time step, ranging from \eqn{i=1} to \eqn{i = n-1}, where \eqn{n} is the number of elements in \eqn{X}.
#'
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `update_by()` caller
#' function. The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like the other Deephaven `uby_*` functions, is a generator function. That is, its output is another
#' function that is intended to be used in a call to `update_by()`. This detail is typically hidden from the user by
#' `update_by()`, which calls the generated functions internally. However, it is important to understand this detail
#' for debugging purposes, as the output of a `uby_*` function can otherwise seem unexpected.
#'
#' @param decay_time ISO-8601-formatted string specifying the decay rate.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the exponential moving minimum for all non-grouping columns.
#' @param operation_control OperationControl that defines how special cases will behave. See `?op_control` for more information.
#' @return UpdateByOp to be used in `update_by()`.
#'
#' @examples
#' print("hello!")
#'
#' @export
uby_emmin_time <- function(ts_col, decay_time, cols = character(), operation_control = op_control()) {
  verify_string("ts_col", ts_col, TRUE)
  verify_string("decay_time", decay_time, TRUE)
  verify_string("cols", cols, FALSE)
  verify_type("operation_control", operation_control, TRUE, "OperationControl", "a Deephaven OperationControl")
  return(UpdateByOp$new(INTERNAL_emmin_time(ts_col, decay_time, cols, operation_control$.internal_rcpp_object)))
}

#' @name
#' uby_emmax_tick
#' @title
#' Exponential moving maximum by group with ticks as the decay unit
#' @md
#'
#' @description
#' Creates an exponential moving maximum (EMMAX) UpdateByOp for each column in `cols`, using ticks as the decay unit.
#'
#' @details
#' The formula used is
#' \deqn{a = e^{\frac{-1}{\tau}}}
#' \deqn{\max_0(X) = x_0}
#' \deqn{\max_i(X) = \max(a*\max_{i-1}(X), \; x_i)}
#'
#' Where:
#' - \eqn{\tau} is `decay_ticks`, an input parameter to the method.
#' - \eqn{\max_i(X)} is the exponential moving maximum of column \eqn{X} at step \eqn{i}.
#' - \eqn{x_i} is the current value.
#' - \eqn{i} denotes the time step, ranging from \eqn{i=1} to \eqn{i = n-1}, where \eqn{n} is the number of elements in \eqn{X}.
#'
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `update_by()` caller
#' function. The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like the other Deephaven `uby_*` functions, is a generator function. That is, its output is another
#' function that is intended to be used in a call to `update_by()`. This detail is typically hidden from the user by
#' `update_by()`, which calls the generated functions internally. However, it is important to understand this detail
#' for debugging purposes, as the output of a `uby_*` function can otherwise seem unexpected.
#'
#' @param decay_ticks Numeric scalar denoting the decay rate in ticks.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the exponential moving maximum for all non-grouping columns.
#' @param operation_control OperationControl that defines how special cases will behave. See `?op_control` for more information.
#' @return UpdateByOp to be used in `update_by()`.
#'
#' @examples
#' print("hello!")
#'
#' @export
uby_emmax_tick <- function(decay_ticks, cols = character(), operation_control = op_control()) {
  verify_real("decay_ticks", decay_ticks, TRUE)
  verify_string("cols", cols, FALSE)
  verify_type("operation_control", operation_control, TRUE, "OperationControl", "a Deephaven OperationControl")
  return(UpdateByOp$new(INTERNAL_emmax_tick(decay_ticks, cols, operation_control$.internal_rcpp_object)))
}

#' @name
#' uby_emmax_time
#' @title
#' Exponential moving maximum by group with time as the decay unit
#' @md
#'
#' @description
#' Creates an exponential moving maximum (EMMAX) UpdateByOp for each column in `cols`, using time as the decay unit.
#'
#' @details
#' The formula used is
#' \deqn{a_i = e^{\frac{-dt_i}{\tau}}}
#' \deqn{\max_0(X) = x_0}
#' \deqn{\max_i(X) = \max(a*\max_{i-1}(X), \; x_i)}
#'
#' Where:
#' - \eqn{dt_i} is the difference between time \eqn{t_i} and \eqn{t_{i-1}} in nanoseconds.
#' - \eqn{\tau} is `decay_time` in nanoseconds, an input parameter to the method.
#' - \eqn{\max_i(X)} is the exponential moving maximum of column \eqn{X} at time step \eqn{i}.
#' - \eqn{x_i} is the current value.
#' - \eqn{i} denotes the time step, ranging from \eqn{i=1} to \eqn{i = n-1}, where \eqn{n} is the number of elements in \eqn{X}.
#'
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `update_by()` caller
#' function. The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like the other Deephaven `uby_*` functions, is a generator function. That is, its output is another
#' function that is intended to be used in a call to `update_by()`. This detail is typically hidden from the user by
#' `update_by()`, which calls the generated functions internally. However, it is important to understand this detail
#' for debugging purposes, as the output of a `uby_*` function can otherwise seem unexpected.
#'
#' @param decay_time ISO-8601-formatted string specifying the decay rate.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the exponential moving maximum for all non-grouping columns.
#' @param operation_control OperationControl that defines how special cases will behave. See `?op_control` for more information.
#' @return UpdateByOp to be used in `update_by()`.
#'
#' @examples
#' print("hello!")
#'
#' @export
uby_emmax_time <- function(ts_col, decay_time, cols = character(), operation_control = op_control()) {
  verify_string("ts_col", ts_col, TRUE)
  verify_string("decay_time", decay_time, TRUE)
  verify_string("cols", cols, FALSE)
  verify_type("operation_control", operation_control, TRUE, "OperationControl", "a Deephaven OperationControl")
  return(UpdateByOp$new(INTERNAL_emmax_time(ts_col, decay_time, cols, operation_control$.internal_rcpp_object)))
}

#' @name
#' uby_emstd_tick
#' @title
#' Exponential moving standard deviation by group with ticks as the decay unit
#' @md
#'
#' @description
#' Creates an exponential moving standard deviation (EMSTD) UpdateByOp for each column in `cols`, using ticks as the decay unit.
#'
#' @details
#' The formula used is
#' \deqn{a = e^{\frac{-1}{\tau}}}
#' \deqn{s^2_0 = 0}
#' \deqn{s^2_i = a*(s^2_{i-1} + (1-a)*(x_i - \bar{x}_{i-1})^2)}
#' \deqn{s_i = \sqrt{s^2_i}}
#'
#' Where:
#' - \eqn{\tau} is `decay_ticks`, an input parameter to the method.
#' - \eqn{\bar{x}_i} is the [exponential moving average](./ema-tick.md) of column \eqn{X} at step \eqn{i}.
#' - \eqn{s_i} is the exponential moving standard deviation of column \eqn{X} at step \eqn{i}.
#' - \eqn{x_i} is the current value.
#' - \eqn{i} denotes the time step, ranging from \eqn{i=1} to \eqn{i = n-1}, where \eqn{n} is the number of elements in \eqn{X}.
#'
#' Note that in the above formula, \eqn{s^2_0 = 0} yields the correct results for subsequent calculations. However,
#' sample variance for fewer than two data points is undefined, so the first element of an EMSTD calculation will always be `NaN`.
#'
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `update_by()` caller
#' function. The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like the other Deephaven `uby_*` functions, is a generator function. That is, its output is another
#' function that is intended to be used in a call to `update_by()`. This detail is typically hidden from the user by
#' `update_by()`, which calls the generated functions internally. However, it is important to understand this detail
#' for debugging purposes, as the output of a `uby_*` function can otherwise seem unexpected.
#'
#' @param decay_ticks Numeric scalar denoting the decay rate in ticks.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the exponential moving standard deviation for all non-grouping columns.
#' @param operation_control OperationControl that defines how special cases will behave. See `?op_control` for more information.
#' @return UpdateByOp to be used in `update_by()`.
#'
#' @examples
#' print("hello!")
#'
#' @export
uby_emstd_tick <- function(decay_ticks, cols = character(), operation_control = op_control()) {
  verify_real("decay_ticks", decay_ticks, TRUE)
  verify_string("cols", cols, FALSE)
  verify_type("operation_control", operation_control, TRUE, "OperationControl", "a Deephaven OperationControl")
  return(UpdateByOp$new(INTERNAL_emstd_tick(decay_ticks, cols, operation_control$.internal_rcpp_object)))
}

#' @name
#' uby_emstd_time
#' @title
#' Exponential moving standard deviation by group with time as the decay unit
#' @md
#'
#' @description
#' Creates an exponential moving standard deviation (EMSTD) UpdateByOp for each column in `cols`, using time as the decay unit.
#'
#' @details
#' The formula used is
#' \deqn{a_i = e^{\frac{-dt_i}{\tau}}}
#' \deqn{s^2_0 = 0}
#' \deqn{s^2_i = a_i*(s^2_{i-1} + (1-a_i)*(x_i - \bar{x}_{i-1})^2)}
#' \deqn{s_i = \sqrt{s^2_i}}
#'
#' Where:
#' - \eqn{dt_i} is the difference between time \eqn{t_i} and \eqn{t_{i-1}} in nanoseconds.
#' - \eqn{\tau} is `decay_time` in nanoseconds, an input parameter to the method.
#' - \eqn{\bar{x}_i} is the [exponential moving average](./ema-time.md) of column \eqn{X} at step \eqn{i}.
#' - \eqn{s_i} is the exponential moving standard deviation of column \eqn{X} at time step \eqn{i}.
#' - \eqn{x_i} is the current value.
#' - \eqn{i} denotes the time step, ranging from \eqn{i=1} to \eqn{i = n-1}, where \eqn{n} is the number of elements in \eqn{X}.
#'
#' Note that in the above formula, \eqn{s^2_0 = 0} yields the correct results for subsequent calculations. However,
#' sample variance for fewer than two data points is undefined, so the first element of an EMSTD calculation will always be `NaN`.
#'
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `update_by()` caller
#' function. The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like the other Deephaven `uby_*` functions, is a generator function. That is, its output is another
#' function that is intended to be used in a call to `update_by()`. This detail is typically hidden from the user by
#' `update_by()`, which calls the generated functions internally. However, it is important to understand this detail
#' for debugging purposes, as the output of a `uby_*` function can otherwise seem unexpected.
#'
#' @param decay_time ISO-8601-formatted string specifying the decay rate.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the exponential moving standard deviation for all non-grouping columns.
#' @param operation_control OperationControl that defines how special cases will behave. See `?op_control` for more information.
#' @return UpdateByOp to be used in `update_by()`.
#'
#' @examples
#' print("hello!")
#'
#' @export
uby_emstd_time <- function(ts_col, decay_time, cols = character(), operation_control = op_control()) {
  verify_string("ts_col", ts_col, TRUE)
  verify_string("decay_time", decay_time, TRUE)
  verify_string("cols", cols, FALSE)
  verify_type("operation_control", operation_control, TRUE, "OperationControl", "a Deephaven OperationControl")
  return(UpdateByOp$new(INTERNAL_emstd_time(ts_col, decay_time, cols, operation_control$.internal_rcpp_object)))
}

#' @name
#' uby_rolling_sum_tick
#' @title
#' Rolling sum by group with ticks as the windowing unit
#' @md
#'
#' @description
#' Creates a rolling sum UpdateByOp for each column in `cols`, using ticks as the windowing unit.
#'
#' @details
#' Ticks are row counts, and you may specify the reverse and forward window in number of rows to include.
#' The current row is considered to belong to the reverse window but not the forward window.
#' Also, negative values are allowed and can be used to generate completely forward or completely reverse windows.
#' Here are some examples of window values:
#' - `rev_ticks = 1, fwd_ticks = 0` - contains only the current row
#' - `rev_ticks = 10, fwd_ticks = 0` - contains 9 previous rows and the current row
#' - `rev_ticks = 0, fwd_ticks = 10` - contains the following 10 rows, excludes the current row
#' - `rev_ticks = 10, fwd_ticks = 10` - contains the previous 9 rows, the current row and the 10 rows following
#' - `rev_ticks = 10, fwd_ticks = -5` - contains 5 rows, beginning at 9 rows before, ending at 5 rows before
#'     the current row (inclusive)
#' - `rev_ticks = 11, fwd_ticks = -1` - contains 10 rows, beginning at 10 rows before, ending at 1 row before
#'     the current row (inclusive)
#' - `rev_ticks = -5, fwd_ticks = 10` - contains 5 rows, beginning 5 rows following, ending at 10 rows following
#'     the current row (inclusive)
#'
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `update_by()` caller
#' function. The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like the other Deephaven `uby_*` functions, is a generator function. That is, its output is another
#' function that is intended to be used in a call to `update_by()`. This detail is typically hidden from the user by
#' `update_by()`, which calls the generated functions internally. However, it is important to understand this detail
#' for debugging purposes, as the output of a `uby_*` function can otherwise seem unexpected.
#'
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling sum for all non-grouping columns.
#' @param rev_ticks Integer scalar denoting the look-behind window size in number of rows.
#' @param fwd_ticks Integer scalar denoting the look-ahead window size in number of rows. Default is 0.
#' @return UpdateByOp to be used in `update_by()`.
#'
#' @examples
#' print("hello!")
#'
#' @export
uby_rolling_sum_tick <- function(cols, rev_ticks, fwd_ticks = 0) {
  verify_string("cols", cols, FALSE)
  verify_any_int("rev_ticks", rev_ticks, TRUE)
  verify_any_int("fwd_ticks", fwd_ticks, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_sum_tick(cols, rev_ticks, fwd_ticks)))
}

#' @name
#' uby_rolling_sum_time
#' @title
#' Rolling sum by group with time as the windowing unit
#' @md
#'
#' @description
#' Creates a rolling sum UpdateByOp for each column in `cols`, using time as the windowing unit.
#'
#' @details
#' This uses ISO-8601 time strings as the reverse and forward window parameters.
#' Negative values are allowed and can be used to generate completely forward or completely reverse windows.
#' A row containing a null value in the timestamp column belongs to no window and will not be considered
#' in the windows of other rows; its output will be null.
#' Here are some examples of window values:
#' - `rev_time = "PT00:10:00", fwd_time = "PT00:10:00"` - contains rows from 10m before through 10m following
#'     the current row timestamp (inclusive)
#' - `rev_time = "PT00:10:00", fwd_time = "-PT00:05:00"` - contains rows from 10m before through 5m before the
#'     current row timestamp (inclusive), this is a purely backwards looking window
#' - `rev_time = "-PT00:05:00", fwd_time = "PT00:10:00"` - contains rows from 5m following through 10m
#'     following the current row timestamp (inclusive), this is a purely forwards looking window
#'
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `update_by()` caller
#' function. The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like the other Deephaven `uby_*` functions, is a generator function. That is, its output is another
#' function that is intended to be used in a call to `update_by()`. This detail is typically hidden from the user by
#' `update_by()`, which calls the generated functions internally. However, it is important to understand this detail
#' for debugging purposes, as the output of a `uby_*` function can otherwise seem unexpected.
#'
#' @param ts_col String denoting the column to use as the timestamp.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling sum for all non-grouping columns.
#' @param rev_time ISO-8601-formatted string specifying the look-behind window size.
#' @param fwd_time ISO-8601-formatted string specifying the look-ahead window size. Default is 0 seconds.
#' @return UpdateByOp to be used in `update_by()`.
#'
#' @examples
#' print("hello!")
#'
#' @export
uby_rolling_sum_time <- function(ts_col, cols, rev_time, fwd_time = "PT0s") {
  verify_string("ts_col", ts_col, TRUE)
  verify_string("cols", cols, FALSE)
  verify_string("rev_time", rev_time, TRUE)
  verify_string("fwd_time", fwd_time, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_sum_time(ts_col, cols, rev_time, fwd_time)))
}

#' @name
#' uby_rolling_group_tick
#' @title
#' Rolling grouping with ticks as the windowing unit
#' @md
#'
#' @description
#' Creates a rolling group UpdateByOp for each column in `cols`, using ticks as the windowing unit.
#'
#' @details
#' Ticks are row counts, and you may specify the reverse and forward window in number of rows to include.
#' The current row is considered to belong to the reverse window but not the forward window.
#' Also, negative values are allowed and can be used to generate completely forward or completely reverse windows.
#' Here are some examples of window values:
#' - `rev_ticks = 1, fwd_ticks = 0` - contains only the current row
#' - `rev_ticks = 10, fwd_ticks = 0` - contains 9 previous rows and the current row
#' - `rev_ticks = 0, fwd_ticks = 10` - contains the following 10 rows, excludes the current row
#' - `rev_ticks = 10, fwd_ticks = 10` - contains the previous 9 rows, the current row and the 10 rows following
#' - `rev_ticks = 10, fwd_ticks = -5` - contains 5 rows, beginning at 9 rows before, ending at 5 rows before
#'     the current row (inclusive)
#' - `rev_ticks = 11, fwd_ticks = -1` - contains 10 rows, beginning at 10 rows before, ending at 1 row before
#'     the current row (inclusive)
#' - `rev_ticks = -5, fwd_ticks = 10` - contains 5 rows, beginning 5 rows following, ending at 10 rows following
#'     the current row (inclusive)
#'
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `update_by()` caller
#' function. The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like the other Deephaven `uby_*` functions, is a generator function. That is, its output is another
#' function that is intended to be used in a call to `update_by()`. This detail is typically hidden from the user by
#' `update_by()`, which calls the generated functions internally. However, it is important to understand this detail
#' for debugging purposes, as the output of a `uby_*` function can otherwise seem unexpected.
#'
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling sum for all non-grouping columns.
#' @param rev_ticks Integer scalar denoting the look-behind window size in number of rows.
#' @param fwd_ticks Integer scalar denoting the look-ahead window size in number of rows. Default is 0.
#' @return UpdateByOp to be used in `update_by()`.
#'
#' @examples
#' print("hello!")
#'
#' @export
uby_rolling_group_tick <- function(cols, rev_ticks, fwd_ticks = 0) {
  verify_string("cols", cols, FALSE)
  verify_any_int("rev_ticks", rev_ticks, TRUE)
  verify_any_int("fwd_ticks", fwd_ticks, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_group_tick(cols, rev_ticks, fwd_ticks)))
}

#' @name
#' uby_rolling_group_time
#' @title
#' Rolling grouping with time as the windowing unit
#' @md
#'
#' @description
#' Creates a rolling group UpdateByOp for each column in `cols`, using time as the windowing unit.
#'
#' @details
#' This uses ISO-8601 time strings as the reverse and forward window parameters.
#' Negative values are allowed and can be used to generate completely forward or completely reverse windows.
#' A row containing a null value in the timestamp column belongs to no window and will not be considered
#' in the windows of other rows; its output will be null.
#' Here are some examples of window values:
#' - `rev_time = "PT00:10:00", fwd_time = "PT00:10:00"` - contains rows from 10m before through 10m following
#'     the current row timestamp (inclusive)
#' - `rev_time = "PT00:10:00", fwd_time = "-PT00:05:00"` - contains rows from 10m before through 5m before the
#'     current row timestamp (inclusive), this is a purely backwards looking window
#' - `rev_time = "-PT00:05:00", fwd_time = "PT00:10:00"` - contains rows from 5m following through 10m
#'     following the current row timestamp (inclusive), this is a purely forwards looking window
#'
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `update_by()` caller
#' function. The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like the other Deephaven `uby_*` functions, is a generator function. That is, its output is another
#' function that is intended to be used in a call to `update_by()`. This detail is typically hidden from the user by
#' `update_by()`, which calls the generated functions internally. However, it is important to understand this detail
#' for debugging purposes, as the output of a `uby_*` function can otherwise seem unexpected.
#'
#' @param ts_col String denoting the column to use as the timestamp.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling sum for all non-grouping columns.
#' @param rev_time ISO-8601-formatted string specifying the look-behind window size.
#' @param fwd_time ISO-8601-formatted string specifying the look-ahead window size. Default is 0 seconds.
#' @return UpdateByOp to be used in `update_by()`.
#'
#' @examples
#' print("hello!")
#'
#' @export
uby_rolling_group_time <- function(ts_col, cols, rev_time, fwd_time = "PT0s") {
  verify_string("ts_col", ts_col, TRUE)
  verify_string("cols", cols, FALSE)
  verify_string("rev_time", rev_time, TRUE)
  verify_string("fwd_time", fwd_time, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_group_time(ts_col, cols, rev_time, fwd_time)))
}

#' @name
#' uby_rolling_avg_tick
#' @title
#' Rolling average with ticks as the windowing unit
#' @md
#'
#' @description
#' Creates a simple moving average (SMA) UpdateByOp for each column in `cols`, using ticks as the windowing unit.
#'
#' @details
#' Ticks are row counts, and you may specify the reverse and forward window in number of rows to include.
#' The current row is considered to belong to the reverse window but not the forward window.
#' Also, negative values are allowed and can be used to generate completely forward or completely reverse windows.
#' Here are some examples of window values:
#' - `rev_ticks = 1, fwd_ticks = 0` - contains only the current row
#' - `rev_ticks = 10, fwd_ticks = 0` - contains 9 previous rows and the current row
#' - `rev_ticks = 0, fwd_ticks = 10` - contains the following 10 rows, excludes the current row
#' - `rev_ticks = 10, fwd_ticks = 10` - contains the previous 9 rows, the current row and the 10 rows following
#' - `rev_ticks = 10, fwd_ticks = -5` - contains 5 rows, beginning at 9 rows before, ending at 5 rows before
#'     the current row (inclusive)
#' - `rev_ticks = 11, fwd_ticks = -1` - contains 10 rows, beginning at 10 rows before, ending at 1 row before
#'     the current row (inclusive)
#' - `rev_ticks = -5, fwd_ticks = 10` - contains 5 rows, beginning 5 rows following, ending at 10 rows following
#'     the current row (inclusive)
#'
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `update_by()` caller
#' function. The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like the other Deephaven `uby_*` functions, is a generator function. That is, its output is another
#' function that is intended to be used in a call to `update_by()`. This detail is typically hidden from the user by
#' `update_by()`, which calls the generated functions internally. However, it is important to understand this detail
#' for debugging purposes, as the output of a `uby_*` function can otherwise seem unexpected.
#'
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling sum for all non-grouping columns.
#' @param rev_ticks Integer scalar denoting the look-behind window size in number of rows.
#' @param fwd_ticks Integer scalar denoting the look-ahead window size in number of rows. Default is 0.
#' @return UpdateByOp to be used in `update_by()`.
#'
#' @examples
#' print("hello!")
#'
#' @export
uby_rolling_avg_tick <- function(cols, rev_ticks, fwd_ticks = 0) {
  verify_string("cols", cols, FALSE)
  verify_any_int("rev_ticks", rev_ticks, TRUE)
  verify_any_int("fwd_ticks", fwd_ticks, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_avg_tick(cols, rev_ticks, fwd_ticks)))
}

#' @name
#' uby_rolling_avg_time
#' @title
#' Rolling average with time as the windowing unit
#' @md
#'
#' @description
#' Creates a simple moving average (SMA) UpdateByOp for each column in `cols`, using time as the windowing unit.
#'
#' @details
#' This uses ISO-8601 time strings as the reverse and forward window parameters.
#' Negative values are allowed and can be used to generate completely forward or completely reverse windows.
#' A row containing a null value in the timestamp column belongs to no window and will not be considered
#' in the windows of other rows; its output will be null.
#' Here are some examples of window values:
#' - `rev_time = "PT00:10:00", fwd_time = "PT00:10:00"` - contains rows from 10m before through 10m following
#'     the current row timestamp (inclusive)
#' - `rev_time = "PT00:10:00", fwd_time = "-PT00:05:00"` - contains rows from 10m before through 5m before the
#'     current row timestamp (inclusive), this is a purely backwards looking window
#' - `rev_time = "-PT00:05:00", fwd_time = "PT00:10:00"` - contains rows from 5m following through 10m
#'     following the current row timestamp (inclusive), this is a purely forwards looking window
#'
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `update_by()` caller
#' function. The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like the other Deephaven `uby_*` functions, is a generator function. That is, its output is another
#' function that is intended to be used in a call to `update_by()`. This detail is typically hidden from the user by
#' `update_by()`, which calls the generated functions internally. However, it is important to understand this detail
#' for debugging purposes, as the output of a `uby_*` function can otherwise seem unexpected.
#'
#' @param ts_col String denoting the column to use as the timestamp.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling sum for all non-grouping columns.
#' @param rev_time ISO-8601-formatted string specifying the look-behind window size.
#' @param fwd_time ISO-8601-formatted string specifying the look-ahead window size. Default is 0 seconds.
#' @return UpdateByOp to be used in `update_by()`.
#'
#' @examples
#' print("hello!")
#'
#' @export
uby_rolling_avg_time <- function(ts_col, cols, rev_time, fwd_time = "PT0s") {
  verify_string("ts_col", ts_col, TRUE)
  verify_string("cols", cols, FALSE)
  verify_string("rev_time", rev_time, TRUE)
  verify_string("fwd_time", fwd_time, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_avg_time(ts_col, cols, rev_time, fwd_time)))
}

#' @name
#' uby_rolling_min_tick
#' @title
#' Rolling minimum with ticks as the windowing unit
#' @md
#'
#' @description
#' Creates a rolling minimum UpdateByOp for each column in `cols`, using ticks as the windowing unit.
#'
#' @details
#' Ticks are row counts, and you may specify the reverse and forward window in number of rows to include.
#' The current row is considered to belong to the reverse window but not the forward window.
#' Also, negative values are allowed and can be used to generate completely forward or completely reverse windows.
#' Here are some examples of window values:
#' - `rev_ticks = 1, fwd_ticks = 0` - contains only the current row
#' - `rev_ticks = 10, fwd_ticks = 0` - contains 9 previous rows and the current row
#' - `rev_ticks = 0, fwd_ticks = 10` - contains the following 10 rows, excludes the current row
#' - `rev_ticks = 10, fwd_ticks = 10` - contains the previous 9 rows, the current row and the 10 rows following
#' - `rev_ticks = 10, fwd_ticks = -5` - contains 5 rows, beginning at 9 rows before, ending at 5 rows before
#'     the current row (inclusive)
#' - `rev_ticks = 11, fwd_ticks = -1` - contains 10 rows, beginning at 10 rows before, ending at 1 row before
#'     the current row (inclusive)
#' - `rev_ticks = -5, fwd_ticks = 10` - contains 5 rows, beginning 5 rows following, ending at 10 rows following
#'     the current row (inclusive)
#'
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `update_by()` caller
#' function. The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like the other Deephaven `uby_*` functions, is a generator function. That is, its output is another
#' function that is intended to be used in a call to `update_by()`. This detail is typically hidden from the user by
#' `update_by()`, which calls the generated functions internally. However, it is important to understand this detail
#' for debugging purposes, as the output of a `uby_*` function can otherwise seem unexpected.
#'
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling sum for all non-grouping columns.
#' @param rev_ticks Integer scalar denoting the look-behind window size in number of rows.
#' @param fwd_ticks Integer scalar denoting the look-ahead window size in number of rows. Default is 0.
#' @return UpdateByOp to be used in `update_by()`.
#'
#' @examples
#' print("hello!")
#'
#' @export
uby_rolling_min_tick <- function(cols, rev_ticks, fwd_ticks = 0) {
  verify_string("cols", cols, FALSE)
  verify_any_int("rev_ticks", rev_ticks, TRUE)
  verify_any_int("fwd_ticks", fwd_ticks, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_min_tick(cols, rev_ticks, fwd_ticks)))
}

#' @name
#' uby_rolling_min_time
#' @title
#' Rolling minimum with time as the windowing unit
#' @md
#'
#' @description
#' Creates a rolling minimum UpdateByOp for each column in `cols`, using time as the windowing unit.
#'
#' @details
#' This uses ISO-8601 time strings as the reverse and forward window parameters.
#' Negative values are allowed and can be used to generate completely forward or completely reverse windows.
#' A row containing a null value in the timestamp column belongs to no window and will not be considered
#' in the windows of other rows; its output will be null.
#' Here are some examples of window values:
#' - `rev_time = "PT00:10:00", fwd_time = "PT00:10:00"` - contains rows from 10m before through 10m following
#'     the current row timestamp (inclusive)
#' - `rev_time = "PT00:10:00", fwd_time = "-PT00:05:00"` - contains rows from 10m before through 5m before the
#'     current row timestamp (inclusive), this is a purely backwards looking window
#' - `rev_time = "-PT00:05:00", fwd_time = "PT00:10:00"` - contains rows from 5m following through 10m
#'     following the current row timestamp (inclusive), this is a purely forwards looking window
#'
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `update_by()` caller
#' function. The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like the other Deephaven `uby_*` functions, is a generator function. That is, its output is another
#' function that is intended to be used in a call to `update_by()`. This detail is typically hidden from the user by
#' `update_by()`, which calls the generated functions internally. However, it is important to understand this detail
#' for debugging purposes, as the output of a `uby_*` function can otherwise seem unexpected.
#'
#' @param ts_col String denoting the column to use as the timestamp.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling sum for all non-grouping columns.
#' @param rev_time ISO-8601-formatted string specifying the look-behind window size.
#' @param fwd_time ISO-8601-formatted string specifying the look-ahead window size. Default is 0 seconds.
#' @return UpdateByOp to be used in `update_by()`.
#'
#' @examples
#' print("hello!")
#'
#' @export
uby_rolling_min_time <- function(ts_col, cols, rev_time, fwd_time = "PT0s") {
  verify_string("ts_col", ts_col, TRUE)
  verify_string("cols", cols, FALSE)
  verify_string("rev_time", rev_time, TRUE)
  verify_string("fwd_time", fwd_time, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_min_time(ts_col, cols, rev_time, fwd_time)))
}

#' @name
#' uby_rolling_max_tick
#' @title
#' Rolling maximum with ticks as the windowing unit
#' @md
#'
#' @description
#' Creates a rolling maximum UpdateByOp for each column in `cols`, using ticks as the windowing unit.
#'
#' @details
#' Ticks are row counts, and you may specify the reverse and forward window in number of rows to include.
#' The current row is considered to belong to the reverse window but not the forward window.
#' Also, negative values are allowed and can be used to generate completely forward or completely reverse windows.
#' Here are some examples of window values:
#' - `rev_ticks = 1, fwd_ticks = 0` - contains only the current row
#' - `rev_ticks = 10, fwd_ticks = 0` - contains 9 previous rows and the current row
#' - `rev_ticks = 0, fwd_ticks = 10` - contains the following 10 rows, excludes the current row
#' - `rev_ticks = 10, fwd_ticks = 10` - contains the previous 9 rows, the current row and the 10 rows following
#' - `rev_ticks = 10, fwd_ticks = -5` - contains 5 rows, beginning at 9 rows before, ending at 5 rows before
#'     the current row (inclusive)
#' - `rev_ticks = 11, fwd_ticks = -1` - contains 10 rows, beginning at 10 rows before, ending at 1 row before
#'     the current row (inclusive)
#' - `rev_ticks = -5, fwd_ticks = 10` - contains 5 rows, beginning 5 rows following, ending at 10 rows following
#'     the current row (inclusive)
#'
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `update_by()` caller
#' function. The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like the other Deephaven `uby_*` functions, is a generator function. That is, its output is another
#' function that is intended to be used in a call to `update_by()`. This detail is typically hidden from the user by
#' `update_by()`, which calls the generated functions internally. However, it is important to understand this detail
#' for debugging purposes, as the output of a `uby_*` function can otherwise seem unexpected.
#'
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling sum for all non-grouping columns.
#' @param rev_ticks Integer scalar denoting the look-behind window size in number of rows.
#' @param fwd_ticks Integer scalar denoting the look-ahead window size in number of rows. Default is 0.
#' @return UpdateByOp to be used in `update_by()`.
#'
#' @examples
#' print("hello!")
#'
#' @export
uby_rolling_max_tick <- function(cols, rev_ticks, fwd_ticks = 0) {
  verify_string("cols", cols, FALSE)
  verify_any_int("rev_ticks", rev_ticks, TRUE)
  verify_any_int("fwd_ticks", fwd_ticks, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_max_tick(cols, rev_ticks, fwd_ticks)))
}

#' @name
#' uby_rolling_max_time
#' @title
#' Rolling maximum with time as the windowing unit
#' @md
#'
#' @description
#' Creates a rolling maximum UpdateByOp for each column in `cols`, using time as the windowing unit.
#'
#' @details
#' This uses ISO-8601 time strings as the reverse and forward window parameters.
#' Negative values are allowed and can be used to generate completely forward or completely reverse windows.
#' A row containing a null value in the timestamp column belongs to no window and will not be considered
#' in the windows of other rows; its output will be null.
#' Here are some examples of window values:
#' - `rev_time = "PT00:10:00", fwd_time = "PT00:10:00"` - contains rows from 10m before through 10m following
#'     the current row timestamp (inclusive)
#' - `rev_time = "PT00:10:00", fwd_time = "-PT00:05:00"` - contains rows from 10m before through 5m before the
#'     current row timestamp (inclusive), this is a purely backwards looking window
#' - `rev_time = "-PT00:05:00", fwd_time = "PT00:10:00"` - contains rows from 5m following through 10m
#'     following the current row timestamp (inclusive), this is a purely forwards looking window
#'
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `update_by()` caller
#' function. The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like the other Deephaven `uby_*` functions, is a generator function. That is, its output is another
#' function that is intended to be used in a call to `update_by()`. This detail is typically hidden from the user by
#' `update_by()`, which calls the generated functions internally. However, it is important to understand this detail
#' for debugging purposes, as the output of a `uby_*` function can otherwise seem unexpected.
#'
#' @param ts_col String denoting the column to use as the timestamp.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling sum for all non-grouping columns.
#' @param rev_time ISO-8601-formatted string specifying the look-behind window size.
#' @param fwd_time ISO-8601-formatted string specifying the look-ahead window size. Default is 0 seconds.
#' @return UpdateByOp to be used in `update_by()`.
#'
#' @examples
#' print("hello!")
#'
#' @export
uby_rolling_max_time <- function(ts_col, cols, rev_time, fwd_time = "PT0s") {
  verify_string("ts_col", ts_col, TRUE)
  verify_string("cols", cols, FALSE)
  verify_string("rev_time", rev_time, TRUE)
  verify_string("fwd_time", fwd_time, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_max_time(ts_col, cols, rev_time, fwd_time)))
}

#' @name
#' uby_rolling_prod_tick
#' @title
#' Rolling prod with ticks as the windowing unit
#' @md
#'
#' @description
#' Creates a rolling product UpdateByOp for each column in `cols`, using ticks as the windowing unit.
#'
#' @details
#' Ticks are row counts, and you may specify the reverse and forward window in number of rows to include.
#' The current row is considered to belong to the reverse window but not the forward window.
#' Also, negative values are allowed and can be used to generate completely forward or completely reverse windows.
#' Here are some examples of window values:
#' - `rev_ticks = 1, fwd_ticks = 0` - contains only the current row
#' - `rev_ticks = 10, fwd_ticks = 0` - contains 9 previous rows and the current row
#' - `rev_ticks = 0, fwd_ticks = 10` - contains the following 10 rows, excludes the current row
#' - `rev_ticks = 10, fwd_ticks = 10` - contains the previous 9 rows, the current row and the 10 rows following
#' - `rev_ticks = 10, fwd_ticks = -5` - contains 5 rows, beginning at 9 rows before, ending at 5 rows before
#'     the current row (inclusive)
#' - `rev_ticks = 11, fwd_ticks = -1` - contains 10 rows, beginning at 10 rows before, ending at 1 row before
#'     the current row (inclusive)
#' - `rev_ticks = -5, fwd_ticks = 10` - contains 5 rows, beginning 5 rows following, ending at 10 rows following
#'     the current row (inclusive)
#'
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `update_by()` caller
#' function. The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like the other Deephaven `uby_*` functions, is a generator function. That is, its output is another
#' function that is intended to be used in a call to `update_by()`. This detail is typically hidden from the user by
#' `update_by()`, which calls the generated functions internally. However, it is important to understand this detail
#' for debugging purposes, as the output of a `uby_*` function can otherwise seem unexpected.
#'
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling sum for all non-grouping columns.
#' @param rev_ticks Integer scalar denoting the look-behind window size in number of rows.
#' @param fwd_ticks Integer scalar denoting the look-ahead window size in number of rows. Default is 0.
#' @return UpdateByOp to be used in `update_by()`.
#'
#' @examples
#' print("hello!")
#'
#' @export
uby_rolling_prod_tick <- function(cols, rev_ticks, fwd_ticks = 0) {
  verify_string("cols", cols, FALSE)
  verify_any_int("rev_ticks", rev_ticks, TRUE)
  verify_any_int("fwd_ticks", fwd_ticks, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_prod_tick(cols, rev_ticks, fwd_ticks)))
}

#' @name
#' uby_rolling_prod_time
#' @title
#' Rolling product with time as the windowing unit
#' @md
#'
#' @description
#' Creates a rolling product UpdateByOp for each column in `cols`, using time as the windowing unit.
#'
#' @details
#' This uses ISO-8601 time strings as the reverse and forward window parameters.
#' Negative values are allowed and can be used to generate completely forward or completely reverse windows.
#' A row containing a null value in the timestamp column belongs to no window and will not be considered
#' in the windows of other rows; its output will be null.
#' Here are some examples of window values:
#' - `rev_time = "PT00:10:00", fwd_time = "PT00:10:00"` - contains rows from 10m before through 10m following
#'     the current row timestamp (inclusive)
#' - `rev_time = "PT00:10:00", fwd_time = "-PT00:05:00"` - contains rows from 10m before through 5m before the
#'     current row timestamp (inclusive), this is a purely backwards looking window
#' - `rev_time = "-PT00:05:00", fwd_time = "PT00:10:00"` - contains rows from 5m following through 10m
#'     following the current row timestamp (inclusive), this is a purely forwards looking window
#'
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `update_by()` caller
#' function. The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like the other Deephaven `uby_*` functions, is a generator function. That is, its output is another
#' function that is intended to be used in a call to `update_by()`. This detail is typically hidden from the user by
#' `update_by()`, which calls the generated functions internally. However, it is important to understand this detail
#' for debugging purposes, as the output of a `uby_*` function can otherwise seem unexpected.
#'
#' @param ts_col String denoting the column to use as the timestamp.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling sum for all non-grouping columns.
#' @param rev_time ISO-8601-formatted string specifying the look-behind window size.
#' @param fwd_time ISO-8601-formatted string specifying the look-ahead window size. Default is 0 seconds.
#' @return UpdateByOp to be used in `update_by()`.
#'
#' @examples
#' print("hello!")
#'
#' @export
uby_rolling_prod_time <- function(ts_col, cols, rev_time, fwd_time = "PT0s") {
  verify_string("ts_col", ts_col, TRUE)
  verify_string("cols", cols, FALSE)
  verify_string("rev_time", rev_time, TRUE)
  verify_string("fwd_time", fwd_time, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_prod_time(ts_col, cols, rev_time, fwd_time)))
}

#' @name
#' uby_rolling_count_tick
#' @title
#' Rolling count with ticks as the windowing unit
#' @md
#'
#' @description
#' Creates a rolling count UpdateByOp for each column in `cols`, using ticks as the windowing unit.
#'
#' @details
#' Ticks are row counts, and you may specify the reverse and forward window in number of rows to include.
#' The current row is considered to belong to the reverse window but not the forward window.
#' Also, negative values are allowed and can be used to generate completely forward or completely reverse windows.
#' Here are some examples of window values:
#' - `rev_ticks = 1, fwd_ticks = 0` - contains only the current row
#' - `rev_ticks = 10, fwd_ticks = 0` - contains 9 previous rows and the current row
#' - `rev_ticks = 0, fwd_ticks = 10` - contains the following 10 rows, excludes the current row
#' - `rev_ticks = 10, fwd_ticks = 10` - contains the previous 9 rows, the current row and the 10 rows following
#' - `rev_ticks = 10, fwd_ticks = -5` - contains 5 rows, beginning at 9 rows before, ending at 5 rows before
#'     the current row (inclusive)
#' - `rev_ticks = 11, fwd_ticks = -1` - contains 10 rows, beginning at 10 rows before, ending at 1 row before
#'     the current row (inclusive)
#' - `rev_ticks = -5, fwd_ticks = 10` - contains 5 rows, beginning 5 rows following, ending at 10 rows following
#'     the current row (inclusive)
#'
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `update_by()` caller
#' function. The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like the other Deephaven `uby_*` functions, is a generator function. That is, its output is another
#' function that is intended to be used in a call to `update_by()`. This detail is typically hidden from the user by
#' `update_by()`, which calls the generated functions internally. However, it is important to understand this detail
#' for debugging purposes, as the output of a `uby_*` function can otherwise seem unexpected.
#'
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling sum for all non-grouping columns.
#' @param rev_ticks Integer scalar denoting the look-behind window size in number of rows.
#' @param fwd_ticks Integer scalar denoting the look-ahead window size in number of rows. Default is 0.
#' @return UpdateByOp to be used in `update_by()`.
#'
#' @examples
#' print("hello!")
#'
#' @export
uby_rolling_count_tick <- function(cols, rev_ticks, fwd_ticks = 0) {
  verify_string("cols", cols, FALSE)
  verify_any_int("rev_ticks", rev_ticks, TRUE)
  verify_any_int("fwd_ticks", fwd_ticks, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_count_tick(cols, rev_ticks, fwd_ticks)))
}

#' @name
#' uby_rolling_count_time
#' @title
#' Rolling count with time as the windowing unit
#' @md
#'
#' @description
#' Creates a rolling count UpdateByOp for each column in `cols`, using time as the windowing unit.
#'
#' @details
#' This uses ISO-8601 time strings as the reverse and forward window parameters.
#' Negative values are allowed and can be used to generate completely forward or completely reverse windows.
#' A row containing a null value in the timestamp column belongs to no window and will not be considered
#' in the windows of other rows; its output will be null.
#' Here are some examples of window values:
#' - `rev_time = "PT00:10:00", fwd_time = "PT00:10:00"` - contains rows from 10m before through 10m following
#'     the current row timestamp (inclusive)
#' - `rev_time = "PT00:10:00", fwd_time = "-PT00:05:00"` - contains rows from 10m before through 5m before the
#'     current row timestamp (inclusive), this is a purely backwards looking window
#' - `rev_time = "-PT00:05:00", fwd_time = "PT00:10:00"` - contains rows from 5m following through 10m
#'     following the current row timestamp (inclusive), this is a purely forwards looking window
#'
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `update_by()` caller
#' function. The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like the other Deephaven `uby_*` functions, is a generator function. That is, its output is another
#' function that is intended to be used in a call to `update_by()`. This detail is typically hidden from the user by
#' `update_by()`, which calls the generated functions internally. However, it is important to understand this detail
#' for debugging purposes, as the output of a `uby_*` function can otherwise seem unexpected.
#'
#' @param ts_col String denoting the column to use as the timestamp.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling sum for all non-grouping columns.
#' @param rev_time ISO-8601-formatted string specifying the look-behind window size.
#' @param fwd_time ISO-8601-formatted string specifying the look-ahead window size. Default is 0 seconds.
#' @return UpdateByOp to be used in `update_by()`.
#'
#' @examples
#' print("hello!")
#'
#' @export
uby_rolling_count_time <- function(ts_col, cols, rev_time, fwd_time = "PT0s") {
  verify_string("ts_col", ts_col, TRUE)
  verify_string("cols", cols, FALSE)
  verify_string("rev_time", rev_time, TRUE)
  verify_string("fwd_time", fwd_time, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_count_time(ts_col, cols, rev_time, fwd_time)))
}

#' @name
#' uby_rolling_std_tick
#' @title
#' Rolling standard deviation with ticks as the windowing unit
#' @md
#'
#' @description
#' Creates a rolling standard deviation UpdateByOp for each column in `cols`, using ticks as the windowing unit.
#'
#' @details
#' Ticks are row counts, and you may specify the reverse and forward window in number of rows to include.
#' The current row is considered to belong to the reverse window but not the forward window.
#' Also, negative values are allowed and can be used to generate completely forward or completely reverse windows.
#' Here are some examples of window values:
#' - `rev_ticks = 1, fwd_ticks = 0` - contains only the current row
#' - `rev_ticks = 10, fwd_ticks = 0` - contains 9 previous rows and the current row
#' - `rev_ticks = 0, fwd_ticks = 10` - contains the following 10 rows, excludes the current row
#' - `rev_ticks = 10, fwd_ticks = 10` - contains the previous 9 rows, the current row and the 10 rows following
#' - `rev_ticks = 10, fwd_ticks = -5` - contains 5 rows, beginning at 9 rows before, ending at 5 rows before
#'     the current row (inclusive)
#' - `rev_ticks = 11, fwd_ticks = -1` - contains 10 rows, beginning at 10 rows before, ending at 1 row before
#'     the current row (inclusive)
#' - `rev_ticks = -5, fwd_ticks = 10` - contains 5 rows, beginning 5 rows following, ending at 10 rows following
#'     the current row (inclusive)
#'
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `update_by()` caller
#' function. The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like the other Deephaven `uby_*` functions, is a generator function. That is, its output is another
#' function that is intended to be used in a call to `update_by()`. This detail is typically hidden from the user by
#' `update_by()`, which calls the generated functions internally. However, it is important to understand this detail
#' for debugging purposes, as the output of a `uby_*` function can otherwise seem unexpected.
#'
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling sum for all non-grouping columns.
#' @param rev_ticks Integer scalar denoting the look-behind window size in number of rows.
#' @param fwd_ticks Integer scalar denoting the look-ahead window size in number of rows. Default is 0.
#' @return UpdateByOp to be used in `update_by()`.
#'
#' @examples
#' print("hello!")
#'
#' @export
uby_rolling_std_tick <- function(cols, rev_ticks, fwd_ticks = 0) {
  verify_string("cols", cols, FALSE)
  verify_any_int("rev_ticks", rev_ticks, TRUE)
  verify_any_int("fwd_ticks", fwd_ticks, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_std_tick(cols, rev_ticks, fwd_ticks)))
}

#' @name
#' uby_rolling_std_time
#' @title
#' Rolling standard deviation with time as the windowing unit
#' @md
#'
#' @description
#' Creates a rolling standard deviation UpdateByOp for each column in `cols`, using time as the windowing unit.
#'
#' @details
#' This uses ISO-8601 time strings as the reverse and forward window parameters.
#' Negative values are allowed and can be used to generate completely forward or completely reverse windows.
#' A row containing a null value in the timestamp column belongs to no window and will not be considered
#' in the windows of other rows; its output will be null.
#' Here are some examples of window values:
#' - `rev_time = "PT00:10:00", fwd_time = "PT00:10:00"` - contains rows from 10m before through 10m following
#'     the current row timestamp (inclusive)
#' - `rev_time = "PT00:10:00", fwd_time = "-PT00:05:00"` - contains rows from 10m before through 5m before the
#'     current row timestamp (inclusive), this is a purely backwards looking window
#' - `rev_time = "-PT00:05:00", fwd_time = "PT00:10:00"` - contains rows from 5m following through 10m
#'     following the current row timestamp (inclusive), this is a purely forwards looking window
#'
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `update_by()` caller
#' function. The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like the other Deephaven `uby_*` functions, is a generator function. That is, its output is another
#' function that is intended to be used in a call to `update_by()`. This detail is typically hidden from the user by
#' `update_by()`, which calls the generated functions internally. However, it is important to understand this detail
#' for debugging purposes, as the output of a `uby_*` function can otherwise seem unexpected.
#'
#' @param ts_col String denoting the column to use as the timestamp.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling sum for all non-grouping columns.
#' @param rev_time ISO-8601-formatted string specifying the look-behind window size.
#' @param fwd_time ISO-8601-formatted string specifying the look-ahead window size. Default is 0 seconds.
#' @return UpdateByOp to be used in `update_by()`.
#'
#' @examples
#' print("hello!")
#'
#' @export
uby_rolling_std_time <- function(ts_col, cols, rev_time, fwd_time = "PT0s") {
  verify_string("ts_col", ts_col, TRUE)
  verify_string("cols", cols, FALSE)
  verify_string("rev_time", rev_time, TRUE)
  verify_string("fwd_time", fwd_time, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_std_time(ts_col, cols, rev_time, fwd_time)))
}

#' @name
#' uby_rolling_wavg_tick
#' @title
#' Rolling weighted average with ticks as the windowing unit
#' @md
#'
#' @description
#' Creates a rolling weighted average UpdateByOp for each column in `cols`, using ticks as the windowing unit.
#'
#' @details
#' Ticks are row counts, and you may specify the reverse and forward window in number of rows to include.
#' The current row is considered to belong to the reverse window but not the forward window.
#' Also, negative values are allowed and can be used to generate completely forward or completely reverse windows.
#' Here are some examples of window values:
#' - `rev_ticks = 1, fwd_ticks = 0` - contains only the current row
#' - `rev_ticks = 10, fwd_ticks = 0` - contains 9 previous rows and the current row
#' - `rev_ticks = 0, fwd_ticks = 10` - contains the following 10 rows, excludes the current row
#' - `rev_ticks = 10, fwd_ticks = 10` - contains the previous 9 rows, the current row and the 10 rows following
#' - `rev_ticks = 10, fwd_ticks = -5` - contains 5 rows, beginning at 9 rows before, ending at 5 rows before
#'     the current row (inclusive)
#' - `rev_ticks = 11, fwd_ticks = -1` - contains 10 rows, beginning at 10 rows before, ending at 1 row before
#'     the current row (inclusive)
#' - `rev_ticks = -5, fwd_ticks = 10` - contains 5 rows, beginning 5 rows following, ending at 10 rows following
#'     the current row (inclusive)
#'
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `update_by()` caller
#' function. The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like the other Deephaven `uby_*` functions, is a generator function. That is, its output is another
#' function that is intended to be used in a call to `update_by()`. This detail is typically hidden from the user by
#' `update_by()`, which calls the generated functions internally. However, it is important to understand this detail
#' for debugging purposes, as the output of a `uby_*` function can otherwise seem unexpected.
#'
#' @param wcol String denoting the column to use for weights. This must be a numeric column.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling weighted average for all non-grouping columns.
#' @param rev_ticks Integer scalar denoting the look-behind window size in number of rows.
#' @param fwd_ticks Integer scalar denoting the look-ahead window size in number of rows. Default is 0.
#' @return UpdateByOp to be used in `update_by()`.
#'
#' @examples
#' print("hello!")
#'
#' @export
uby_rolling_wavg_tick <- function(wcol, cols, rev_ticks, fwd_ticks = 0) {
  verify_string("wcol", wcol, TRUE)
  verify_string("cols", cols, FALSE)
  verify_any_int("rev_ticks", rev_ticks, TRUE)
  verify_any_int("fwd_ticks", fwd_ticks, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_wavg_tick(wcol, cols, rev_ticks, fwd_ticks)))
}

#' @name
#' uby_rolling_wavg_time
#' @title
#' Rolling weighted average with time as the windowing unit
#' @md
#'
#' @description
#' Creates a rolling weighted average UpdateByOp for each column in `cols`, using time as the windowing unit.
#'
#' @details
#' This uses ISO-8601 time strings as the reverse and forward window parameters.
#' Negative values are allowed and can be used to generate completely forward or completely reverse windows.
#' A row containing a null value in the timestamp column belongs to no window and will not be considered
#' in the windows of other rows; its output will be null.
#' Here are some examples of window values:
#' - `rev_time = "PT00:10:00", fwd_time = "PT00:10:00"` - contains rows from 10m before through 10m following
#'     the current row timestamp (inclusive)
#' - `rev_time = "PT00:10:00", fwd_time = "-PT00:05:00"` - contains rows from 10m before through 5m before the
#'     current row timestamp (inclusive), this is a purely backwards looking window
#' - `rev_time = "-PT00:05:00", fwd_time = "PT00:10:00"` - contains rows from 5m following through 10m
#'     following the current row timestamp (inclusive), this is a purely forwards looking window
#'
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `update_by()` caller
#' function. The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like the other Deephaven `uby_*` functions, is a generator function. That is, its output is another
#' function that is intended to be used in a call to `update_by()`. This detail is typically hidden from the user by
#' `update_by()`, which calls the generated functions internally. However, it is important to understand this detail
#' for debugging purposes, as the output of a `uby_*` function can otherwise seem unexpected.
#'
#' @param ts_col String denoting the column to use as the timestamp.
#' @param wcol String denoting the column to use for weights. This must be a numeric column.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling weighted average for all non-grouping columns.
#' @param rev_time ISO-8601-formatted string specifying the look-behind window size.
#' @param fwd_time ISO-8601-formatted string specifying the look-ahead window size. Default is 0 seconds.
#' @return UpdateByOp to be used in `update_by()`.
#'
#' @examples
#' print("hello!")
#'
#' @export
uby_rolling_wavg_time <- function(ts_col, wcol, cols, rev_time, fwd_time = "PT0s") {
  verify_string("ts_col", ts_col, TRUE)
  verify_string("wcol", wcol, TRUE)
  verify_string("cols", cols, FALSE)
  verify_string("rev_time", rev_time, TRUE)
  verify_string("fwd_time", fwd_time, TRUE)
  return(UpdateByOp$new(INTERNAL_rolling_wavg_time(ts_col, wcol, cols, rev_time, fwd_time)))
}
