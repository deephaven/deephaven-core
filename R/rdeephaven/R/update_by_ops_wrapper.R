# An UpdateByOp represents a window-based operator that can be passed to update_by(). This is the return type of
# all of the uby functions. It is a wrapper around an Rcpp_INTERNAL_UpdateByOp, which itself is a wrapper around a
# C++ UpdateByOpWrapper, which is finally a wrapper around a C++ UpdateByOperation. See rdeephaven/src/client.cpp for details.
# Note that UpdateByOps should not be instantiated directly by user code, but rather by provided uby functions.


#' @name
#' UpdateBy
#' @title
#' Deephaven's UpdateBy Operations
#' @md
#' @usage NULL
#' @format NULL
#' @docType class
#'
#' @description
#' Deephaven's `update_by()` table method and suite of `uby` functions enable cumulative and moving calculations
#' on static _and_ streaming tables. Complex operations like cumulative minima and maxima, exponential moving averages,
#' and rolling standard deviations are all possible and effortless to execute. As always in Deephaven,
#' the results of these calculations will continue to update as their parent tables are updated. Additionally, it's easy
#' to group data by one or more columns, enabling complex group-wise calculations with a single line of code.
#'
#' @section
#' Applying UpdateBy operations to a table:
#' The table method `update_by()` is the entry point for UpdateBy operations. It takes two arguments: the first is an
#' [`UpdateByOp`][UpdateByOp] or a list of `UpdateByOp`s denoting the calculations to perform on specific columns of the
#' table. Then, it takes a column name or a list of column names that define the groups on which to perform the calculations.
#' If you don't want grouped calculations, omit this argument.
#'
#' The `update_by()` method itself does not know anything about the columns on which you want to perform calculations.
#' Rather, the desired columns are passed to individual `uby` functions, enabling a massive amount of flexibility.
#'
#' @section
#' `uby` functions:
#' `uby` functions are the workers that actually execute the complex UpdateBy calculations. These functions are
#' _generators_, meaning they return _functions_ that the Deephaven engine knows how to interpret. We call the functions
#' that they return [`UpdateByOp`][UpdateByOp]s. These `UpdateByOp`s are not R-level functions, but Deephaven-specific
#' data types that perform all of the intensive calculations. Here is a list of all `uby` functions available in Deephaven:
#'
#' - [`uby_cum_min()`][uby_cum_min]
#' - [`uby_cum_max()`][uby_cum_max]
#' - [`uby_cum_sum()`][uby_cum_sum]
#' - [`uby_cum_prod()`][uby_cum_prod]
#' - [`uby_forward_fill()`][uby_forward_fill]
#' - [`uby_delta()`][uby_delta]
#' - [`uby_emmin_tick()`][uby_emmin_tick]
#' - [`uby_emmin_time()`][uby_emmin_time]
#' - [`uby_emmax_tick()`][uby_emmax_tick]
#' - [`uby_emmax_time()`][uby_emmax_time]
#' - [`uby_ems_tick()`][uby_ems_tick]
#' - [`uby_ems_time()`][uby_ems_time]
#' - [`uby_ema_tick()`][uby_ema_tick]
#' - [`uby_ema_time()`][uby_ema_time]
#' - [`uby_emstd_tick()`][uby_emstd_tick]
#' - [`uby_emstd_time()`][uby_emstd_time]
#' - [`uby_rolling_count_tick()`][uby_rolling_count_tick]
#' - [`uby_rolling_count_time()`][uby_rolling_count_time]
#' - [`uby_rolling_group_tick()`][uby_rolling_group_tick]
#' - [`uby_rolling_group_time()`][uby_rolling_group_time]
#' - [`uby_rolling_min_tick()`][uby_rolling_min_tick]
#' - [`uby_rolling_min_time()`][uby_rolling_min_time]
#' - [`uby_rolling_max_tick()`][uby_rolling_max_tick]
#' - [`uby_rolling_max_time()`][uby_rolling_max_time]
#' - [`uby_rolling_sum_tick()`][uby_rolling_sum_tick]
#' - [`uby_rolling_sum_time()`][uby_rolling_sum_time]
#' - [`uby_rolling_prod_tick()`][uby_rolling_prod_tick]
#' - [`uby_rolling_prod_time()`][uby_rolling_prod_time]
#' - [`uby_rolling_avg_tick()`][uby_rolling_avg_tick]
#' - [`uby_rolling_avg_time()`][uby_rolling_avg_time]
#' - [`uby_rolling_wavg_tick()`][uby_rolling_wavg_tick]
#' - [`uby_rolling_wavg_time()`][uby_rolling_wavg_time]
#' - [`uby_rolling_std_tick()`][uby_rolling_std_tick]
#' - [`uby_rolling_std_time()`][uby_rolling_std_time]
#'
#' For more details on each aggregation function, click on one of the methods above or see the reference documentation
#' by running `?uby_cum_min`, `?uby_delta`, etc.
#'
#' @examples
#' \dontrun{
#' library(rdeephaven)
#'
#' # connecting to Deephaven server
#' client <- Client$new("localhost:10000", auth_type="psk", auth_token="my_secret_token")
#'
#' # create data frame, push to server, retrieve TableHandle
#' df <- data.frame(
#'   timeCol = seq.POSIXt(as.POSIXct(Sys.Date()), as.POSIXct(Sys.Date() + 0.01), by = "1 sec")[1:500],
#'   boolCol = sample(c(TRUE,FALSE), 500, TRUE),
#'   col1 = sample(10000, size = 500, replace = TRUE),
#'   col2 = sample(10000, size = 500, replace = TRUE),
#'   col3 = 1:500
#' )
#' th <- client$import_table(df)
#'
#' # compute 10-row exponential weighted moving average of col1 and col2, grouped by boolCol
#' th1 <- th$
#'   update_by(uby_ema_tick(decay_ticks=10, cols=c("col1Ema = col1", "col2Ema = col2")), by="boolCol")
#'
#' # compute rolling 10-second weighted average and standard deviation of col1 and col2, weighted by col3
#' th2 <- th$
#'   update_by(
#'     c(uby_rolling_wavg_time(ts_col="timeCol", wcol="col3", cols=c("col1WAvg = col1", "col2WAvg = col2"), rev_time="PT10s"),
#'       uby_rolling_std_time(ts_col="timeCol", cols=c("col1Std = col1", "col2Std = col2"), rev_time="PT10s")))
#'
#' # compute cumulative minimum and maximum of col1 and col2 respectively, and the rolling 20-row sum of col3, grouped by boolCol
#' th3 <- th$
#'   update_by(
#'     c(uby_cum_min(cols="col1"),
#'       uby_cum_max(cols="col2"),
#'       uby_rolling_sum_tick(cols="col3", rev_ticks=20)),
#'     by="boolCol")
#'
#' client$close()
#' }
#'
NULL


#' @name UpdateByOp
#' @title Deephaven UpdateByOps
#' @md
#' @description
#' An `UpdateByOp` is the return type of one of Deephaven's [`uby`][UpdateBy] functions. It is a function that performs
#' the computation specified by the `uby` function. These are intended to be passed directly to `update_by()`,
#' and should never be instantiated directly be user code.
#'
#' If multiple tables have the same schema and the same UpdateBy operations need to be applied to each table, saving
#' these objects directly in a variable may be useful to avoid having to re-create them each time:
#' ```
#' operations <- c(uby_rolling_avg_tick("XAvg = X", "YAvg = Y"),
#'                 uby_rolling_std_tick("XStd = X", "YStd = Y"))
#'
#' result1 <- th1$update_by(operations, by="Group")
#' result2 <- th2$update_by(operations, by="Group")
#' ```
#' In this example, `operations` would be a vector of two `UpdateByOp`s that can be reused in multiple calls to `update_by()`.
#'
#' @usage NULL
#' @format NULL
#' @docType class
#' @export
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
#' This function acts on aggregation groups specified with the `by` parameter of the `update_by()` caller function.
#' The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like other Deephaven `uby` functions, is a generator function. That is, its output is another
#' function called an [`UpdateByOp`][UpdateByOp] intended to be used in a call to `update_by()`. This detail is typically
#' hidden from the user. However, it is important to understand this detail for debugging purposes, as the output of
#' a `uby` function can otherwise seem unexpected.
#'
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the cumulative sum for all non-grouping columns.
#' @return `UpdateByOp` to be used in a call to `update_by()`.
#'
#' @examples
#' \dontrun{
#' library(rdeephaven)
#'
#' # connecting to Deephaven server
#' client <- Client$new("localhost:10000", auth_type="psk", auth_token="my_secret_token")
#'
#' # create data frame, push to server, retrieve TableHandle
#' df <- data.frame(
#'   timeCol = seq.POSIXt(as.POSIXct(Sys.Date()), as.POSIXct(Sys.Date() + 0.01), by = "1 sec")[1:500],
#'   boolCol = sample(c(TRUE,FALSE), 500, TRUE),
#'   col1 = sample(10000, size = 500, replace = TRUE),
#'   col2 = sample(10000, size = 500, replace = TRUE),
#'   col3 = 1:500
#' )
#' th <- client$import_table(df)
#'
#' # compute cumulative sum of col1 and col2
#' th1 <- th$
#'   update_by(uby_cum_sum(c("col1CumSum = col1", "col2CumSum = col2")))
#'
#' # compute cumulative sum of col1 and col2, grouped by boolCol
#' th2 <- th$
#'   update_by(uby_cum_sum(c("col1CumSum = col1", "col2CumSum = col2")), by="boolCol")
#'
#' # compute cumulative sum of col1 and col2, grouped by boolCol and parity of col3
#' th3 <- th$
#'   update("col3Parity = col3 % 2")$
#'   update_by(uby_cum_sum(c("col1CumSum = col1", "col2CumSum = col2")), by=c("boolCol", "col3Parity"))
#'
#' client$close()
#' }
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
#' This function acts on aggregation groups specified with the `by` parameter of the `update_by()` caller function.
#' The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like other Deephaven `uby` functions, is a generator function. That is, its output is another
#' function called an [`UpdateByOp`][UpdateByOp] intended to be used in a call to `update_by()`. This detail is typically
#' hidden from the user. However, it is important to understand this detail for debugging purposes, as the output of
#' a `uby` function can otherwise seem unexpected.
#'
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the cumulative product for all non-grouping columns.
#' @return `UpdateByOp` to be used in a call to `update_by()`.
#'
#' @examples
#' \dontrun{
#' library(rdeephaven)
#'
#' # connecting to Deephaven server
#' client <- Client$new("localhost:10000", auth_type="psk", auth_token="my_secret_token")
#'
#' # create data frame, push to server, retrieve TableHandle
#' df <- data.frame(
#'   timeCol = seq.POSIXt(as.POSIXct(Sys.Date()), as.POSIXct(Sys.Date() + 0.01), by = "1 sec")[1:500],
#'   boolCol = sample(c(TRUE,FALSE), 500, TRUE),
#'   col1 = sample(10000, size = 500, replace = TRUE),
#'   col2 = sample(10000, size = 500, replace = TRUE),
#'   col3 = 1:500
#' )
#' th <- client$import_table(df)
#'
#' # compute cumulative product of col1 and col2
#' th1 <- th$
#'   update_by(uby_cum_prod(c("col1CumProd = col1", "col2CumProd = col2")))
#'
#' # compute cumulative product of col1 and col2, grouped by boolCol
#' th2 <- th$
#'   update_by(uby_cum_prod(c("col1CumProd = col1", "col2CumProd = col2")), by="boolCol")
#'
#' # compute cumulative product of col1 and col2, grouped by boolCol and parity of col3
#' th3 <- th$
#'   update("col3Parity = col3 % 2")$
#'   update_by(uby_cum_prod(c("col1CumProd = col1", "col2CumProd = col2")), by=c("boolCol", "col3Parity"))
#'
#' client$close()
#' }
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
#' This function acts on aggregation groups specified with the `by` parameter of the `update_by()` caller function.
#' The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like other Deephaven `uby` functions, is a generator function. That is, its output is another
#' function called an [`UpdateByOp`][UpdateByOp] intended to be used in a call to `update_by()`. This detail is typically
#' hidden from the user. However, it is important to understand this detail for debugging purposes, as the output of
#' a `uby` function can otherwise seem unexpected.
#'
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the cumulative minimum for all non-grouping columns.
#' @return `UpdateByOp` to be used in a call to `update_by()`.
#'
#' @examples
#' \dontrun{
#' library(rdeephaven)
#'
#' # connecting to Deephaven server
#' client <- Client$new("localhost:10000", auth_type="psk", auth_token="my_secret_token")
#'
#' # create data frame, push to server, retrieve TableHandle
#' df <- data.frame(
#'   timeCol = seq.POSIXt(as.POSIXct(Sys.Date()), as.POSIXct(Sys.Date() + 0.01), by = "1 sec")[1:500],
#'   boolCol = sample(c(TRUE,FALSE), 500, TRUE),
#'   col1 = sample(10000, size = 500, replace = TRUE),
#'   col2 = sample(10000, size = 500, replace = TRUE),
#'   col3 = 1:500
#' )
#' th <- client$import_table(df)
#'
#' # compute cumulative minimum of col1 and col2
#' th1 <- th$
#'   update_by(uby_cum_min(c("col1CumMin = col1", "col2CumMin = col2")))
#'
#' # compute cumulative minimum of col1 and col2, grouped by boolCol
#' th2 <- th$
#'   update_by(uby_cum_min(c("col1CumMin = col1", "col2CumMin = col2")), by="boolCol")
#'
#' # compute cumulative minimum of col1 and col2, grouped by boolCol and parity of col3
#' th3 <- th$
#'   update("col3Parity = col3 % 2")$
#'   update_by(uby_cum_min(c("col1CumMin = col1", "col2CumMin = col2")), by=c("boolCol", "col3Parity"))
#'
#' client$close()
#' }
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
#' This function acts on aggregation groups specified with the `by` parameter of the `update_by()` caller function.
#' The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like other Deephaven `uby` functions, is a generator function. That is, its output is another
#' function called an [`UpdateByOp`][UpdateByOp] intended to be used in a call to `update_by()`. This detail is typically
#' hidden from the user. However, it is important to understand this detail for debugging purposes, as the output of
#' a `uby` function can otherwise seem unexpected.
#'
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the cumulative maximum for all non-grouping columns.
#' @return `UpdateByOp` to be used in a call to `update_by()`.
#'
#' @examples
#' \dontrun{
#' library(rdeephaven)
#'
#' # connecting to Deephaven server
#' client <- Client$new("localhost:10000", auth_type="psk", auth_token="my_secret_token")
#'
#' # create data frame, push to server, retrieve TableHandle
#' df <- data.frame(
#'   timeCol = seq.POSIXt(as.POSIXct(Sys.Date()), as.POSIXct(Sys.Date() + 0.01), by = "1 sec")[1:500],
#'   boolCol = sample(c(TRUE,FALSE), 500, TRUE),
#'   col1 = sample(10000, size = 500, replace = TRUE),
#'   col2 = sample(10000, size = 500, replace = TRUE),
#'   col3 = 1:500
#' )
#' th <- client$import_table(df)
#'
#' # compute cumulative maximum of col1 and col2
#' th1 <- th$
#'   update_by(uby_cum_max(c("col1CumMax = col1", "col2CumMax = col2")))
#'
#' # compute cumulative maximum of col1 and col2, grouped by boolCol
#' th2 <- th$
#'   update_by(uby_cum_max(c("col1CumMax = col1", "col2CumMax = col2")), by="boolCol")
#'
#' # compute cumulative maximum of col1 and col2, grouped by boolCol and parity of col3
#' th3 <- th$
#'   update("col3Parity = col3 % 2")$
#'   update_by(uby_cum_max(c("col1CumMax = col1", "col2CumMax = col2")), by=c("boolCol", "col3Parity"))
#'
#' client$close()
#' }
#'
#' @export
uby_cum_max <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(UpdateByOp$new(INTERNAL_cum_max(cols)))
}

#' @name
#' uby_forward_fill
#' @title
#' Replace null values with the last known non-null value
#' @md
#'
#' @description
#' Creates a forward fill UpdateByOp that replaces null values in `cols` with the last known non-null values.
#' This operation is forward only.
#'
#' @details
#' This function acts on aggregation groups specified with the `by` parameter of the `update_by()` caller function.
#' The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like other Deephaven `uby` functions, is a generator function. That is, its output is another
#' function called an [`UpdateByOp`][UpdateByOp] intended to be used in a call to `update_by()`. This detail is typically
#' hidden from the user. However, it is important to understand this detail for debugging purposes, as the output of
#' a `uby` function can otherwise seem unexpected.
#'
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to perform a forward fill on all non-grouping columns.
#' @return `UpdateByOp` to be used in a call to `update_by()`.
#'
#' @examples
#' \dontrun{
#' library(rdeephaven)
#'
#' # connecting to Deephaven server
#' client <- Client$new("localhost:10000", auth_type="psk", auth_token="my_secret_token")
#'
#' # create data frame, push to server, retrieve TableHandle
#' df <- data.frame(
#'   timeCol = seq.POSIXt(as.POSIXct(Sys.Date()), as.POSIXct(Sys.Date() + 0.01), by = "1 sec")[1:500],
#'   boolCol = sample(c(TRUE,FALSE), 500, TRUE),
#'   col1 = replace(sample(10000, size = 500, replace = TRUE), sample(500, 100), NA),
#'   col2 = replace(sample(10000, size = 500, replace = TRUE), sample(500, 100), NA),
#'   col3 = replace(1:500, sample(500, 100), NA)
#' )
#' th <- client$import_table(df)
#'
#' # forward fill col1 and col2
#' th1 <- th$
#'   update_by(uby_forward_fill(c("col1", "col2")))
#'
#' # forward fill col1 and col2, grouped by boolCol
#' th2 <- th$
#'  update_by(uby_forward_fill(c("col1", "col2")), by="boolCol")
#'
#' # forward fill col3, compute parity of col3, and forward fill col1 and col2, grouped by boolCol and parity of col3
#' th3 <- th$
#'   update_by(uby_forward_fill("col3"))$
#'   update("col3Parity = col3 % 2")$
#'   update_by(uby_forward_fill(c("col1", "col2")), by=c("boolCol", "col3Parity"))
#'
#' client$close()
#' }
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
#' This function acts on aggregation groups specified with the `by` parameter of the `update_by()` caller function.
#' The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like other Deephaven `uby` functions, is a generator function. That is, its output is another
#' function called an [`UpdateByOp`][UpdateByOp] intended to be used in a call to `update_by()`. This detail is typically
#' hidden from the user. However, it is important to understand this detail for debugging purposes, as the output of
#' a `uby` function can otherwise seem unexpected.
#'
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to apply the delta operation to all non-grouping columns.
#' @param delta_control Defines how the delta operation handles null values. Defaults to `'null_dominates'`.
#' @return `UpdateByOp` to be used in a call to `update_by()`.
#'
#' @examples
#' \dontrun{
#' library(rdeephaven)
#'
#' # connecting to Deephaven server
#' client <- Client$new("localhost:10000", auth_type="psk", auth_token="my_secret_token")
#'
#' # create data frame, push to server, retrieve TableHandle
#' df <- data.frame(
#'   timeCol = seq.POSIXt(as.POSIXct(Sys.Date()), as.POSIXct(Sys.Date() + 0.01), by = "1 sec")[1:500],
#'   boolCol = sample(c(TRUE,FALSE), 500, TRUE),
#'   col1 = sample(10000, size = 500, replace = TRUE),
#'   col2 = sample(10000, size = 500, replace = TRUE),
#'   col3 = 1:500
#' )
#' th <- client$import_table(df)
#'
#' # compute consecutive differences of col1 and col2
#' th1 <- th$
#'   update_by(uby_delta(c("col1Delta = col1", "col2Delta = col2")))
#'
#' # compute consecutive differences of col1 and col2, grouped by boolCol
#' th2 <- th$
#'   update_by(uby_delta(c("col1Delta = col1", "col2Delta = col2")), by="boolCol")
#'
#' # compute consecutive differences of col1 and col2, grouped by boolCol and parity of col3
#' th3 <- th$
#'   update("col3Parity = col3 % 2")$
#'   update_by(uby_delta(c("col1Delta = col1", "col2Delta = col2")), by=c("boolCol", "col3Parity"))
#'
#' client$close()
#' }
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
#' This function acts on aggregation groups specified with the `by` parameter of the `update_by()` caller function.
#' The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like other Deephaven `uby` functions, is a generator function. That is, its output is another
#' function called an [`UpdateByOp`][UpdateByOp] intended to be used in a call to `update_by()`. This detail is typically
#' hidden from the user. However, it is important to understand this detail for debugging purposes, as the output of
#' a `uby` function can otherwise seem unexpected.
#'
#' @param decay_ticks Numeric scalar denoting the decay rate in ticks.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the exponential moving average for all non-grouping columns.
#' @param operation_control OperationControl that defines how special cases will behave. See `?op_control` for more information.
#' @return `UpdateByOp` to be used in a call to `update_by()`.
#'
#' @examples
#' \dontrun{
#' library(rdeephaven)
#'
#' # connecting to Deephaven server
#' client <- Client$new("localhost:10000", auth_type="psk", auth_token="my_secret_token")
#'
#' # create data frame, push to server, retrieve TableHandle
#' df <- data.frame(
#'   timeCol = seq.POSIXt(as.POSIXct(Sys.Date()), as.POSIXct(Sys.Date() + 0.01), by = "1 sec")[1:500],
#'   boolCol = sample(c(TRUE,FALSE), 500, TRUE),
#'   col1 = sample(10000, size = 500, replace = TRUE),
#'   col2 = sample(10000, size = 500, replace = TRUE),
#'   col3 = 1:500
#' )
#' th <- client$import_table(df)
#'
#' # compute 10-row exponential moving average of col1 and col2
#' th1 <- th$
#'   update_by(uby_ema_tick(decay_ticks=10, cols=c("col1Ema = col1", "col2Ema = col2")))
#'
#' # compute 5-row exponential moving average of col1 and col2, grouped by boolCol
#' th2 <- th$
#'   update_by(uby_ema_tick(decay_ticks=5, cols=c("col1Ema = col1", "col2Ema = col2")), by="boolCol")
#'
#' # compute 20-row exponential moving average of col1 and col2, grouped by boolCol and parity of col3
#' th3 <- th$
#'   update("col3Parity = col3 % 2")$
#'   update_by(uby_ema_tick(decay_ticks=20, cols=c("col1Ema = col1", "col2Ema = col2")), by=c("boolCol", "col3Parity"))
#'
#' client$close()
#' }
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
#' This function acts on aggregation groups specified with the `by` parameter of the `update_by()` caller function.
#' The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like other Deephaven `uby` functions, is a generator function. That is, its output is another
#' function called an [`UpdateByOp`][UpdateByOp] intended to be used in a call to `update_by()`. This detail is typically
#' hidden from the user. However, it is important to understand this detail for debugging purposes, as the output of
#' a `uby` function can otherwise seem unexpected.
#'
#' @param ts_col String denoting the column to use as the timestamp.
#' @param decay_time ISO-8601-formatted duration string specifying the decay rate.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the exponential moving average for all non-grouping columns.
#' @param operation_control OperationControl that defines how special cases will behave. See `?op_control` for more information.
#' @return `UpdateByOp` to be used in a call to `update_by()`.
#'
#' @examples
#' \dontrun{
#' library(rdeephaven)
#'
#' # connecting to Deephaven server
#' client <- Client$new("localhost:10000", auth_type="psk", auth_token="my_secret_token")
#'
#' # create data frame, push to server, retrieve TableHandle
#' df <- data.frame(
#'   timeCol = seq.POSIXt(as.POSIXct(Sys.Date()), as.POSIXct(Sys.Date() + 0.01), by = "1 sec")[1:500],
#'   boolCol = sample(c(TRUE,FALSE), 500, TRUE),
#'   col1 = sample(10000, size = 500, replace = TRUE),
#'   col2 = sample(10000, size = 500, replace = TRUE),
#'   col3 = 1:500
#' )
#' th <- client$import_table(df)
#'
#' # compute 10-second exponential moving average of col1 and col2
#' th1 <- th$
#'   update_by(uby_ema_time(ts_col="timeCol", decay_time="PT10s", cols=c("col1Ema = col1", "col2Ema = col2")))
#'
#' # compute 5-second exponential moving average of col1 and col2, grouped by boolCol
#' th2 <- th$
#'   update_by(uby_ema_time(ts_col="timeCol", decay_time="PT5s", cols=c("col1Ema = col1", "col2Ema = col2")), by="boolCol")
#'
#' # compute 20-second exponential moving average of col1 and col2, grouped by boolCol and parity of col3
#' th3 <- th$
#'   update("col3Parity = col3 % 2")$
#'   update_by(uby_ema_time(ts_col="timeCol", decay_time="PT20s", cols=c("col1Ema = col1", "col2Ema = col2")), by=c("boolCol", "col3Parity"))
#'
#' client$close()
#' }
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
#' This function acts on aggregation groups specified with the `by` parameter of the `update_by()` caller function.
#' The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like other Deephaven `uby` functions, is a generator function. That is, its output is another
#' function called an [`UpdateByOp`][UpdateByOp] intended to be used in a call to `update_by()`. This detail is typically
#' hidden from the user. However, it is important to understand this detail for debugging purposes, as the output of
#' a `uby` function can otherwise seem unexpected.
#'
#' @param decay_ticks Numeric scalar denoting the decay rate in ticks.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the exponential moving sum for all non-grouping columns.
#' @param operation_control OperationControl that defines how special cases will behave. See `?op_control` for more information.
#' @return `UpdateByOp` to be used in a call to `update_by()`.
#'
#' @examples
#' \dontrun{
#' library(rdeephaven)
#'
#' # connecting to Deephaven server
#' client <- Client$new("localhost:10000", auth_type="psk", auth_token="my_secret_token")
#'
#' # create data frame, push to server, retrieve TableHandle
#' df <- data.frame(
#'   timeCol = seq.POSIXt(as.POSIXct(Sys.Date()), as.POSIXct(Sys.Date() + 0.01), by = "1 sec")[1:500],
#'   boolCol = sample(c(TRUE,FALSE), 500, TRUE),
#'   col1 = sample(10000, size = 500, replace = TRUE),
#'   col2 = sample(10000, size = 500, replace = TRUE),
#'   col3 = 1:500
#' )
#' th <- client$import_table(df)
#'
#' # compute 10-row exponential moving sum of col1 and col2
#' th1 <- th$
#'   update_by(uby_ems_tick(decay_ticks=10, cols=c("col1Ems = col1", "col2Ems = col2")))
#'
#' # compute 5-row exponential moving sum of col1 and col2, grouped by boolCol
#' th2 <- th$
#'   update_by(uby_ems_tick(decay_ticks=5, cols=c("col1Ems = col1", "col2Ems = col2")), by="boolCol")
#'
#' # compute 20-row exponential moving sum of col1 and col2, grouped by boolCol and parity of col3
#' th3 <- th$
#'   update("col3Parity = col3 % 2")$
#'   update_by(uby_ems_tick(decay_ticks=20, cols=c("col1Ems = col1", "col2Ems = col2")), by=c("boolCol", "col3Parity"))
#'
#' client$close()
#' }
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
#' This function acts on aggregation groups specified with the `by` parameter of the `update_by()` caller function.
#' The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like other Deephaven `uby` functions, is a generator function. That is, its output is another
#' function called an [`UpdateByOp`][UpdateByOp] intended to be used in a call to `update_by()`. This detail is typically
#' hidden from the user. However, it is important to understand this detail for debugging purposes, as the output of
#' a `uby` function can otherwise seem unexpected.
#'
#' @param decay_time ISO-8601-formatted duration string specifying the decay rate.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the exponential moving sum for all non-grouping columns.
#' @param operation_control OperationControl that defines how special cases will behave. See `?op_control` for more information.
#' @return `UpdateByOp` to be used in a call to `update_by()`.
#'
#' @examples
#' \dontrun{
#' library(rdeephaven)
#'
#' # connecting to Deephaven server
#' client <- Client$new("localhost:10000", auth_type="psk", auth_token="my_secret_token")
#'
#' # create data frame, push to server, retrieve TableHandle
#' df <- data.frame(
#'   timeCol = seq.POSIXt(as.POSIXct(Sys.Date()), as.POSIXct(Sys.Date() + 0.01), by = "1 sec")[1:500],
#'   boolCol = sample(c(TRUE,FALSE), 500, TRUE),
#'   col1 = sample(10000, size = 500, replace = TRUE),
#'   col2 = sample(10000, size = 500, replace = TRUE),
#'   col3 = 1:500
#' )
#' th <- client$import_table(df)
#'
#' # compute 10-second exponential moving sum of col1 and col2
#' th1 <- th$
#'   update_by(uby_ems_time(ts_col="timeCol", decay_time="PT10s", cols=c("col1Ems = col1", "col2Ems = col2")))
#'
#' # compute 5-second exponential moving sum of col1 and col2, grouped by boolCol
#' th2 <- th$
#'   update_by(uby_ems_time(ts_col="timeCol", decay_time="PT5s", cols=c("col1Ems = col1", "col2Ems = col2")), by="boolCol")
#'
#' # compute 20-second exponential moving sum of col1 and col2, grouped by boolCol and parity of col3
#' th3 <- th$
#'   update("col3Parity = col3 % 2")$
#'   update_by(uby_ems_time(ts_col="timeCol", decay_time="PT20s", cols=c("col1Ems = col1", "col2Ems = col2")), by=c("boolCol", "col3Parity"))
#'
#' client$close()
#' }
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
#' This function acts on aggregation groups specified with the `by` parameter of the `update_by()` caller function.
#' The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like other Deephaven `uby` functions, is a generator function. That is, its output is another
#' function called an [`UpdateByOp`][UpdateByOp] intended to be used in a call to `update_by()`. This detail is typically
#' hidden from the user. However, it is important to understand this detail for debugging purposes, as the output of
#' a `uby` function can otherwise seem unexpected.
#'
#' @param decay_ticks Numeric scalar denoting the decay rate in ticks.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the exponential moving minimum for all non-grouping columns.
#' @param operation_control OperationControl that defines how special cases will behave. See `?op_control` for more information.
#' @return `UpdateByOp` to be used in a call to `update_by()`.
#'
#' @examples
#' \dontrun{
#' library(rdeephaven)
#'
#' # connecting to Deephaven server
#' client <- Client$new("localhost:10000", auth_type="psk", auth_token="my_secret_token")
#'
#' # create data frame, push to server, retrieve TableHandle
#' df <- data.frame(
#'   timeCol = seq.POSIXt(as.POSIXct(Sys.Date()), as.POSIXct(Sys.Date() + 0.01), by = "1 sec")[1:500],
#'   boolCol = sample(c(TRUE,FALSE), 500, TRUE),
#'   col1 = sample(10000, size = 500, replace = TRUE),
#'   col2 = sample(10000, size = 500, replace = TRUE),
#'   col3 = 1:500
#' )
#' th <- client$import_table(df)
#'
#' # compute 10-row exponential moving minimum of col1 and col2
#' th1 <- th$
#'   update_by(uby_emmin_tick(decay_ticks=10, cols=c("col1Emmin = col1", "col2Emmin = col2")))
#'
#' # compute 5-row exponential moving minimum of col1 and col2, grouped by boolCol
#' th2 <- th$
#'   update_by(uby_emmin_tick(decay_ticks=5, cols=c("col1Emmin = col1", "col2Emmin = col2")), by="boolCol")
#'
#' # compute 20-row exponential moving minimum of col1 and col2, grouped by boolCol and parity of col3
#' th3 <- th$
#'   update("col3Parity = col3 % 2")$
#'   update_by(uby_emmin_tick(decay_ticks=20, cols=c("col1Emmin = col1", "col2Emmin = col2")), by=c("boolCol", "col3Parity"))
#'
#' client$close()
#' }
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
#' This function acts on aggregation groups specified with the `by` parameter of the `update_by()` caller function.
#' The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like other Deephaven `uby` functions, is a generator function. That is, its output is another
#' function called an [`UpdateByOp`][UpdateByOp] intended to be used in a call to `update_by()`. This detail is typically
#' hidden from the user. However, it is important to understand this detail for debugging purposes, as the output of
#' a `uby` function can otherwise seem unexpected.
#'
#' @param decay_time ISO-8601-formatted duration string specifying the decay rate.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the exponential moving minimum for all non-grouping columns.
#' @param operation_control OperationControl that defines how special cases will behave. See `?op_control` for more information.
#' @return `UpdateByOp` to be used in a call to `update_by()`.
#'
#' @examples
#' \dontrun{
#' library(rdeephaven)
#'
#' # connecting to Deephaven server
#' client <- Client$new("localhost:10000", auth_type="psk", auth_token="my_secret_token")
#'
#' # create data frame, push to server, retrieve TableHandle
#' df <- data.frame(
#'   timeCol = seq.POSIXt(as.POSIXct(Sys.Date()), as.POSIXct(Sys.Date() + 0.01), by = "1 sec")[1:500],
#'   boolCol = sample(c(TRUE,FALSE), 500, TRUE),
#'   col1 = sample(10000, size = 500, replace = TRUE),
#'   col2 = sample(10000, size = 500, replace = TRUE),
#'   col3 = 1:500
#' )
#' th <- client$import_table(df)
#'
#' # compute 10-second exponential moving minimum of col1 and col2
#' th1 <- th$
#'   update_by(uby_emmin_time(ts_col="timeCol", decay_time="PT10s", cols=c("col1Emmin = col1", "col2Emmin = col2")))
#'
#' # compute 5-second exponential moving minimum of col1 and col2, grouped by boolCol
#' th2 <- th$
#'   update_by(uby_emmin_time(ts_col="timeCol", decay_time="PT5s", cols=c("col1Emmin = col1", "col2Emmin = col2")), by="boolCol")
#'
#' # compute 20-second exponential moving minimum of col1 and col2, grouped by boolCol and parity of col3
#' th3 <- th$
#'   update("col3Parity = col3 % 2")$
#'   update_by(uby_emmin_time(ts_col="timeCol", decay_time="PT20s", cols=c("col1Emmin = col1", "col2Emmin = col2")), by=c("boolCol", "col3Parity"))
#'
#' client$close()
#' }
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
#' This function acts on aggregation groups specified with the `by` parameter of the `update_by()` caller function.
#' The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like other Deephaven `uby` functions, is a generator function. That is, its output is another
#' function called an [`UpdateByOp`][UpdateByOp] intended to be used in a call to `update_by()`. This detail is typically
#' hidden from the user. However, it is important to understand this detail for debugging purposes, as the output of
#' a `uby` function can otherwise seem unexpected.
#'
#' @param decay_ticks Numeric scalar denoting the decay rate in ticks.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the exponential moving maximum for all non-grouping columns.
#' @param operation_control OperationControl that defines how special cases will behave. See `?op_control` for more information.
#' @return `UpdateByOp` to be used in a call to `update_by()`.
#'
#' @examples
#' \dontrun{
#' library(rdeephaven)
#'
#' # connecting to Deephaven server
#' client <- Client$new("localhost:10000", auth_type="psk", auth_token="my_secret_token")
#'
#' # create data frame, push to server, retrieve TableHandle
#' df <- data.frame(
#'   timeCol = seq.POSIXt(as.POSIXct(Sys.Date()), as.POSIXct(Sys.Date() + 0.01), by = "1 sec")[1:500],
#'   boolCol = sample(c(TRUE,FALSE), 500, TRUE),
#'   col1 = sample(10000, size = 500, replace = TRUE),
#'   col2 = sample(10000, size = 500, replace = TRUE),
#'   col3 = 1:500
#' )
#' th <- client$import_table(df)
#'
#' # compute 10-row exponential moving maximum of col1 and col2
#' th1 <- th$
#'   update_by(uby_emmax_tick(decay_ticks=10, cols=c("col1Emmax = col1", "col2Emmax = col2")))
#'
#' # compute 5-row exponential moving maximum of col1 and col2, grouped by boolCol
#' th2 <- th$
#'   update_by(uby_emmax_tick(decay_ticks=5, cols=c("col1Emmax = col1", "col2Emmax = col2")), by="boolCol")
#'
#' # compute 20-row exponential moving maximum of col1 and col2, grouped by boolCol and parity of col3
#' th3 <- th$
#'   update("col3Parity = col3 % 2")$
#'   update_by(uby_emmax_tick(decay_ticks=20, cols=c("col1Emmax = col1", "col2Emmax = col2")), by=c("boolCol", "col3Parity"))
#'
#' client$close()
#' }
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
#' This function acts on aggregation groups specified with the `by` parameter of the `update_by()` caller function.
#' The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like other Deephaven `uby` functions, is a generator function. That is, its output is another
#' function called an [`UpdateByOp`][UpdateByOp] intended to be used in a call to `update_by()`. This detail is typically
#' hidden from the user. However, it is important to understand this detail for debugging purposes, as the output of
#' a `uby` function can otherwise seem unexpected.
#'
#' @param decay_time ISO-8601-formatted duration string specifying the decay rate.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the exponential moving maximum for all non-grouping columns.
#' @param operation_control OperationControl that defines how special cases will behave. See `?op_control` for more information.
#' @return `UpdateByOp` to be used in a call to `update_by()`.
#'
#' @examples
#' \dontrun{
#' library(rdeephaven)
#'
#' # connecting to Deephaven server
#' client <- Client$new("localhost:10000", auth_type="psk", auth_token="my_secret_token")
#'
#' # create data frame, push to server, retrieve TableHandle
#' df <- data.frame(
#'   timeCol = seq.POSIXt(as.POSIXct(Sys.Date()), as.POSIXct(Sys.Date() + 0.01), by = "1 sec")[1:500],
#'   boolCol = sample(c(TRUE,FALSE), 500, TRUE),
#'   col1 = sample(10000, size = 500, replace = TRUE),
#'   col2 = sample(10000, size = 500, replace = TRUE),
#'   col3 = 1:500
#' )
#' th <- client$import_table(df)
#'
#' # compute 10-second exponential moving maximum of col1 and col2
#' th1 <- th$
#'   update_by(uby_emmax_time(ts_col="timeCol", decay_time="PT10s", cols=c("col1Emmax = col1", "col2Emmax = col2")))
#'
#' # compute 5-second exponential moving maximum of col1 and col2, grouped by boolCol
#' th2 <- th$
#'   update_by(uby_emmax_time(ts_col="timeCol", decay_time="PT5s", cols=c("col1Emmax = col1", "col2Emmax = col2")), by="boolCol")
#'
#' # compute 20-second exponential moving maximum of col1 and col2, grouped by boolCol and parity of col3
#' th3 <- th$
#'   update("col3Parity = col3 % 2")$
#'   update_by(uby_emmax_time(ts_col="timeCol", decay_time="PT20s", cols=c("col1Emmax = col1", "col2Emmax = col2")), by=c("boolCol", "col3Parity"))
#'
#' client$close()
#' }
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
#' This function acts on aggregation groups specified with the `by` parameter of the `update_by()` caller function.
#' The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like other Deephaven `uby` functions, is a generator function. That is, its output is another
#' function called an [`UpdateByOp`][UpdateByOp] intended to be used in a call to `update_by()`. This detail is typically
#' hidden from the user. However, it is important to understand this detail for debugging purposes, as the output of
#' a `uby` function can otherwise seem unexpected.
#'
#' @param decay_ticks Numeric scalar denoting the decay rate in ticks.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the exponential moving standard deviation for all non-grouping columns.
#' @param operation_control OperationControl that defines how special cases will behave. See `?op_control` for more information.
#' @return `UpdateByOp` to be used in a call to `update_by()`.
#'
#' @examples
#' \dontrun{
#' library(rdeephaven)
#'
#' # connecting to Deephaven server
#' client <- Client$new("localhost:10000", auth_type="psk", auth_token="my_secret_token")
#'
#' # create data frame, push to server, retrieve TableHandle
#' df <- data.frame(
#'   timeCol = seq.POSIXt(as.POSIXct(Sys.Date()), as.POSIXct(Sys.Date() + 0.01), by = "1 sec")[1:500],
#'   boolCol = sample(c(TRUE,FALSE), 500, TRUE),
#'   col1 = sample(10000, size = 500, replace = TRUE),
#'   col2 = sample(10000, size = 500, replace = TRUE),
#'   col3 = 1:500
#' )
#' th <- client$import_table(df)
#'
#' # compute 10-row exponential moving standard deviation of col1 and col2
#' th1 <- th$
#'   update_by(uby_emstd_tick(decay_ticks=10, cols=c("col1Emstd = col1", "col2Emstd = col2")))
#'
#' # compute 5-row exponential moving standard deviation of col1 and col2, grouped by boolCol
#' th2 <- th$
#'   update_by(uby_emstd_tick(decay_ticks=5, cols=c("col1Emstd = col1", "col2Emstd = col2")), by="boolCol")
#'
#' # compute 20-row exponential moving standard deviation of col1 and col2, grouped by boolCol and parity of col3
#' th3 <- th$
#'   update("col3Parity = col3 % 2")$
#'   update_by(uby_emstd_tick(decay_ticks=20, cols=c("col1Emstd = col1", "col2Emstd = col2")), by=c("boolCol", "col3Parity"))
#'
#' client$close()
#' }
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
#' This function acts on aggregation groups specified with the `by` parameter of the `update_by()` caller function.
#' The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like other Deephaven `uby` functions, is a generator function. That is, its output is another
#' function called an [`UpdateByOp`][UpdateByOp] intended to be used in a call to `update_by()`. This detail is typically
#' hidden from the user. However, it is important to understand this detail for debugging purposes, as the output of
#' a `uby` function can otherwise seem unexpected.
#'
#' @param decay_time ISO-8601-formatted duration string specifying the decay rate.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the exponential moving standard deviation for all non-grouping columns.
#' @param operation_control OperationControl that defines how special cases will behave. See `?op_control` for more information.
#' @return `UpdateByOp` to be used in a call to `update_by()`.
#'
#' @examples
#' \dontrun{
#' library(rdeephaven)
#'
#' # connecting to Deephaven server
#' client <- Client$new("localhost:10000", auth_type="psk", auth_token="my_secret_token")
#'
#' # create data frame, push to server, retrieve TableHandle
#' df <- data.frame(
#'   timeCol = seq.POSIXt(as.POSIXct(Sys.Date()), as.POSIXct(Sys.Date() + 0.01), by = "1 sec")[1:500],
#'   boolCol = sample(c(TRUE,FALSE), 500, TRUE),
#'   col1 = sample(10000, size = 500, replace = TRUE),
#'   col2 = sample(10000, size = 500, replace = TRUE),
#'   col3 = 1:500
#' )
#' th <- client$import_table(df)
#'
#' # compute 10-second exponential moving standard deviation of col1 and col2
#' th1 <- th$
#'   update_by(uby_emstd_time(ts_col="timeCol", decay_time="PT10s", cols=c("col1Emstd = col1", "col2Emstd = col2")))
#'
#' # compute 5-second exponential moving standard deviation of col1 and col2, grouped by boolCol
#' th2 <- th$
#'   update_by(uby_emstd_time(ts_col="timeCol", decay_time="PT5s", cols=c("col1Emstd = col1", "col2Emstd = col2")), by="boolCol")
#'
#' # compute 20-second exponential moving standard deviation of col1 and col2, grouped by boolCol and parity of col3
#' th3 <- th$
#'   update("col3Parity = col3 % 2")$
#'   update_by(uby_emstd_time(ts_col="timeCol", decay_time="PT20s", cols=c("col1Emstd = col1", "col2Emstd = col2")), by=c("boolCol", "col3Parity"))
#'
#' client$close()
#' }
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
#' This function acts on aggregation groups specified with the `by` parameter of the `update_by()` caller function.
#' The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like other Deephaven `uby` functions, is a generator function. That is, its output is another
#' function called an [`UpdateByOp`][UpdateByOp] intended to be used in a call to `update_by()`. This detail is typically
#' hidden from the user. However, it is important to understand this detail for debugging purposes, as the output of
#' a `uby` function can otherwise seem unexpected.
#'
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling sum for all non-grouping columns.
#' @param rev_ticks Integer scalar denoting the look-behind window size in number of rows.
#' @param fwd_ticks Integer scalar denoting the look-ahead window size in number of rows. Default is 0.
#' @return `UpdateByOp` to be used in a call to `update_by()`.
#'
#' @examples
#' \dontrun{
#' library(rdeephaven)
#'
#' # connecting to Deephaven server
#' client <- Client$new("localhost:10000", auth_type="psk", auth_token="my_secret_token")
#'
#' # create data frame, push to server, retrieve TableHandle
#' df <- data.frame(
#'   timeCol = seq.POSIXt(as.POSIXct(Sys.Date()), as.POSIXct(Sys.Date() + 0.01), by = "1 sec")[1:500],
#'   boolCol = sample(c(TRUE,FALSE), 500, TRUE),
#'   col1 = sample(10000, size = 500, replace = TRUE),
#'   col2 = sample(10000, size = 500, replace = TRUE),
#'   col3 = 1:500
#' )
#' th <- client$import_table(df)
#'
#' # compute rolling sum of col1 and col2, using the previous 5 rows and current row
#' th1 <- th$
#'   update_by(uby_rolling_sum_tick(cols=c("col1RollSum = col1", "col2RollSum = col2"), rev_ticks=6))
#'
#' # compute rolling sum of col1 and col2, grouped by boolCol, using previous 5 rows, current row, and following 5 rows
#' th2 <- th$
#'   update_by(uby_rolling_sum_tick(cols=c("col1RollSum = col1", "col2RollSum = col2"), rev_ticks=6, fwd_ticks=5)), by="boolCol")
#'
#' # compute rolling sum of col1 and col2, grouped by boolCol and parity of col3, using current row and following 10 rows
#' th3 <- th$
#'   update("col3Parity = col3 % 2")$
#'   update_by(uby_rolling_sum_tick(cols=c("col1RollSum = col1", "col2RollSum = col2"), rev_ticks=1, fwd_ticks=10)), by=c("boolCol", "col3Parity"))
#'
#' client$close()
#' }
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
#' This uses [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) time strings as the reverse and forward window parameters.
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
#' This function acts on aggregation groups specified with the `by` parameter of the `update_by()` caller function.
#' The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like other Deephaven `uby` functions, is a generator function. That is, its output is another
#' function called an [`UpdateByOp`][UpdateByOp] intended to be used in a call to `update_by()`. This detail is typically
#' hidden from the user. However, it is important to understand this detail for debugging purposes, as the output of
#' a `uby` function can otherwise seem unexpected.
#'
#' @param ts_col String denoting the column to use as the timestamp.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling sum for all non-grouping columns.
#' @param rev_time ISO-8601-formatted string specifying the look-behind window size.
#' @param fwd_time ISO-8601-formatted string specifying the look-ahead window size. Default is 0 seconds.
#' @return `UpdateByOp` to be used in a call to `update_by()`.
#'
#' @examples
#' \dontrun{
#' library(rdeephaven)
#'
#' # connecting to Deephaven server
#' client <- Client$new("localhost:10000", auth_type="psk", auth_token="my_secret_token")
#'
#' # create data frame, push to server, retrieve TableHandle
#' df <- data.frame(
#'   timeCol = seq.POSIXt(as.POSIXct(Sys.Date()), as.POSIXct(Sys.Date() + 0.01), by = "1 sec")[1:500],
#'   boolCol = sample(c(TRUE,FALSE), 500, TRUE),
#'   col1 = sample(10000, size = 500, replace = TRUE),
#'   col2 = sample(10000, size = 500, replace = TRUE),
#'   col3 = 1:500
#' )
#' th <- client$import_table(df)
#'
#' # compute rolling sum of col1 and col2, using the previous 5 seconds
#' th1 <- th$
#'   update_by(uby_rolling_sum_time(ts_col="timeCol", cols=c("col1RollSum = col1", "col2RollSum = col2"), rev_time="PT5s"))
#'
#' # compute rolling sum of col1 and col2, grouped by boolCol, using previous 5 seconds, and following 5 seconds
#' th2 <- th$
#'   update_by(uby_rolling_sum_time(ts_col="timeCol", cols=c("col1RollSum = col1", "col2RollSum = col2"), rev_time="PT5s", fwd_ticks="PT5s")), by="boolCol")
#'
#' # compute rolling sum of col1 and col2, grouped by boolCol and parity of col3, using following 10 seconds
#' th3 <- th$
#'   update("col3Parity = col3 % 2")$
#'   update_by(uby_rolling_sum_time(ts_col="timeCol", cols=c("col1RollSum = col1", "col2RollSum = col2"), rev_time="PT0s", fwd_time="PT10s")), by=c("boolCol", "col3Parity"))
#'
#' client$close()
#' }
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
#' This function acts on aggregation groups specified with the `by` parameter of the `update_by()` caller function.
#' The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like other Deephaven `uby` functions, is a generator function. That is, its output is another
#' function called an [`UpdateByOp`][UpdateByOp] intended to be used in a call to `update_by()`. This detail is typically
#' hidden from the user. However, it is important to understand this detail for debugging purposes, as the output of
#' a `uby` function can otherwise seem unexpected.
#'
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling group for all non-grouping columns.
#' @param rev_ticks Integer scalar denoting the look-behind window size in number of rows.
#' @param fwd_ticks Integer scalar denoting the look-ahead window size in number of rows. Default is 0.
#' @return `UpdateByOp` to be used in a call to `update_by()`.
#'
#' @examples
#' \dontrun{
#' library(rdeephaven)
#'
#' # connecting to Deephaven server
#' client <- Client$new("localhost:10000", auth_type="psk", auth_token="my_secret_token")
#'
#' # create data frame, push to server, retrieve TableHandle
#' df <- data.frame(
#'   timeCol = seq.POSIXt(as.POSIXct(Sys.Date()), as.POSIXct(Sys.Date() + 0.01), by = "1 sec")[1:500],
#'   boolCol = sample(c(TRUE,FALSE), 500, TRUE),
#'   col1 = sample(10000, size = 500, replace = TRUE),
#'   col2 = sample(10000, size = 500, replace = TRUE),
#'   col3 = 1:500
#' )
#' th <- client$import_table(df)
#'
#' # compute rolling group of col1 and col2, grouped by boolCol, using previous 5 rows, current row, and following 5 rows
#' th1 <- th$
#'   update_by(uby_rolling_group_tick(cols=c("col1RollGroup = col1", "col2RollGroup = col2"), rev_ticks=6, fwd_ticks=5)), by="boolCol")
#'
#' # compute rolling group of col1 and col2, grouped by boolCol and parity of col3, using current row and following 10 rows
#' th2 <- th$
#'   update("col3Parity = col3 % 2")$
#'   update_by(uby_rolling_group_tick(cols=c("col1RollGroup = col1", "col2RollGroup = col2"), rev_ticks=1, fwd_ticks=10)), by=c("boolCol", "col3Parity"))
#'
#' client$close()
#' }
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
#' This uses [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) time strings as the reverse and forward window parameters.
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
#' This function acts on aggregation groups specified with the `by` parameter of the `update_by()` caller function.
#' The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like other Deephaven `uby` functions, is a generator function. That is, its output is another
#' function called an [`UpdateByOp`][UpdateByOp] intended to be used in a call to `update_by()`. This detail is typically
#' hidden from the user. However, it is important to understand this detail for debugging purposes, as the output of
#' a `uby` function can otherwise seem unexpected.
#'
#' @param ts_col String denoting the column to use as the timestamp.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling group for all non-grouping columns.
#' @param rev_time ISO-8601-formatted string specifying the look-behind window size.
#' @param fwd_time ISO-8601-formatted string specifying the look-ahead window size. Default is 0 seconds.
#' @return `UpdateByOp` to be used in a call to `update_by()`.
#'
#' @examples
#' \dontrun{
#' library(rdeephaven)
#'
#' # connecting to Deephaven server
#' client <- Client$new("localhost:10000", auth_type="psk", auth_token="my_secret_token")
#'
#' # create data frame, push to server, retrieve TableHandle
#' df <- data.frame(
#'   timeCol = seq.POSIXt(as.POSIXct(Sys.Date()), as.POSIXct(Sys.Date() + 0.01), by = "1 sec")[1:500],
#'   boolCol = sample(c(TRUE,FALSE), 500, TRUE),
#'   col1 = sample(10000, size = 500, replace = TRUE),
#'   col2 = sample(10000, size = 500, replace = TRUE),
#'   col3 = 1:500
#' )
#' th <- client$import_table(df)
#'
#' # compute rolling group of col1 and col2, grouped by boolCol, using previous 5 seconds, and following 5 seconds
#' th1 <- th$
#'   update_by(uby_rolling_group_time(ts_col="timeCol", cols=c("col1RollGroup = col1", "col2RollGroup = col2"), rev_time="PT5s", fwd_ticks="PT5s")), by="boolCol")
#'
#' # compute rolling group of col1 and col2, grouped by boolCol and parity of col3, using following 10 seconds
#' th2 <- th$
#'   update("col3Parity = col3 % 2")$
#'   update_by(uby_rolling_group_time(ts_col="timeCol", cols=c("col1RollGroup = col1", "col2RollGroup = col2"), rev_time="PT0s", fwd_time="PT10s")), by=c("boolCol", "col3Parity"))
#'
#' client$close()
#' }
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
#' This function acts on aggregation groups specified with the `by` parameter of the `update_by()` caller function.
#' The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like other Deephaven `uby` functions, is a generator function. That is, its output is another
#' function called an [`UpdateByOp`][UpdateByOp] intended to be used in a call to `update_by()`. This detail is typically
#' hidden from the user. However, it is important to understand this detail for debugging purposes, as the output of
#' a `uby` function can otherwise seem unexpected.
#'
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling average for all non-grouping columns.
#' @param rev_ticks Integer scalar denoting the look-behind window size in number of rows.
#' @param fwd_ticks Integer scalar denoting the look-ahead window size in number of rows. Default is 0.
#' @return `UpdateByOp` to be used in a call to `update_by()`.
#'
#' @examples
#' \dontrun{
#' library(rdeephaven)
#'
#' # connecting to Deephaven server
#' client <- Client$new("localhost:10000", auth_type="psk", auth_token="my_secret_token")
#'
#' # create data frame, push to server, retrieve TableHandle
#' df <- data.frame(
#'   timeCol = seq.POSIXt(as.POSIXct(Sys.Date()), as.POSIXct(Sys.Date() + 0.01), by = "1 sec")[1:500],
#'   boolCol = sample(c(TRUE,FALSE), 500, TRUE),
#'   col1 = sample(10000, size = 500, replace = TRUE),
#'   col2 = sample(10000, size = 500, replace = TRUE),
#'   col3 = 1:500
#' )
#' th <- client$import_table(df)
#'
#' # compute rolling average of col1 and col2, using the previous 5 rows and current row
#' th1 <- th$
#'   update_by(uby_rolling_avg_tick(cols=c("col1RollAvg = col1", "col2RollAvg = col2"), rev_ticks=6))
#'
#' # compute rolling average of col1 and col2, grouped by boolCol, using previous 5 rows, current row, and following 5 rows
#' th2 <- th$
#'   update_by(uby_rolling_avg_tick(cols=c("col1RollAvg = col1", "col2RollAvg = col2"), rev_ticks=6, fwd_ticks=5)), by="boolCol")
#'
#' # compute rolling average of col1 and col2, grouped by boolCol and parity of col3, using current row and following 10 rows
#' th3 <- th$
#'   update("col3Parity = col3 % 2")$
#'   update_by(uby_rolling_avg_tick(cols=c("col1RollAvg = col1", "col2RollAvg = col2"), rev_ticks=1, fwd_ticks=10)), by=c("boolCol", "col3Parity"))
#'
#' client$close()
#' }
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
#' This uses [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) time strings as the reverse and forward window parameters.
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
#' This function acts on aggregation groups specified with the `by` parameter of the `update_by()` caller function.
#' The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like other Deephaven `uby` functions, is a generator function. That is, its output is another
#' function called an [`UpdateByOp`][UpdateByOp] intended to be used in a call to `update_by()`. This detail is typically
#' hidden from the user. However, it is important to understand this detail for debugging purposes, as the output of
#' a `uby` function can otherwise seem unexpected.
#'
#' @param ts_col String denoting the column to use as the timestamp.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling average for all non-grouping columns.
#' @param rev_time ISO-8601-formatted string specifying the look-behind window size.
#' @param fwd_time ISO-8601-formatted string specifying the look-ahead window size. Default is 0 seconds.
#' @return `UpdateByOp` to be used in a call to `update_by()`.
#'
#' @examples
#' \dontrun{
#' library(rdeephaven)
#'
#' # connecting to Deephaven server
#' client <- Client$new("localhost:10000", auth_type="psk", auth_token="my_secret_token")
#'
#' # create data frame, push to server, retrieve TableHandle
#' df <- data.frame(
#'   timeCol = seq.POSIXt(as.POSIXct(Sys.Date()), as.POSIXct(Sys.Date() + 0.01), by = "1 sec")[1:500],
#'   boolCol = sample(c(TRUE,FALSE), 500, TRUE),
#'   col1 = sample(10000, size = 500, replace = TRUE),
#'   col2 = sample(10000, size = 500, replace = TRUE),
#'   col3 = 1:500
#' )
#' th <- client$import_table(df)
#'
#' # compute rolling average of col1 and col2, using the previous 5 seconds
#' th1 <- th$
#'   update_by(uby_rolling_avg_time(ts_col="timeCol", cols=c("col1RollAvg = col1", "col2RollAvg = col2"), rev_time="PT5s"))
#'
#' # compute rolling average of col1 and col2, grouped by boolCol, using previous 5 seconds, and following 5 seconds
#' th2 <- th$
#'   update_by(uby_rolling_avg_time(ts_col="timeCol", cols=c("col1RollAvg = col1", "col2RollAvg = col2"), rev_time="PT5s", fwd_ticks="PT5s")), by="boolCol")
#'
#' # compute rolling average of col1 and col2, grouped by boolCol and parity of col3, using following 10 seconds
#' th3 <- th$
#'   update("col3Parity = col3 % 2")$
#'   update_by(uby_rolling_avg_time(ts_col="timeCol", cols=c("col1RollAvg = col1", "col2RollAvg = col2"), rev_time="PT0s", fwd_time="PT10s")), by=c("boolCol", "col3Parity"))
#'
#' client$close()
#' }
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
#' This function acts on aggregation groups specified with the `by` parameter of the `update_by()` caller function.
#' The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like other Deephaven `uby` functions, is a generator function. That is, its output is another
#' function called an [`UpdateByOp`][UpdateByOp] intended to be used in a call to `update_by()`. This detail is typically
#' hidden from the user. However, it is important to understand this detail for debugging purposes, as the output of
#' a `uby` function can otherwise seem unexpected.
#'
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling minimum for all non-grouping columns.
#' @param rev_ticks Integer scalar denoting the look-behind window size in number of rows.
#' @param fwd_ticks Integer scalar denoting the look-ahead window size in number of rows. Default is 0.
#' @return `UpdateByOp` to be used in a call to `update_by()`.
#'
#' @examples
#' \dontrun{
#' library(rdeephaven)
#'
#' # connecting to Deephaven server
#' client <- Client$new("localhost:10000", auth_type="psk", auth_token="my_secret_token")
#'
#' # create data frame, push to server, retrieve TableHandle
#' df <- data.frame(
#'   timeCol = seq.POSIXt(as.POSIXct(Sys.Date()), as.POSIXct(Sys.Date() + 0.01), by = "1 sec")[1:500],
#'   boolCol = sample(c(TRUE,FALSE), 500, TRUE),
#'   col1 = sample(10000, size = 500, replace = TRUE),
#'   col2 = sample(10000, size = 500, replace = TRUE),
#'   col3 = 1:500
#' )
#' th <- client$import_table(df)
#'
#' # compute rolling minimum of col1 and col2, using the previous 5 rows and current row
#' th1 <- th$
#'   update_by(uby_rolling_min_tick(cols=c("col1RollMin = col1", "col2RollMin = col2"), rev_ticks=6))
#'
#' # compute rolling minimum of col1 and col2, grouped by boolCol, using previous 5 rows, current row, and following 5 rows
#' th2 <- th$
#'   update_by(uby_rolling_min_tick(cols=c("col1RollMin = col1", "col2RollMin = col2"), rev_ticks=6, fwd_ticks=5)), by="boolCol")
#'
#' # compute rolling minimum of col1 and col2, grouped by boolCol and parity of col3, using current row and following 10 rows
#' th3 <- th$
#'   update("col3Parity = col3 % 2")$
#'   update_by(uby_rolling_min_tick(cols=c("col1RollMin = col1", "col2RollMin = col2"), rev_ticks=1, fwd_ticks=10)), by=c("boolCol", "col3Parity"))
#'
#' client$close()
#' }
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
#' This uses [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) time strings as the reverse and forward window parameters.
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
#' This function acts on aggregation groups specified with the `by` parameter of the `update_by()` caller function.
#' The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like other Deephaven `uby` functions, is a generator function. That is, its output is another
#' function called an [`UpdateByOp`][UpdateByOp] intended to be used in a call to `update_by()`. This detail is typically
#' hidden from the user. However, it is important to understand this detail for debugging purposes, as the output of
#' a `uby` function can otherwise seem unexpected.
#'
#' @param ts_col String denoting the column to use as the timestamp.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling minimum for all non-grouping columns.
#' @param rev_time ISO-8601-formatted string specifying the look-behind window size.
#' @param fwd_time ISO-8601-formatted string specifying the look-ahead window size. Default is 0 seconds.
#' @return `UpdateByOp` to be used in a call to `update_by()`.
#'
#' @examples
#' \dontrun{
#' library(rdeephaven)
#'
#' # connecting to Deephaven server
#' client <- Client$new("localhost:10000", auth_type="psk", auth_token="my_secret_token")
#'
#' # create data frame, push to server, retrieve TableHandle
#' df <- data.frame(
#'   timeCol = seq.POSIXt(as.POSIXct(Sys.Date()), as.POSIXct(Sys.Date() + 0.01), by = "1 sec")[1:500],
#'   boolCol = sample(c(TRUE,FALSE), 500, TRUE),
#'   col1 = sample(10000, size = 500, replace = TRUE),
#'   col2 = sample(10000, size = 500, replace = TRUE),
#'   col3 = 1:500
#' )
#' th <- client$import_table(df)
#'
#' # compute rolling minimum of col1 and col2, using the previous 5 seconds
#' th1 <- th$
#'   update_by(uby_rolling_min_time(ts_col="timeCol", cols=c("col1RollMin = col1", "col2RollMin = col2"), rev_time="PT5s"))
#'
#' # compute rolling minimum of col1 and col2, grouped by boolCol, using previous 5 seconds, and following 5 seconds
#' th2 <- th$
#'   update_by(uby_rolling_min_time(ts_col="timeCol", cols=c("col1RollMin = col1", "col2RollMin = col2"), rev_time="PT5s", fwd_ticks="PT5s")), by="boolCol")
#'
#' # compute rolling minimum of col1 and col2, grouped by boolCol and parity of col3, using following 10 seconds
#' th3 <- th$
#'   update("col3Parity = col3 % 2")$
#'   update_by(uby_rolling_min_time(ts_col="timeCol", cols=c("col1RollMin = col1", "col2RollMin = col2"), rev_time="PT0s", fwd_time="PT10s")), by=c("boolCol", "col3Parity"))
#'
#' client$close()
#' }
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
#' This function acts on aggregation groups specified with the `by` parameter of the `update_by()` caller function.
#' The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like other Deephaven `uby` functions, is a generator function. That is, its output is another
#' function called an [`UpdateByOp`][UpdateByOp] intended to be used in a call to `update_by()`. This detail is typically
#' hidden from the user. However, it is important to understand this detail for debugging purposes, as the output of
#' a `uby` function can otherwise seem unexpected.
#'
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling maximum for all non-grouping columns.
#' @param rev_ticks Integer scalar denoting the look-behind window size in number of rows.
#' @param fwd_ticks Integer scalar denoting the look-ahead window size in number of rows. Default is 0.
#' @return `UpdateByOp` to be used in a call to `update_by()`.
#'
#' @examples
#' \dontrun{
#' library(rdeephaven)
#'
#' # connecting to Deephaven server
#' client <- Client$new("localhost:10000", auth_type="psk", auth_token="my_secret_token")
#'
#' # create data frame, push to server, retrieve TableHandle
#' df <- data.frame(
#'   timeCol = seq.POSIXt(as.POSIXct(Sys.Date()), as.POSIXct(Sys.Date() + 0.01), by = "1 sec")[1:500],
#'   boolCol = sample(c(TRUE,FALSE), 500, TRUE),
#'   col1 = sample(10000, size = 500, replace = TRUE),
#'   col2 = sample(10000, size = 500, replace = TRUE),
#'   col3 = 1:500
#' )
#' th <- client$import_table(df)
#'
#' # compute rolling maximum of col1 and col2, using the previous 5 rows and current row
#' th1 <- th$
#'   update_by(uby_rolling_max_tick(cols=c("col1RollMax = col1", "col2RollMax = col2"), rev_ticks=6))
#'
#' # compute rolling maximum of col1 and col2, grouped by boolCol, using previous 5 rows, current row, and following 5 rows
#' th2 <- th$
#'   update_by(uby_rolling_max_tick(cols=c("col1RollMax = col1", "col2RollMax = col2"), rev_ticks=6, fwd_ticks=5)), by="boolCol")
#'
#' # compute rolling maximum of col1 and col2, grouped by boolCol and parity of col3, using current row and following 10 rows
#' th3 <- th$
#'   update("col3Parity = col3 % 2")$
#'   update_by(uby_rolling_max_tick(cols=c("col1RollMax = col1", "col2RollMax = col2"), rev_ticks=1, fwd_ticks=10)), by=c("boolCol", "col3Parity"))
#'
#' client$close()
#' }
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
#' This uses [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) time strings as the reverse and forward window parameters.
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
#' This function acts on aggregation groups specified with the `by` parameter of the `update_by()` caller function.
#' The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like other Deephaven `uby` functions, is a generator function. That is, its output is another
#' function called an [`UpdateByOp`][UpdateByOp] intended to be used in a call to `update_by()`. This detail is typically
#' hidden from the user. However, it is important to understand this detail for debugging purposes, as the output of
#' a `uby` function can otherwise seem unexpected.
#'
#' @param ts_col String denoting the column to use as the timestamp.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling maximum for all non-grouping columns.
#' @param rev_time ISO-8601-formatted string specifying the look-behind window size.
#' @param fwd_time ISO-8601-formatted string specifying the look-ahead window size. Default is 0 seconds.
#' @return `UpdateByOp` to be used in a call to `update_by()`.
#'
#' @examples
#' \dontrun{
#' library(rdeephaven)
#'
#' # connecting to Deephaven server
#' client <- Client$new("localhost:10000", auth_type="psk", auth_token="my_secret_token")
#'
#' # create data frame, push to server, retrieve TableHandle
#' df <- data.frame(
#'   timeCol = seq.POSIXt(as.POSIXct(Sys.Date()), as.POSIXct(Sys.Date() + 0.01), by = "1 sec")[1:500],
#'   boolCol = sample(c(TRUE,FALSE), 500, TRUE),
#'   col1 = sample(10000, size = 500, replace = TRUE),
#'   col2 = sample(10000, size = 500, replace = TRUE),
#'   col3 = 1:500
#' )
#' th <- client$import_table(df)
#'
#' # compute rolling maximum of col1 and col2, using the previous 5 seconds
#' th1 <- th$
#'   update_by(uby_rolling_max_time(ts_col="timeCol", cols=c("col1RollMax = col1", "col2RollMax = col2"), rev_time="PT5s"))
#'
#' # compute rolling maximum of col1 and col2, grouped by boolCol, using previous 5 seconds, and following 5 seconds
#' th2 <- th$
#'   update_by(uby_rolling_max_time(ts_col="timeCol", cols=c("col1RollMax = col1", "col2RollMax = col2"), rev_time="PT5s", fwd_ticks="PT5s")), by="boolCol")
#'
#' # compute rolling maximum of col1 and col2, grouped by boolCol and parity of col3, using following 10 seconds
#' th3 <- th$
#'   update("col3Parity = col3 % 2")$
#'   update_by(uby_rolling_max_time(ts_col="timeCol", cols=c("col1RollMax = col1", "col2RollMax = col2"), rev_time="PT0s", fwd_time="PT10s")), by=c("boolCol", "col3Parity"))
#'
#' client$close()
#' }
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
#' Rolling product with ticks as the windowing unit
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
#' This function acts on aggregation groups specified with the `by` parameter of the `update_by()` caller function.
#' The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like other Deephaven `uby` functions, is a generator function. That is, its output is another
#' function called an [`UpdateByOp`][UpdateByOp] intended to be used in a call to `update_by()`. This detail is typically
#' hidden from the user. However, it is important to understand this detail for debugging purposes, as the output of
#' a `uby` function can otherwise seem unexpected.
#'
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling product for all non-grouping columns.
#' @param rev_ticks Integer scalar denoting the look-behind window size in number of rows.
#' @param fwd_ticks Integer scalar denoting the look-ahead window size in number of rows. Default is 0.
#' @return `UpdateByOp` to be used in a call to `update_by()`.
#'
#' @examples
#' \dontrun{
#' library(rdeephaven)
#'
#' # connecting to Deephaven server
#' client <- Client$new("localhost:10000", auth_type="psk", auth_token="my_secret_token")
#'
#' # create data frame, push to server, retrieve TableHandle
#' df <- data.frame(
#'   timeCol = seq.POSIXt(as.POSIXct(Sys.Date()), as.POSIXct(Sys.Date() + 0.01), by = "1 sec")[1:500],
#'   boolCol = sample(c(TRUE,FALSE), 500, TRUE),
#'   col1 = sample(10000, size = 500, replace = TRUE),
#'   col2 = sample(10000, size = 500, replace = TRUE),
#'   col3 = 1:500
#' )
#' th <- client$import_table(df)
#'
#' # compute rolling product of col1 and col2, using the previous 5 rows and current row
#' th1 <- th$
#'   update_by(uby_rolling_prod_tick(cols=c("col1RollProd = col1", "col2RollProd = col2"), rev_ticks=6))
#'
#' # compute rolling product of col1 and col2, grouped by boolCol, using previous 5 rows, current row, and following 5 rows
#' th2 <- th$
#'   update_by(uby_rolling_prod_tick(cols=c("col1RollProd = col1", "col2RollProd = col2"), rev_ticks=6, fwd_ticks=5)), by="boolCol")
#'
#' # compute rolling product of col1 and col2, grouped by boolCol and parity of col3, using current row and following 10 rows
#' th3 <- th$
#'   update("col3Parity = col3 % 2")$
#'   update_by(uby_rolling_prod_tick(cols=c("col1RollProd = col1", "col2RollProd = col2"), rev_ticks=1, fwd_ticks=10)), by=c("boolCol", "col3Parity"))
#'
#' client$close()
#' }
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
#' This uses [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) time strings as the reverse and forward window parameters.
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
#' This function acts on aggregation groups specified with the `by` parameter of the `update_by()` caller function.
#' The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like other Deephaven `uby` functions, is a generator function. That is, its output is another
#' function called an [`UpdateByOp`][UpdateByOp] intended to be used in a call to `update_by()`. This detail is typically
#' hidden from the user. However, it is important to understand this detail for debugging purposes, as the output of
#' a `uby` function can otherwise seem unexpected.
#'
#' @param ts_col String denoting the column to use as the timestamp.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling product for all non-grouping columns.
#' @param rev_time ISO-8601-formatted string specifying the look-behind window size.
#' @param fwd_time ISO-8601-formatted string specifying the look-ahead window size. Default is 0 seconds.
#' @return `UpdateByOp` to be used in a call to `update_by()`.
#'
#' @examples
#' \dontrun{
#' library(rdeephaven)
#'
#' # connecting to Deephaven server
#' client <- Client$new("localhost:10000", auth_type="psk", auth_token="my_secret_token")
#'
#' # create data frame, push to server, retrieve TableHandle
#' df <- data.frame(
#'   timeCol = seq.POSIXt(as.POSIXct(Sys.Date()), as.POSIXct(Sys.Date() + 0.01), by = "1 sec")[1:500],
#'   boolCol = sample(c(TRUE,FALSE), 500, TRUE),
#'   col1 = sample(10000, size = 500, replace = TRUE),
#'   col2 = sample(10000, size = 500, replace = TRUE),
#'   col3 = 1:500
#' )
#' th <- client$import_table(df)
#'
#' # compute rolling product of col1 and col2, using the previous 5 seconds
#' th1 <- th$
#'   update_by(uby_rolling_prod_time(ts_col="timeCol", cols=c("col1RollProd = col1", "col2RollProd = col2"), rev_time="PT5s"))
#'
#' # compute rolling product of col1 and col2, grouped by boolCol, using previous 5 seconds, and following 5 seconds
#' th2 <- th$
#'   update_by(uby_rolling_prod_time(ts_col="timeCol", cols=c("col1RollProd = col1", "col2RollProd = col2"), rev_time="PT5s", fwd_ticks="PT5s")), by="boolCol")
#'
#' # compute rolling product of col1 and col2, grouped by boolCol and parity of col3, using following 10 seconds
#' th3 <- th$
#'   update("col3Parity = col3 % 2")$
#'   update_by(uby_rolling_prod_time(ts_col="timeCol", cols=c("col1RollProd = col1", "col2RollProd = col2"), rev_time="PT0s", fwd_time="PT10s")), by=c("boolCol", "col3Parity"))
#'
#' client$close()
#' }
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
#' This function acts on aggregation groups specified with the `by` parameter of the `update_by()` caller function.
#' The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like other Deephaven `uby` functions, is a generator function. That is, its output is another
#' function called an [`UpdateByOp`][UpdateByOp] intended to be used in a call to `update_by()`. This detail is typically
#' hidden from the user. However, it is important to understand this detail for debugging purposes, as the output of
#' a `uby` function can otherwise seem unexpected.
#'
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling count for all non-grouping columns.
#' @param rev_ticks Integer scalar denoting the look-behind window size in number of rows.
#' @param fwd_ticks Integer scalar denoting the look-ahead window size in number of rows. Default is 0.
#' @return `UpdateByOp` to be used in a call to `update_by()`.
#'
#' @examples
#' \dontrun{
#' library(rdeephaven)
#'
#' # connecting to Deephaven server
#' client <- Client$new("localhost:10000", auth_type="psk", auth_token="my_secret_token")
#'
#' # create data frame, push to server, retrieve TableHandle
#' df <- data.frame(
#'   timeCol = seq.POSIXt(as.POSIXct(Sys.Date()), as.POSIXct(Sys.Date() + 0.01), by = "1 sec")[1:500],
#'   boolCol = sample(c(TRUE,FALSE), 500, TRUE),
#'   col1 = sample(10000, size = 500, replace = TRUE),
#'   col2 = sample(10000, size = 500, replace = TRUE),
#'   col3 = 1:500
#' )
#' th <- client$import_table(df)
#'
#' # compute rolling count of col1 and col2, using the previous 5 rows and current row
#' th1 <- th$
#'   update_by(uby_rolling_count_tick(cols=c("col1RollCount = col1", "col2RollCount = col2"), rev_ticks=6))
#'
#' # compute rolling count of col1 and col2, grouped by boolCol, using previous 5 rows, current row, and following 5 rows
#' th2 <- th$
#'   update_by(uby_rolling_count_tick(cols=c("col1RollCount = col1", "col2RollCount = col2"), rev_ticks=6, fwd_ticks=5), by="boolCol")
#'
#' # compute rolling count of col1 and col2, grouped by boolCol and parity of col3, using current row and following 10 rows
#' th3 <- th$
#'   update("col3Parity = col3 % 2")$
#'   update_by(uby_rolling_count_tick(cols=c("col1RollCount = col1", "col2RollCount = col2"), rev_ticks=1, fwd_ticks=10), by=c("boolCol", "col3Parity"))
#'
#' client$close()
#' }
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
#' This uses [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) time strings as the reverse and forward window parameters.
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
#' This function acts on aggregation groups specified with the `by` parameter of the `update_by()` caller function.
#' The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like other Deephaven `uby` functions, is a generator function. That is, its output is another
#' function called an [`UpdateByOp`][UpdateByOp] intended to be used in a call to `update_by()`. This detail is typically
#' hidden from the user. However, it is important to understand this detail for debugging purposes, as the output of
#' a `uby` function can otherwise seem unexpected.
#'
#' @param ts_col String denoting the column to use as the timestamp.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling count for all non-grouping columns.
#' @param rev_time ISO-8601-formatted string specifying the look-behind window size.
#' @param fwd_time ISO-8601-formatted string specifying the look-ahead window size. Default is 0 seconds.
#' @return `UpdateByOp` to be used in a call to `update_by()`.
#'
#' @examples
#' \dontrun{
#' library(rdeephaven)
#'
#' # connecting to Deephaven server
#' client <- Client$new("localhost:10000", auth_type="psk", auth_token="my_secret_token")
#'
#' # create data frame, push to server, retrieve TableHandle
#' df <- data.frame(
#'   timeCol = seq.POSIXt(as.POSIXct(Sys.Date()), as.POSIXct(Sys.Date() + 0.01), by = "1 sec")[1:500],
#'   boolCol = sample(c(TRUE,FALSE), 500, TRUE),
#'   col1 = sample(10000, size = 500, replace = TRUE),
#'   col2 = sample(10000, size = 500, replace = TRUE),
#'   col3 = 1:500
#' )
#' th <- client$import_table(df)
#'
#' # compute rolling count of col1 and col2, using the previous 5 seconds
#' th1 <- th$
#'   update_by(uby_rolling_count_time(ts_col="timeCol", cols=c("col1RollCount = col1", "col2RollCount = col2"), rev_time="PT5s"))
#'
#' # compute rolling count of col1 and col2, grouped by boolCol, using previous 5 seconds, and following 5 seconds
#' th2 <- th$
#'   update_by(uby_rolling_count_time(ts_col="timeCol", cols=c("col1RollCount = col1", "col2RollCount = col2"), rev_time="PT5s", fwd_ticks="PT5s"), by="boolCol")
#'
#' # compute rolling count of col1 and col2, grouped by boolCol and parity of col3, using following 10 seconds
#' th3 <- th$
#'   update("col3Parity = col3 % 2")$
#'   update_by(uby_rolling_count_time(ts_col="timeCol", cols=c("col1RollCount = col1", "col2RollCount = col2"), rev_time="PT0s", fwd_time="PT10s"), by=c("boolCol", "col3Parity"))
#'
#' client$close()
#' }
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
#' This function acts on aggregation groups specified with the `by` parameter of the `update_by()` caller function.
#' The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like other Deephaven `uby` functions, is a generator function. That is, its output is another
#' function called an [`UpdateByOp`][UpdateByOp] intended to be used in a call to `update_by()`. This detail is typically
#' hidden from the user. However, it is important to understand this detail for debugging purposes, as the output of
#' a `uby` function can otherwise seem unexpected.
#'
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling standard deviation for all non-grouping columns.
#' @param rev_ticks Integer scalar denoting the look-behind window size in number of rows.
#' @param fwd_ticks Integer scalar denoting the look-ahead window size in number of rows. Default is 0.
#' @return `UpdateByOp` to be used in a call to `update_by()`.
#'
#' @examples
#' \dontrun{
#' library(rdeephaven)
#'
#' # connecting to Deephaven server
#' client <- Client$new("localhost:10000", auth_type="psk", auth_token="my_secret_token")
#'
#' # create data frame, push to server, retrieve TableHandle
#' df <- data.frame(
#'   timeCol = seq.POSIXt(as.POSIXct(Sys.Date()), as.POSIXct(Sys.Date() + 0.01), by = "1 sec")[1:500],
#'   boolCol = sample(c(TRUE,FALSE), 500, TRUE),
#'   col1 = sample(10000, size = 500, replace = TRUE),
#'   col2 = sample(10000, size = 500, replace = TRUE),
#'   col3 = 1:500
#' )
#' th <- client$import_table(df)
#'
#' # compute rolling standard deviation of col1 and col2, using the previous 5 rows and current row
#' th1 <- th$
#'   update_by(uby_rolling_std_tick(cols=c("col1RollStd = col1", "col2RollStd = col2"), rev_ticks=6))
#'
#' # compute rolling standard deviation of col1 and col2, grouped by boolCol, using previous 5 rows, current row, and following 5 rows
#' th2 <- th$
#'   update_by(uby_rolling_std_tick(cols=c("col1RollStd = col1", "col2RollStd = col2"), rev_ticks=6, fwd_ticks=5), by="boolCol")
#'
#' # compute rolling standard deviation of col1 and col2, grouped by boolCol and parity of col3, using current row and following 10 rows
#' th3 <- th$
#'   update("col3Parity = col3 % 2")$
#'   update_by(uby_rolling_std_tick(cols=c("col1RollStd = col1", "col2RollStd = col2"), rev_ticks=1, fwd_ticks=10), by=c("boolCol", "col3Parity"))
#'
#' client$close()
#' }
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
#' This uses [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) time strings as the reverse and forward window parameters.
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
#' This function acts on aggregation groups specified with the `by` parameter of the `update_by()` caller function.
#' The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like other Deephaven `uby` functions, is a generator function. That is, its output is another
#' function called an [`UpdateByOp`][UpdateByOp] intended to be used in a call to `update_by()`. This detail is typically
#' hidden from the user. However, it is important to understand this detail for debugging purposes, as the output of
#' a `uby` function can otherwise seem unexpected.
#'
#' @param ts_col String denoting the column to use as the timestamp.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling standard deviation for all non-grouping columns.
#' @param rev_time ISO-8601-formatted string specifying the look-behind window size.
#' @param fwd_time ISO-8601-formatted string specifying the look-ahead window size. Default is 0 seconds.
#' @return `UpdateByOp` to be used in a call to `update_by()`.
#'
#' @examples
#' \dontrun{
#' library(rdeephaven)
#'
#' # connecting to Deephaven server
#' client <- Client$new("localhost:10000", auth_type="psk", auth_token="my_secret_token")
#'
#' # create data frame, push to server, retrieve TableHandle
#' df <- data.frame(
#'   timeCol = seq.POSIXt(as.POSIXct(Sys.Date()), as.POSIXct(Sys.Date() + 0.01), by = "1 sec")[1:500],
#'   boolCol = sample(c(TRUE,FALSE), 500, TRUE),
#'   col1 = sample(10000, size = 500, replace = TRUE),
#'   col2 = sample(10000, size = 500, replace = TRUE),
#'   col3 = 1:500
#' )
#' th <- client$import_table(df)
#'
#' # compute rolling standard deviation of col1 and col2, using the previous 5 seconds
#' th1 <- th$
#'   update_by(uby_rolling_std_time(ts_col="timeCol", cols=c("col1RollStd = col1", "col2RollStd = col2"), rev_time="PT5s"))
#'
#' # compute rolling standard deviation of col1 and col2, grouped by boolCol, using previous 5 seconds, and following 5 seconds
#' th2 <- th$
#'   update_by(uby_rolling_std_time(ts_col="timeCol", cols=c("col1RollStd = col1", "col2RollStd = col2"), rev_time="PT5s", fwd_ticks="PT5s"), by="boolCol")
#'
#' # compute rolling standard deviation of col1 and col2, grouped by boolCol and parity of col3, using following 10 seconds
#' th3 <- th$
#'   update("col3Parity = col3 % 2")$
#'   update_by(uby_rolling_std_time(ts_col="timeCol", cols=c("col1RollStd = col1", "col2RollStd = col2"), rev_time="PT0s", fwd_time="PT10s"), by=c("boolCol", "col3Parity"))
#'
#' client$close()
#' }
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
#' This function acts on aggregation groups specified with the `by` parameter of the `update_by()` caller function.
#' The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like other Deephaven `uby` functions, is a generator function. That is, its output is another
#' function called an [`UpdateByOp`][UpdateByOp] intended to be used in a call to `update_by()`. This detail is typically
#' hidden from the user. However, it is important to understand this detail for debugging purposes, as the output of
#' a `uby` function can otherwise seem unexpected.
#'
#' @param wcol String denoting the column to use for weights. This must be a numeric column.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling weighted average for all non-grouping columns.
#' @param rev_ticks Integer scalar denoting the look-behind window size in number of rows.
#' @param fwd_ticks Integer scalar denoting the look-ahead window size in number of rows. Default is 0.
#' @return `UpdateByOp` to be used in a call to `update_by()`.
#'
#' @examples
#' \dontrun{
#' library(rdeephaven)
#'
#' # connecting to Deephaven server
#' client <- Client$new("localhost:10000", auth_type="psk", auth_token="my_secret_token")
#'
#' # create data frame, push to server, retrieve TableHandle
#' df <- data.frame(
#'   timeCol = seq.POSIXt(as.POSIXct(Sys.Date()), as.POSIXct(Sys.Date() + 0.01), by = "1 sec")[1:500],
#'   boolCol = sample(c(TRUE,FALSE), 500, TRUE),
#'   col1 = sample(10000, size = 500, replace = TRUE),
#'   col2 = sample(10000, size = 500, replace = TRUE),
#'   col3 = 1:500
#' )
#' th <- client$import_table(df)
#'
#' # compute rolling weighted average of col1 and col2, weighted by col3, using the previous 5 rows and current row
#' th1 <- th$
#'   update_by(uby_rolling_wavg_tick(wcol="col3", cols=c("col1RollWAvg = col1", "col2RollWAvg = col2"), rev_ticks=6))
#'
#' # compute rolling weighted average of col1 and col2, weighted by col3, grouped by boolCol, using previous 5 rows, current row, and following 5 rows
#' th2 <- th$
#'   update_by(uby_rolling_wavg_tick(wcol="col3", cols=c("col1RollWAvg = col1", "col2RollWAvg = col2"), rev_ticks=6, fwd_ticks=5), by="boolCol")
#'
#' # compute rolling weighted average of col1 and col2, weighted by col3, grouped by boolCol and parity of col3, using current row and following 10 rows
#' th3 <- th$
#'   update("col3Parity = col3 % 2")$
#'   update_by(uby_rolling_wavg_tick(wcol="col3", cols=c("col1RollWAvg = col1", "col2RollWAvg = col2"), rev_ticks=1, fwd_ticks=10), by=c("boolCol", "col3Parity"))
#'
#' client$close()
#' }
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
#' This uses [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) time strings as the reverse and forward window parameters.
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
#' This function acts on aggregation groups specified with the `by` parameter of the `update_by()` caller function.
#' The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the
#' `A` and `B` columns.
#'
#' This function, like other Deephaven `uby` functions, is a generator function. That is, its output is another
#' function called an [`UpdateByOp`][UpdateByOp] intended to be used in a call to `update_by()`. This detail is typically
#' hidden from the user. However, it is important to understand this detail for debugging purposes, as the output of
#' a `uby` function can otherwise seem unexpected.
#'
#' @param ts_col String denoting the column to use as the timestamp.
#' @param wcol String denoting the column to use for weights. This must be a numeric column.
#' @param cols String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to compute the rolling weighted average for all non-grouping columns.
#' @param rev_time ISO-8601-formatted string specifying the look-behind window size.
#' @param fwd_time ISO-8601-formatted string specifying the look-ahead window size. Default is 0 seconds.
#' @return `UpdateByOp` to be used in a call to `update_by()`.
#'
#' @examples
#' \dontrun{
#' library(rdeephaven)
#'
#' # connecting to Deephaven server
#' client <- Client$new("localhost:10000", auth_type="psk", auth_token="my_secret_token")
#'
#' # create data frame, push to server, retrieve TableHandle
#' df <- data.frame(
#'   timeCol = seq.POSIXt(as.POSIXct(Sys.Date()), as.POSIXct(Sys.Date() + 0.01), by = "1 sec")[1:500],
#'   boolCol = sample(c(TRUE,FALSE), 500, TRUE),
#'   col1 = sample(10000, size = 500, replace = TRUE),
#'   col2 = sample(10000, size = 500, replace = TRUE),
#'   col3 = 1:500
#' )
#' th <- client$import_table(df)
#'
#' # compute rolling weighted average of col1 and col2, weighted by col3, using the previous 5 seconds
#' th1 <- th$
#'   update_by(uby_rolling_wavg_time(ts_col="timeCol", wcol="col3", cols=c("col1RollWAvg = col1", "col2RollWAvg = col2"), rev_time="PT5s"))
#'
#' # compute rolling weighted average of col1 and col2, weighted by col3, grouped by boolCol, using previous 5 seconds, and following 5 seconds
#' th2 <- th$
#'   update_by(uby_rolling_wavg_time(ts_col="timeCol", wcol="col3", cols=c("col1RollWAvg = col1", "col2RollWAvg = col2"), rev_time="PT5s", fwd_ticks="PT5s"), by="boolCol")
#'
#' # compute rolling weighted average of col1 and col2, weighted by col3, grouped by boolCol and parity of col3, using following 10 seconds
#' th3 <- th$
#'   update("col3Parity = col3 % 2")$
#'   update_by(uby_rolling_wavg_time(ts_col="timeCol", wcol="col3", cols=c("col1RollWAvg = col1", "col2RollWAvg = col2"), rev_time="PT0s", fwd_time="PT10s"), by=c("boolCol", "col3Parity"))
#'
#' client$close()
#' }
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
