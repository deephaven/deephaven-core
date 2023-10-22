# An OperationControl defines the control parameters of some UpdateByOps used in an update_by table operation.
# This is the return type of op_control(). It is a wrapper around an Rcpp_INTERNAL_OperationControl, which itself is a
# wrapper around a C++ OperationControl. See rdeephaven/src/client.cpp for details.
# Note that OperationControl should not be instantiated directly by user code, but rather by op_control().


#' @name OperationControl
#' @title Deephaven OperationControl
#' @md
#' @description
#' An `OperationControl` is the return type of Deephaven's [`op_control`][op_control] function. It is a function that
#' determines how special values will be handled in the context of an [`update_by`][UpdateBy] operation.
#' An `OperationControl` is intended to be passed directly to a `uby` function, and should never be instantiated
#' directly be user code.
#'
#' If you plan to use the same operation control parameters for multiple `uby` functions in one or more `update_by` calls,
#' consider creating an `OperationControl` object and reusing it. For example:
#' ```
#' opc <- op_control(on_null = 'skip', on_nan = 'poison', big_value_context = 'decimal64')
#'
#' result <- th$update_by(c(uby_ema_tick(5, c("XEma = X", "YEma = Y"), opc),
#'                          uby_emstd_tick(5, c("XEmstd = X", "YEmstd = Y"), opc)),
#'                        by="Group")
#' ```
#'
#' @usage NULL
#' @format NULL
#' @docType class
#' @export
OperationControl <- R6Class("OperationControl",
  cloneable = FALSE,
  public = list(
    .internal_rcpp_object = NULL,
    initialize = function(op_control) {
      if (class(op_control) != "Rcpp_INTERNAL_OperationControl") {
        stop("'op_control' should be an internal Deephaven OperationControl. If you're seeing this,\n  you are trying to call the constructor of an OperationControl directly, which is not advised.\n  Please use the op_control function instead.")
      }
      self$.internal_rcpp_object <- op_control
    }
  )
)

#' @name
#' op_control
#' @title
#' Handling special values in some UpdateBy operations
#' @md
#'
#' @description
#' Creates an `OperationControl` that controls behavior on special values in some UpdateBy operations. This function is
#' the only way to properly create an `OperationControl` object.
#'
#' @details
#' An [`OperationControl`][OperationControl] defines the control parameters of some `uby` functions used in an `update_by()` call.
#' The `uby` functions that can use an `OperationControl` to handle erroneous data are:
#'
#' - [`uby_ema_tick()`][uby_ema_tick]
#' - [`uby_ema_time()`][uby_ema_time]
#' - [`uby_ems_tick()`][uby_ems_tick]
#' - [`uby_ems_time()`][uby_ems_time]
#' - [`uby_emmin_tick()`][uby_emmin_tick]
#' - [`uby_emmin_time()`][uby_emmin_time]
#' - [`uby_emmax_tick()`][uby_emmax_tick]
#' - [`uby_emmax_time()`][uby_emmax_time]
#' - [`uby_emstd_tick()`][uby_emstd_tick]
#' - [`uby_emstd_time()`][uby_emstd_time]
#'
#' The arguments `on_null` and `on_nan` can take the following values:
#'
#' - `'poison'`: Allow bad data to poison the result, meaning that any calculation involving at least one NaN will yield
#'   NaN. This is only valid for use with NaN.
#' - `'reset'`: Reset the state for the bucket to NULL when invalid data is encountered.
#' - `'skip'`: Skip and do not process the invalid data without changing state.
#' - `'throw'`: Throw an exception and abort processing when bad data is encountered.
#'
#' The argument `big_value_context` can take the following values:
#'
#' - `'decimal128'`: IEEE 754R Decimal128 format. 34 digits and rounding is half-even.
#' - `'decimal32'`: IEEE 754R Decimal32 format. 7 digits and rounding is half-even.
#' - `'decimal64'`: IEEE 754R Decimal64 format. 16 digits and rounding is half-even.
#' - `'unlimited'`: Unlimited precision arithmetic. Rounding is half-up.
#'
#' This function is a generator function. That is, its output is another function called an [`OperationControl`][OperationControl] intended
#' to be used in a call to one of the above `uby` functions. This detail is typically hidden from the user. However,
#' it is important to understand this detail for debugging purposes, as the output of a call to `op_control()` can otherwise seem unexpected.
#'
#' @param on_null Defines how a `uby` function handles null values it encounters. `'skip'` is the default.
#' @param on_nan Defines how a `uby` function handles NaN values it encounters. `'skip'` is the default.
#' @param big_value_context Defines how a `uby` function handles exceptionally large values it encounters.
#' `'decimal128'` is the default.
#' @return `OperationControl` to be used in a `uby` function.
#'
#' @examples
#' print("hello!")
#'
#' @export
op_control <- function(on_null="skip", on_nan="skip", big_value_context="decimal128") {

  if (!(on_null %in% c("poison", "reset", "skip", "throw"))) {
    stop(paste0("'on_null' must be one of 'poison', 'reset', 'skip', or 'throw'. Got '", on_null, "'."))
  }
  if (!(on_nan %in% c("poison", "reset", "skip", "throw"))) {
    stop(paste0("'on_nan' must be one of 'poison', 'reset', 'skip', or 'throw'. Got '", on_nan, "'."))
  }
  if (!(big_value_context %in% c("decimal32", "decimal64", "decimal128", "unlimited"))) {
    stop(paste0("'big_value_context' must be one of 'decimal32', 'decimal64', 'decimal128', or 'unlimited'. Got '", big_value_context, "'."))
  }
  return(OperationControl$new(INTERNAL_op_control_generator(on_null, on_nan, big_value_context)))
}
