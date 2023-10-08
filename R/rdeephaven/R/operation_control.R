#' @description
#' An OperationControl defines the control parameters of some UpdateByOps used in an update_by table operation.
#' The UpdateByOps can use OperationControl to handle erroneous data are `ema_tick`, `ema_time`, `emmax_tick`,
#' `emmax_time`, `emmin_tick`, `emmin_time`, and `ems_time`.
#' Note that OperationControl should not be instantiated directly by user code, but rather `by op_control()`.
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

#' @description
#' Creates an OperationControl that controls behavior on special values in `uby_em*` operations. The arguments `on_null`
#' and `on_nan` can take the following values:
#'   'poison': Allow bad data to poison the result. This is only valid for use with NaN.
#'   'reset': Reset the state for the bucket to NULL when invalid data is encountered.
#'   'skip': Skip and do not process the invalid data without changing state.
#'   'throw': Throw an exception and abort processing when bad data is encountered.
#' @param on_null Defines how an UpdateByOp handles null values it encounters. 'skip' is the default.
#' @param on_nan Defines how an UpdateByOp handles NaN values it encounters. 'skip' is the default.
#' @param big_value_context Defines how an UpdateByOp handles exceptionally large values it encounters.
#' The default value is 'decimal128'. The following values are available:
#'   'decimal128': IEEE 754R Decimal128 format. 34 digits and rounding is half-even.
#'   'decimal32': IEEE 754R Decimal32 format. 7 digits and rounding is half-even.
#'   'decimal64': IEEE 754R Decimal64 format. 16 digits and rounding is half-even.
#'   'unlimited': Unlimited precision arithmetic. Rounding is half-up.
#' @return OperationControl to be used in `uby_em*`.
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
