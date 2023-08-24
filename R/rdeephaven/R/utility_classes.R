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
