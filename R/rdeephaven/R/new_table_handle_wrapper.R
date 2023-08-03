#' @importFrom arrow as_arrow_table RecordBatchStreamReader
#' @importFrom dplyr as_tibble

#' @export
setClass(
  "S4TableHandle",
  representation(
    .internal_rcpp_object = "Rcpp_INTERNAL_TableHandle"
  )
)

### TABLEHANDLE PROPERTIES

setGeneric(
  "is_static",
  function(table_handle_instance) {
    return(standardGeneric("is_static"))
  },
  signature = c("table_handle_instance")
)

#' @export
setMethod(
  "is_static",
  signature = c(table_handle_instance = "S4TableHandle"),
  function(table_handle_instance) {
    return(table_handle_instance@.internal_rcpp_object$is_static())
  }
)

#' @export
setMethod(
  "head",
  signature = c(x = "S4TableHandle"),
  function(x, n, ...) {
    verify_positive_int("n", n, TRUE)
    return(new("S4TableHandle", .internal_rcpp_object = x@.internal_rcpp_object$head(n)))
  }
)

#' @export
setMethod(
  "tail",
  signature = c(x = "S4TableHandle"),
  function(x, n, ...) {
    print("I'm here!")
    verify_positive_int("n", n, TRUE)
    return(new("S4TableHandle", .internal_rcpp_object = x@.internal_rcpp_object$tail(n)))
  }
)

# TODO: Implement nrow, ncol, dim

### TABLEHANDLE CONVERSIONS ###

# could potentially be implemented by other packages
setGeneric(
  "as_arrow_record_batch_stream_reader",
  function(x, ...) {
    return(standardGeneric("as_arrow_record_batch_stream_reader"))
  },
  signature = c("x")
)

#' @export
setMethod(
  "as_arrow_record_batch_stream_reader",
  signature = c(x = "S4TableHandle"),
  function(x, ...) {
    ptr <- x@.internal_rcpp_object$get_arrow_array_stream_ptr()
    rbsr <- RecordBatchStreamReader$import_from_c(ptr)
    return(rbsr)
  }
)

#' @export
setMethod(
  "as_arrow_table",
  signature = c(x = "S4TableHandle"),
  function(x, ...) {
    rbsr <- as_arrow_record_batch_stream_reader(x)
    arrow_tbl <- rbsr$read_table()
    return(arrow_tbl)
  }
)

#' @export
setMethod(
  "as_tibble",
  signature = c(x = "S4TableHandle"),
  function(x, ...) {
    rbsr <- as_arrow_record_batch_stream_reader(x)
    arrow_tbl <- rbsr$read_table()
    return(as_tibble(arrow_tbl))
  }
)

#' @export
setMethod(
  "as.data.frame",
  signature = c(x = "S4TableHandle"),
  function(x, ...) {
    arrow_tbl <- as_arrow_table(x)
    return(as.data.frame(as.data.frame(arrow_tbl)))
  }
)

### TABLEHANDLE OPERATIONS ###

setGeneric(
  "bind_to_variable",
  function(table_handle_instance, name) {
    return(standardGeneric("bind_to_variable"))
  },
  signature = c("table_handle_instance", "name")
)

#' @export
setMethod(
  "bind_to_variable",
  signature = c(table_handle_instance = "S4TableHandle"),
  function(table_handle_instance, name) {
    verify_string("name", name, TRUE)
    table_handle_instance@.internal_rcpp_object$bind_to_variable(name)
    return(NULL)
  }
)