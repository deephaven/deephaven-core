#' @export
setClass(
  "TableHandle",
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
  signature = c(table_handle_instance = "TableHandle"),
  function(table_handle_instance) {
    return(table_handle_instance@.internal_rcpp_object$is_static())
  }
)

#' @export
setMethod(
  "head",
  signature = c(x = "TableHandle"),
  function(x, n, ...) {
    verify_positive_int("n", n, TRUE)
    return(new("TableHandle", .internal_rcpp_object = x@.internal_rcpp_object$head(n)))
  }
)

#' @export
setMethod(
  "tail",
  signature = c(x = "TableHandle"),
  function(x, n, ...) {
    verify_positive_int("n", n, TRUE)
    return(new("TableHandle", .internal_rcpp_object = x@.internal_rcpp_object$tail(n)))
  }
)

#' @export
setMethod(
  "nrow",
  signature = c(x = "TableHandle"),
  function(x) {
    return(x@.internal_rcpp_object$num_rows())
  }
)

#' @export
setMethod(
  "ncol",
  signature = c(x = "TableHandle"),
  function(x) {
    return(x@.internal_rcpp_object$num_cols())
  }
)

#' @export
setMethod(
  "dim",
  signature = c(x = "TableHandle"),
  function(x) {
    return(c(x@.internal_rcpp_object$num_rows(), x@.internal_rcpp_object$num_cols()))
  }
)

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
  signature = c(table_handle_instance = "TableHandle", name = "character"),
  function(table_handle_instance, name) {
    verify_string("name", name, TRUE)
    table_handle_instance@.internal_rcpp_object$bind_to_variable(name)
    return(NULL)
  }
)


### TABLEHANDLE CONVERSIONS ###

#' @export
setMethod(
  "as_record_batch_reader",
  signature = c(x = "TableHandle"),
  function(x, ...) {
    ptr <- x@.internal_rcpp_object$get_arrow_array_stream_ptr()
    rbsr <- RecordBatchStreamReader$import_from_c(ptr)
    return(rbsr)
  }
)

#' @export
setMethod(
  "as_arrow_table",
  signature = c(x = "TableHandle"),
  function(x, ...) {
    rbsr <- as_record_batch_reader(x)
    arrow_tbl <- rbsr$read_table()
    return(arrow_tbl)
  }
)

#' @export
setMethod(
  "as_tibble",
  signature = c(x = "TableHandle"),
  function(x, ...) {
    rbsr <- as_record_batch_reader(x)
    arrow_tbl <- rbsr$read_table()
    return(as_tibble(arrow_tbl))
  }
)

#' @export
setMethod(
  "as.data.frame",
  signature = c(x = "TableHandle"),
  function(x, ...) {
    arrow_tbl <- as_arrow_table(x)
    return(as.data.frame(as.data.frame(arrow_tbl)))
  }
)

#' @export
setMethod(
  "as_data_frame",
  signature = c(x = "TableHandle"),
  function(x, ...) {
    return(as.data.frame(x))
  }
)
