#' @export
TableHandle <- R6Class("TableHandle",
  cloneable = FALSE,
  public = list(
    .internal_rcpp_object = NULL,
    initialize = function(table_handle) {
      if (class(table_handle)[[1]] != "Rcpp_INTERNAL_TableHandle") {
        stop("'table_handle' should be an internal Deephaven TableHandle. If you're seeing this,
                you are trying to call the constructor of TableHandle directly, which is not advised.")
      }
      self$.internal_rcpp_object <- table_handle
    },
    is_static = function() {
      return(self$.internal_rcpp_object$is_static())
    },
    bind_to_variable = function(name) {
      verify_string("name", name, TRUE)
      self$.internal_rcpp_object$bind_to_variable(name)
    },

    ### BASE R METHODS, ALSO IMPLEMENTED FUNCTIONALLY

    head = function(n) {
      verify_positive_int("n", n, TRUE)
      return(TableHandle$new(self$.internal_rcpp_object$head(n)))
    },
    tail = function(n) {
      verify_positive_int("n", n, TRUE)
      return(TableHandle$new(self$.internal_rcpp_object$tail(n)))
    },
    nrow = function() {
      return(self$.internal_rcpp_object$num_rows())
    },
    ncol = function() {
      return(self$.internal_rcpp_object$num_cols())
    },
    dim = function() {
      return(c(self$nrow(), self$ncol()))
    },
    merge = function(...) {
      table_list <- unlist(c(...))
      if (length(table_list) == 0) {
        return(self)
      }
      verify_type("table_list", table_list, "TableHandle", "Deephaven TableHandle", FALSE)
      unwrapped_table_list <- lapply(table_list, strip_r6_wrapping)
      return(TableHandle$new(self$.internal_rcpp_object$merge(unwrapped_table_list)))
    },

    ### CONVERSION METHODS, ALSO IMPLEMENTED FUNCTIONALLY

    as_record_batch_reader = function() {
      ptr <- self$.internal_rcpp_object$get_arrow_array_stream_ptr()
      rbsr <- RecordBatchStreamReader$import_from_c(ptr)
      return(rbsr)
    },
    as_arrow_table = function() {
      rbsr <- self$as_record_batch_reader()
      arrow_tbl <- rbsr$read_table()
      return(arrow_tbl)
    },
    as_tibble = function() {
      rbsr <- self$as_record_batch_reader()
      arrow_tbl <- rbsr$read_table()
      return(as_tibble(arrow_tbl))
    },
    as_data_frame = function() {
      arrow_tbl <- self$as_arrow_table()
      return(as.data.frame(as.data.frame(arrow_tbl))) # TODO: for some reason as.data.frame on arrow table returns a tibble, not a data frame
    },

    ### DEEPHAVEN TABLE OPERATIONS

    select = function(formulas = character()) {
      verify_string("formulas", formulas, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$select(formulas)))
    },
    view = function(formulas = character()) {
      verify_string("formulas", formulas, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$view(formulas)))
    },
    update = function(formulas = character()) {
      verify_string("formulas", formulas, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$update(formulas)))
    },
    update_view = function(formulas = character()) {
      verify_string("formulas", formulas, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$update_view(formulas)))
    },
    drop_columns = function(cols = character()) {
      verify_string("cols", cols, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$drop_columns(cols)))
    },
    where = function(filter) {
      verify_string("filter", filter, TRUE)
      return(TableHandle$new(self$.internal_rcpp_object$where(filter)))
    },
    group_by = function(by = character()) {
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$group_by(by)))
    },
    ungroup = function(by = character()) {
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$ungroup(by)))
    },
    agg_by = function(aggs, by = character()) {
      verify_type("aggs", aggs, "Aggregation", "Deephaven Aggregation", FALSE)
      verify_string("by", by, FALSE)
      aggs <- c(aggs)
      unwrapped_aggs <- lapply(aggs, strip_r6_wrapping)
      return(TableHandle$new(self$.internal_rcpp_object$agg_by(unwrapped_aggs, by)))
    },
    first_by = function(by = character()) {
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$first_by(by)))
    },
    last_by = function(by = character()) {
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$last_by(by)))
    },
    head_by = function(num_rows, by = character()) {
      verify_positive_int("num_rows", num_rows, TRUE)
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$head_by(num_rows, by)))
    },
    tail_by = function(num_rows, by = character()) {
      verify_positive_int("num_rows", num_rows, TRUE)
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$tail_by(num_rows, by)))
    },
    min_by = function(by = character()) {
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$min_by(by)))
    },
    max_by = function(by = character()) {
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$max_by(by)))
    },
    sum_by = function(by = character()) {
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$sum_by(by)))
    },
    abs_sum_by = function(by = character()) {
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$abs_sum_by(by)))
    },
    avg_by = function(by = character()) {
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$avg_by(by)))
    },
    w_avg_by = function(wcol, by = character()) {
      verify_string("wcol", wcol, TRUE)
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$w_avg_by(wcol, by)))
    },
    median_by = function(by = character()) {
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$median_by(by)))
    },
    var_by = function(by = character()) {
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$var_by(by)))
    },
    std_by = function(by = character()) {
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$std_by(by)))
    },
    percentile_by = function(percentile, by = character()) {
      verify_in_unit_interval("percentile", percentile, TRUE)
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$percentile_by(percentile, by)))
    },
    count_by = function(col = "n", by = character()) {
      verify_string("col", col, TRUE)
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$count_by(col, by)))
    },
    cross_join = function(table, on = character(), joins = character()) {
      verify_string("on", on, FALSE)
      verify_string("joins", joins, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$cross_join(
        table$.internal_rcpp_object,
        on, joins
      )))
    },
    natural_join = function(table, on = character(), joins = character()) {
      verify_string("on", on, FALSE)
      verify_string("joins", joins, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$natural_join(
        table$.internal_rcpp_object,
        on, joins
      )))
    },
    exact_join = function(table, on = character(), joins = character()) {
      verify_string("on", on, FALSE)
      verify_string("joins", joins, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$exact_join(
        table$.internal_rcpp_object,
        on, joins
      )))
    },
    sort = function(order_by, descending = FALSE, abs_sort = FALSE) {
      verify_string("order_by", order_by, FALSE)
      verify_bool("descending", descending, FALSE)
      verify_bool("abs_sort", abs_sort, FALSE)
      if ((length(descending) > 1) && length(descending) != length(order_by)) {
        stop(paste0("'descending' must be the same length as 'order_by' if more than one entry is supplied. Got 'order_by' with length ", length(order_by), " and 'descending' with length ", length(descending), "."))
      }
      if ((length(abs_sort) > 1) && length(abs_sort) != length(by)) {
        stop(paste0("'abs_sort' must be the same length as 'order_by' if more than one entry is supplied. Got 'order_by' with length ", length(order_by), " and 'abs_sort' with length ", length(abs_sort), "."))
      }
      return(TableHandle$new(self$.internal_rcpp_object$sort(order_by, descending, abs_sort)))
    }
  )
)

#' @export
head.TableHandle <- function(x, n = 1, ...) {
  return(x$head(n))
}

#' @export
tail.TableHandle <- function(x, n = 1, ...) {
  return(x$tail(n))
}

#' @export
nrow.TableHandle <- function(x) {
  return(x$nrow())
}

#' @export
ncol.TableHandle <- function(x) {
  return(x$ncol())
}

#' @export
dim.TableHandle <- function(x) {
  return(x$dim())
}

#' @export
merge_tables <- function(...) {
  table_list <- unlist(c(...))
  if (length(table_list) == 0) {
    return(NULL)
  }
  verify_type("table_list", table_list, "TableHandle", "Deephaven TableHandle", FALSE)
  if (length(table_list) == 1) {
    return(table_list[[1]])
  }
  return(table_list[[1]]$merge(table_list[2:length(table_list)]))
}

#' @export
as_record_batch_reader.TableHandle <- function(x, ...) {
  return(x$as_record_batch_reader())
}

#' @export
as_arrow_table.TableHandle <- function(x, ...) {
  return(x$as_arrow_table())
}

#' @export
as_tibble.TableHandle <- function(x, ...) {
  return(x$as_tibble())
}

#' @export
as.data.frame.TableHandle <- function(x, row.names = NULL, optional = FALSE, ...) {
  return(x$as_data_frame())
}
