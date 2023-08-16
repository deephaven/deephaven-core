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
  unwrapped_table_list <- lapply(table_list, strip_s4_wrapping)
  return(new("TableHandle", .internal_rcpp_object = unwrapped_table_list[[1]]$merge(unwrapped_table_list[2:length(unwrapped_table_list)])))
}

setGeneric(
  "select",
  function(table_handle, by = character(), ...) {
    return(standardGeneric("select"))
  },
  signature = c("table_handle", "by")
)

#' @export
setMethod(
  "select",
  signature = c(table_handle = "TableHandle"),
  function(table_handle, by = character()) {
    verify_string("by", by, FALSE)
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$select(by)))
  }
)

setGeneric(
  "view",
  function(table_handle, by = character(), ...) {
    return(standardGeneric("view"))
  },
  signature = c("table_handle", "by")
)

#' @export
setMethod(
  "view",
  signature = c(table_handle = "TableHandle"),
  function(table_handle, by = character()) {
    verify_string("by", by, FALSE)
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$view(by)))
  }
)

setGeneric(
  "update",
  function(table_handle, by = character(), ...) {
    standardGeneric("update")
  },
  signature = c("table_handle", "by")
)

#' @export
setMethod(
  "update",
  signature = c(table_handle = "TableHandle"),
  function(table_handle, by = character()) {
    verify_string("by", by, FALSE)
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$update(by)))
  }
)

setGeneric(
  "update_view",
  function(table_handle, by = character(), ...) {
    standardGeneric("update_view")
  },
  signature = c("table_handle", "by")
)

#' @export
setMethod(
  "update_view",
  signature = c(table_handle = "TableHandle"),
  function(table_handle, by = character()) {
    verify_string("by", by, FALSE)
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$update_view(by)))
  }
)

setGeneric(
  "drop_columns",
  function(table_handle, by = character(), ...) {
    standardGeneric("drop_columns")
  },
  signature = c("table_handle", "by")
)

#' @export
setMethod(
  "drop_columns",
  signature = c(table_handle = "TableHandle"),
  function(table_handle, by = character()) {
    verify_string("by", by, FALSE)
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$drop_columns(by)))
  }
)

setGeneric(
  "where",
  function(table_handle, filter, ...) {
    standardGeneric("where")
  },
  signature = c("table_handle", "filter")
)

#' @export
setMethod(
  "where",
  signature = c(table_handle = "TableHandle"),
  function(table_handle, filter) {
    verify_string("filter", filter, TRUE)
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$where(filter)))
  }
)

setGeneric(
  "group_by",
  function(table_handle, by = character(), ...) {
    standardGeneric("group_by")
  },
  signature = c("table_handle", "by")
)

#' @export
setMethod(
  "group_by",
  signature = c(table_handle = "TableHandle"),
  function(table_handle, by = character()) {
    verify_string("by", by, FALSE)
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$group_by(by)))
  }
)

setGeneric(
  "ungroup",
  function(table_handle, by = character(), ...) {
    standardGeneric("ungroup")
  },
  signature = c("table_handle", "by")
)

#' @export
setMethod(
  "ungroup",
  signature = c(table_handle = "TableHandle"),
  function(table_handle, by = character()) {
    verify_string("by", by, FALSE)
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$ungroup(by)))
  }
)

setGeneric(
  "agg_by",
  function(table_handle, aggs, by = character(), ...) {
    standardGeneric("agg_by")
  },
  signature = c("table_handle", "aggs", "by")
)

#' @export
setMethod(
  "agg_by",
  signature = c(table_handle = "TableHandle"),
  function(table_handle, aggs, by = character()) {
    verify_type("aggs", aggs, "Aggregation", "Deephaven Aggregation", FALSE)
    verify_string("by", by, FALSE)
    aggs <- c(aggs)
    unwrapped_aggs <- lapply(aggs, strip_s4_wrapping)
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$agg_by(unwrapped_aggs, by)))
  }
)

setGeneric(
  "first_by",
  function(table_handle, by = character(), ...) {
    standardGeneric("first_by")
  },
  signature = c("table_handle", "by")
)

#' @export
setMethod(
  "first_by",
  signature = c(table_handle = "TableHandle"),
  function(table_handle, by = character()) {
    verify_string("by", by, FALSE)
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$first_by(by)))
  }
)

setGeneric(
  "last_by",
  function(table_handle, by = character(), ...) {
    standardGeneric("last_by")
  },
  signature = c("table_handle", "by")
)

#' @export
setMethod(
  "last_by",
  signature = c(table_handle = "TableHandle"),
  function(table_handle, by = character()) {
    verify_string("by", by, FALSE)
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$last_by(by)))
  }
)

setGeneric(
  "head_by",
  function(table_handle, num_rows, by = character(), ...) {
    standardGeneric("head_by")
  },
  signature = c("table_handle", "num_rows", "by")
)

#' @export
setMethod(
  "head_by",
  signature = c(table_handle = "TableHandle"),
  function(table_handle, num_rows, by = character()) {
    verify_positive_int("num_rows", num_rows, TRUE)
    verify_string("by", by, FALSE)
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$head_by(num_rows, by)))
  }
)

setGeneric(
  "tail_by",
  function(table_handle, num_rows, by = character(), ...) {
    standardGeneric("tail_by")
  },
  signature = c("table_handle", "num_rows", "by")
)

#' @export
setMethod(
  "tail_by",
  signature = c(table_handle = "TableHandle"),
  function(table_handle, num_rows, by = character()) {
    verify_positive_int("num_rows", num_rows, TRUE)
    verify_string("by", by, FALSE)
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$tail_by(num_rows, by)))
  }
)

setGeneric(
  "min_by",
  function(table_handle, by = character(), ...) {
    standardGeneric("min_by")
  },
  signature = c("table_handle", "by")
)

#' @export
setMethod(
  "min_by",
  signature = c(table_handle = "TableHandle"),
  function(table_handle, by = character()) {
    verify_string("by", by, FALSE)
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$min_by(by)))
  }
)

setGeneric(
  "max_by",
  function(table_handle, by = character(), ...) {
    standardGeneric("max_by")
  },
  signature = c("table_handle", "by")
)

#' @export
setMethod(
  "max_by",
  signature = c(table_handle = "TableHandle"),
  function(table_handle, by = character()) {
    verify_string("by", by, FALSE)
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$max_by(by)))
  }
)

setGeneric(
  "sum_by",
  function(table_handle, by = character(), ...) {
    standardGeneric("sum_by")
  },
  signature = c("table_handle", "by")
)

#' @export
setMethod(
  "sum_by",
  signature = c(table_handle = "TableHandle"),
  function(table_handle, by = character()) {
    verify_string("by", by, FALSE)
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$sum_by(by)))
  }
)

setGeneric(
  "abs_sum_by",
  function(table_handle, by = character(), ...) {
    standardGeneric("abs_sum_by")
  },
  signature = c("table_handle", "by")
)

#' @export
setMethod(
  "abs_sum_by",
  signature = c(table_handle = "TableHandle"),
  function(table_handle, by = character()) {
    verify_string("by", by, FALSE)
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$abs_sum_by(by)))
  }
)

setGeneric(
  "avg_by",
  function(table_handle, by = character(), ...) {
    standardGeneric("avg_by")
  },
  signature = c("table_handle", "by")
)

#' @export
setMethod(
  "avg_by",
  signature = c(table_handle = "TableHandle"),
  function(table_handle, by = character()) {
    verify_string("by", by, FALSE)
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$avg_by(by)))
  }
)

setGeneric(
  "w_avg_by",
  function(table_handle, wcol, by = character(), ...) {
    standardGeneric("w_avg_by")
  },
  signature = c("table_handle", "wcol", "by")
)

#' @export
setMethod(
  "w_avg_by",
  signature = c(table_handle = "TableHandle"),
  function(table_handle, wcol, by = character()) {
    verify_string("wcol", wcol, TRUE)
    verify_string("by", by, FALSE)
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$w_avg_by(wcol, by)))
  }
)

setGeneric(
  "median_by",
  function(table_handle, by = character(), ...) {
    standardGeneric("median_by")
  },
  signature = c("table_handle", "by")
)

#' @export
setMethod(
  "median_by",
  signature = c(table_handle = "TableHandle"),
  function(table_handle, by = character()) {
    verify_string("by", by, FALSE)
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$median_by(by)))
  }
)

setGeneric(
  "var_by",
  function(table_handle, by = character(), ...) {
    standardGeneric("var_by")
  },
  signature = c("table_handle", "by")
)

#' @export
setMethod(
  "var_by",
  signature = c(table_handle = "TableHandle"),
  function(table_handle, by = character()) {
    verify_string("by", by, FALSE)
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$var_by(by)))
  }
)

setGeneric(
  "std_by",
  function(table_handle, by = character(), ...) {
    standardGeneric("std_by")
  },
  signature = c("table_handle", "by")
)

#' @export
setMethod(
  "std_by",
  signature = c(table_handle = "TableHandle"),
  function(table_handle, by = character()) {
    verify_string("by", by, FALSE)
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$std_by(by)))
  }
)

setGeneric(
  "percentile_by",
  function(table_handle, percentile, by = character(), ...) {
    standardGeneric("percentile_by")
  },
  signature = c("table_handle", "percentile", "by")
)

#' @export
setMethod(
  "percentile_by",
  signature = c(table_handle = "TableHandle"),
  function(table_handle, percentile, by = character()) {
    verify_in_unit_interval("percentile", percentile, TRUE)
    verify_string("by", by, FALSE)
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$percentile_by(percentile, by)))
  }
)

setGeneric(
  "count_by",
  function(table_handle, col, by = character(), ...) {
    standardGeneric("count_by")
  },
  signature = c("table_handle", "col", "by")
)

#' @export
setMethod(
  "count_by",
  signature = c(table_handle = "TableHandle"),
  function(table_handle, col = "n", by = character()) {
    verify_string("col", col, TRUE)
    verify_string("by", by, FALSE)
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$count_by(col, by)))
  }
)

setGeneric(
  "cross_join",
  function(table_handle, right_side, columns_to_match, columns_to_add, ...) {
    standardGeneric("cross_join")
  },
  signature = c("table_handle", "right_side", "columns_to_match", "columns_to_add")
)

#' @export
setMethod(
  "cross_join",
  signature = c(table_handle = "TableHandle", right_side = "TableHandle"),
  function(table_handle, right_side, columns_to_match = character(), columns_to_add = character()) {
    verify_string("columns_to_match", columns_to_match, FALSE)
    verify_string("columns_to_add", columns_to_add, FALSE)
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$cross_join(
      right_side@.internal_rcpp_object,
      columns_to_match, columns_to_add
    )))
  }
)

setGeneric(
  "natural_join",
  function(table_handle, right_side, columns_to_match, columns_to_add, ...) {
    standardGeneric("natural_join")
  },
  signature = c("table_handle", "right_side", "columns_to_match", "columns_to_add")
)

#' @export
setMethod(
  "natural_join",
  signature = c(table_handle = "TableHandle", right_side = "TableHandle"),
  function(table_handle, right_side, columns_to_match = character(), columns_to_add = character()) {
    verify_string("columns_to_match", columns_to_match, FALSE)
    verify_string("columns_to_add", columns_to_add, FALSE)
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$natural_join(
      right_side@.internal_rcpp_object,
      columns_to_match, columns_to_add
    )))
  }
)

setGeneric(
  "exact_join",
  function(table_handle, right_side, columns_to_match, columns_to_add, ...) {
    standardGeneric("exact_join")
  },
  signature = c("table_handle", "right_side", "columns_to_match", "columns_to_add")
)

#' @export
setMethod(
  "exact_join",
  signature = c(table_handle = "TableHandle", right_side = "TableHandle"),
  function(table_handle, right_side, columns_to_match = character(), columns_to_add = character()) {
    verify_string("columns_to_match", columns_to_match, FALSE)
    verify_string("columns_to_add", columns_to_add, FALSE)
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$exact_join(
      right_side@.internal_rcpp_object,
      columns_to_match, columns_to_add
    )))
  }
)

#' @export
setGeneric(
  "sort",
  function(table_handle, by = character(), descending = FALSE, abs_sort = FALSE, ...) {
    standardGeneric("sort")
  },
  signature = c("table_handle", "by", "descending")
)

#' @export
setMethod(
  "sort",
  signature = c(table_handle = "TableHandle"),
  function(table_handle, by, descending = FALSE, abs_sort = FALSE) {
    verify_string("by", by, FALSE)
    verify_bool("descending", descending, FALSE)
    verify_bool("abs_sort", abs_sort, FALSE)
    if ((length(descending) > 1) && length(descending) != length(by)) {
      stop(paste0("'descending' must be the same length as 'by' if more than one entry is supplied. Got 'by' with length ", length(by), " and 'descending' with length ", length(descending), "."))
    }
    if ((length(abs_sort) > 1) && length(abs_sort) != length(by)) {
      stop(paste0("'abs_sort' must be the same length as 'by' if more than one entry is supplied. Got 'by' with length ", length(by), " and 'abs_sort' with length ", length(abs_sort), "."))
    }
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$sort(by, descending, abs_sort)))
  }
)
