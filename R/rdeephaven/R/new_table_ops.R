setGeneric(
  "select",
  function(table_handle, columns = character(), ...) {
    return(standardGeneric("select"))
  },
  signature = c("table_handle", "columns")
)

#' @export
setMethod(
  "select",
  signature = c(table_handle = "TableHandle"),
  function(table_handle, columns = character()) {
    verify_string("columns", columns, FALSE)
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$select(columns)))
  }
)

setGeneric(
  "view",
  function(table_handle, columns = character(), ...) {
    return(standardGeneric("view"))
  },
  signature = c("table_handle", "columns")
)

#' @export
setMethod(
  "view",
  signature = c(table_handle = "TableHandle"),
  function(table_handle, columns = character()) {
    verify_string("columns", columns, FALSE)
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$view(columns)))
  }
)

setGeneric(
  "update",
  function(table_handle, columns = character(), ...) {
    standardGeneric("update")
  },
  signature = c("table_handle", "columns")
)

#' @export
setMethod(
  "update",
  signature = c(table_handle = "TableHandle"),
  function(table_handle, columns = character()) {
    verify_string("columns", columns, FALSE)
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$update(columns)))
  }
)

setGeneric(
  "update_view",
  function(table_handle, columns = character(), ...) {
    standardGeneric("update_view")
  },
  signature = c("table_handle", "columns")
)

#' @export
setMethod(
  "update_view",
  signature = c(table_handle = "TableHandle"),
  function(table_handle, columns = character()) {
    verify_string("columns", columns, FALSE)
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$update_view(columns)))
  }
)

setGeneric(
  "drop_columns",
  function(table_handle, columns = character(), ...) {
    standardGeneric("drop_columns")
  },
  signature = c("table_handle", "columns")
)

#' @export
setMethod(
  "drop_columns",
  signature = c(table_handle = "TableHandle"),
  function(table_handle, columns = character()) {
    verify_string("columns", columns, FALSE)
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$drop_columns(columns)))
  }
)

setGeneric(
  "where",
  function(table_handle, condition, ...) {
    standardGeneric("where")
  },
  signature = c("table_handle", "condition")
)

#' @export
setMethod(
  "where",
  signature = c(table_handle = "TableHandle"),
  function(table_handle, condition) {
    verify_string("condition", condition, TRUE)
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$where(condition)))
  }
)

setGeneric(
  "group_by",
  function(table_handle, columns = character(), ...) {
    standardGeneric("group_by")
  },
  signature = c("table_handle", "columns")
)

#' @export
setMethod(
  "group_by",
  signature = c(table_handle = "TableHandle"),
  function(table_handle, columns = character()) {
    verify_string("columns", columns, FALSE)
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$group_by(columns)))
  }
)

setGeneric(
  "ungroup",
  function(table_handle, columns = character(), ...) {
    standardGeneric("ungroup")
  },
  signature = c("table_handle", "columns")
)

#' @export
setMethod(
  "ungroup",
  signature = c(table_handle = "TableHandle"),
  function(table_handle, columns = character()) {
    verify_string("columns", columns, FALSE)
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$ungroup(columns)))
  }
)

setGeneric(
  "agg_by",
  function(table_handle, aggregations, columns = character(), ...) {
    standardGeneric("agg_by")
  },
  signature = c("table_handle", "aggregations", "columns")
)

#' @export
setMethod(
  "agg_by",
  signature = c(table_handle = "TableHandle"),
  function(table_handle, aggregations, columns = character()) {
    verify_type("aggregations", aggregations, "Aggregation", "Deephaven Aggregation", FALSE)
    verify_string("columns", columns, FALSE)
    aggregations <- c(aggregations)
    unwrapped_aggregations <- lapply(aggregations, strip_s4_wrapping)
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$agg_by(unwrapped_aggregations, columns)))
  }
)

setGeneric(
  "first_by",
  function(table_handle, columns = character(), ...) {
    standardGeneric("first_by")
  },
  signature = c("table_handle", "columns")
)

#' @export
setMethod(
  "first_by",
  signature = c(table_handle = "TableHandle"),
  function(table_handle, columns = character()) {
    verify_string("columns", columns, FALSE)
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$first_by(columns)))
  }
)

setGeneric(
  "last_by",
  function(table_handle, columns = character(), ...) {
    standardGeneric("last_by")
  },
  signature = c("table_handle", "columns")
)

#' @export
setMethod(
  "last_by",
  signature = c(table_handle = "TableHandle"),
  function(table_handle, columns = character()) {
    verify_string("columns", columns, FALSE)
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$last_by(columns)))
  }
)

setGeneric(
  "head_by",
  function(table_handle, n, columns = character(), ...) {
    standardGeneric("head_by")
  },
  signature = c("table_handle", "n", "columns")
)

#' @export
setMethod(
  "head_by",
  signature = c(table_handle = "TableHandle"),
  function(table_handle, n, columns = character()) {
    verify_positive_int("n", n, TRUE)
    verify_string("columns", columns, FALSE)
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$head_by(n, columns)))
  }
)

setGeneric(
  "tail_by",
  function(table_handle, n, columns = character(), ...) {
    standardGeneric("tail_by")
  },
  signature = c("table_handle", "n", "columns")
)

#' @export
setMethod(
  "tail_by",
  signature = c(table_handle = "TableHandle"),
  function(table_handle, n, columns = character()) {
    verify_positive_int("n", n, TRUE)
    verify_string("columns", columns, FALSE)
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$tail_by(n, columns)))
  }
)

setGeneric(
  "min_by",
  function(table_handle, columns = character(), ...) {
    standardGeneric("min_by")
  },
  signature = c("table_handle", "columns")
)

#' @export
setMethod(
  "min_by",
  signature = c(table_handle = "TableHandle"),
  function(table_handle, columns = character()) {
    verify_string("columns", columns, FALSE)
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$min_by(columns)))
  }
)

setGeneric(
  "max_by",
  function(table_handle, columns = character(), ...) {
    standardGeneric("max_by")
  },
  signature = c("table_handle", "columns")
)

#' @export
setMethod(
  "max_by",
  signature = c(table_handle = "TableHandle"),
  function(table_handle, columns = character()) {
    verify_string("columns", columns, FALSE)
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$max_by(columns)))
  }
)

setGeneric(
  "sum_by",
  function(table_handle, columns = character(), ...) {
    standardGeneric("sum_by")
  },
  signature = c("table_handle", "columns")
)

#' @export
setMethod(
  "sum_by",
  signature = c(table_handle = "TableHandle"),
  function(table_handle, columns = character()) {
    verify_string("columns", columns, FALSE)
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$sum_by(columns)))
  }
)

setGeneric(
  "abs_sum_by",
  function(table_handle, columns = character(), ...) {
    standardGeneric("abs_sum_by")
  },
  signature = c("table_handle", "columns")
)

#' @export
setMethod(
  "abs_sum_by",
  signature = c(table_handle = "TableHandle"),
  function(table_handle, columns = character()) {
    verify_string("columns", columns, FALSE)
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$abs_sum_by(columns)))
  }
)

setGeneric(
  "avg_by",
  function(table_handle, columns = character(), ...) {
    standardGeneric("avg_by")
  },
  signature = c("table_handle", "columns")
)

#' @export
setMethod(
  "avg_by",
  signature = c(table_handle = "TableHandle"),
  function(table_handle, columns = character()) {
    verify_string("columns", columns, FALSE)
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$avg_by(columns)))
  }
)

setGeneric(
  "w_avg_by",
  function(table_handle, weight_column, columns = character(), ...) {
    standardGeneric("w_avg_by")
  },
  signature = c("table_handle", "weight_column", "columns")
)

#' @export
setMethod(
  "w_avg_by",
  signature = c(table_handle = "TableHandle"),
  function(table_handle, weight_column, columns = character()) {
    verify_string("weight_column", weight_column, TRUE)
    verify_string("columns", columns, FALSE)
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$w_avg_by(weight_column, columns)))
  }
)

setGeneric(
  "median_by",
  function(table_handle, columns = character(), ...) {
    standardGeneric("median_by")
  },
  signature = c("table_handle", "columns")
)

#' @export
setMethod(
  "median_by",
  signature = c(table_handle = "TableHandle"),
  function(table_handle, columns = character()) {
    verify_string("columns", columns, FALSE)
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$median_by(columns)))
  }
)

setGeneric(
  "var_by",
  function(table_handle, columns = character(), ...) {
    standardGeneric("var_by")
  },
  signature = c("table_handle", "columns")
)

#' @export
setMethod(
  "var_by",
  signature = c(table_handle = "TableHandle"),
  function(table_handle, columns = character()) {
    verify_string("columns", columns, FALSE)
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$var_by(columns)))
  }
)

setGeneric(
  "std_by",
  function(table_handle, columns = character(), ...) {
    standardGeneric("std_by")
  },
  signature = c("table_handle", "columns")
)

#' @export
setMethod(
  "std_by",
  signature = c(table_handle = "TableHandle"),
  function(table_handle, columns = character()) {
    verify_string("columns", columns, FALSE)
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$std_by(columns)))
  }
)

setGeneric(
  "percentile_by",
  function(table_handle, percentile, columns = character(), ...) {
    standardGeneric("percentile_by")
  },
  signature = c("table_handle", "percentile", "columns")
)

#' @export
setMethod(
  "percentile_by",
  signature = c(table_handle = "TableHandle"),
  function(table_handle, percentile, columns = character()) {
    verify_in_unit_interval("percentile", percentile, TRUE)
    verify_string("columns", columns, FALSE)
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$percentile_by(percentile, columns)))
  }
)

setGeneric(
  "count_by",
  function(table_handle, count_by_column, columns = character(), ...) {
    standardGeneric("count_by")
  },
  signature = c("table_handle", "count_by_column", "columns")
)

#' @export
setMethod(
  "count_by",
  signature = c(table_handle = "TableHandle"),
  function(table_handle, count_by_column = "n", columns = character()) {
    verify_string("count_by_column", count_by_column, TRUE)
    verify_string("columns", columns, FALSE)
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$count_by(count_by_column, columns)))
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
  function(table_handle, columns = character(), descending = FALSE, ...) {
    standardGeneric("sort")
  },
  signature = c("table_handle", "columns", "descending")
)

#' @export
setMethod(
  "sort",
  signature = c(table_handle = "TableHandle"),
  function(table_handle, columns, descending = FALSE) {
    verify_string("columns", columns, FALSE)
    verify_bool("descending", descending, FALSE)
    if ((length(descending) > 1) && length(descending) != length(columns)) {
      stop(paste0("'descending' must be the same length as 'columns' if more than one entry is supplied. Got 'columns' with length ", length(columns), " and 'descending' with length", length(descending), " instead."))
    }
    return(new("TableHandle", .internal_rcpp_object = table_handle@.internal_rcpp_object$sort(columns, descending)))
  }
)

