#' @name AggOp
#' @title Deephaven AggOps
#' @md
#' @description
#' An `AggOp` is the return type of one of Deephaven's `agg` functions. It is a function that performs the
#' computation specified by the `agg` function. These are intended to be passed directly to `agg_by()` or `agg_all_by()`,
#' and should never be instantiated directly be user code. For more information, see the
#' vignette on `agg` functions with `vignette("agg_by")`.
#'
#' If multiple tables have the same schema and the same aggregations need to be applied to each table, saving these
#' objects directly in a variable may be useful to avoid having to re-create them each time:
#' ```
#' aggregations <- c(agg_min("XMin = X", "YMin = Y"),
#'                   agg_max("XMax = X", "YMax = Y"))
#'
#' result1 <- th1$agg_by(aggregations, by="Group")
#' result2 <- th2$agg_by(aggregations, by="Group")
#' ```
#' In this example, `aggregations` would be a vector of two AggOps that can be reused in multiple calls to `agg_by()`.
#'
#' @usage NULL
#' @format NULL
#' @docType class
#' @export
AggOp <- R6Class("AggOp",
  cloneable = FALSE,
  public = list(
    .internal_rcpp_object = NULL,
    .internal_num_cols = NULL,
    .internal_agg_name = NULL,
    initialize = function(aggregation, agg_name, ...) {
      self$.internal_agg_name <- agg_name
      args <- list(...)
      if (any(names(args) == "cols")) {
        self$.internal_num_cols <- length(args$cols)
      }
      self$.internal_rcpp_object <- do.call(aggregation, args)
    }
  )
)


#' @name
#' agg_first
#' @title
#' First element of specified columns by group
#' @md
#'
#' @description
#' Creates a First aggregation that computes the first value of each column in `cols` for each aggregation group.
#'
#' @details
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `agg_by()` or
#' `agg_all_by()` caller function. The aggregation groups are defined by the unique combinations of values in the `by`
#' columns. For example, if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of
#' values in the `A` and `B` columns.
#'
#' This function, like other Deephaven `agg` functions, is a generator function. That is, its output is another
#' function called an [`AggOp`][AggOp] intended to be used in a call to `agg_by()` or `agg_all_by()`. This detail is
#' typically hidden from the user. However, it is important to understand this detail for debugging purposes,
#' as the output of an `agg` function can otherwise seem unexpected.
#' 
#' For more information, see the vignette on `agg` functions by running
#' `vignette("agg_by")`.
#'
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to aggregate all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return [`AggOp`][AggOp] to be used in a call to `agg_by()` or `agg_all_by()`.
#'
#' @examples
#' \dontrun{
#' library(rdeephaven)
#'
#' # connecting to Deephaven server
#' client <- Client$new("localhost:10000", auth_type = "psk", auth_token = "my_secret_token")
#'
#' # create data frame, push to server, retrieve TableHandle
#' df <- data.frame(
#'   X = c("A", "B", "A", "C", "B", "A", "B", "B", "C"),
#'   Y = c("M", "N", "O", "N", "P", "M", "O", "P", "M"),
#'   Number1 = c(100, -44, 49, 11, -66, 50, 29, 18, -70),
#'   Number2 = c(-55, 76, 20, 130, 230, -50, 73, 137, 214)
#' )
#' th <- client$import_table(df)
#'
#' # get first elements of all columns
#' th1 <- th$
#'   agg_by(agg_first(c("X", "Y", "Number1", "Number2")))
#'
#' # get first elements of Y, Number1, and Number2 grouped by X
#' th2 <- th$
#'   agg_by(agg_first(c("Y", "Number1", "Number2")), by = "X")
#'
#' # get first elements of Number1 and Number2 grouped by X and Y
#' th3 <- th
#' agg_by(agg_first(c("Number1", "Number2")), by = c("X", "Y"))
#'
#' client$close()
#' }
#'
#' @export
agg_first <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(AggOp$new(INTERNAL_agg_first, "agg_first", cols = cols))
}

#' @name
#' agg_last
#' @title
#' Last element of specified columns by group
#' @md
#'
#' @description
#' Creates a Last aggregation that computes the last value of each column in `cols` for each aggregation group.
#'
#' @details
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `agg_by()` or
#' `agg_all_by()` caller function. The aggregation groups are defined by the unique combinations of values in the `by`
#' columns. For example, if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of
#' values in the `A` and `B` columns.
#'
#' This function, like other Deephaven `agg` functions, is a generator function. That is, its output is another
#' function called an [`AggOp`][AggOp] intended to be used in a call to `agg_by()` or `agg_all_by()`. This detail is
#' typically hidden from the user. However, it is important to understand this detail for debugging purposes,
#' as the output of an `agg` function can otherwise seem unexpected.
#' 
#' For more information, see the vignette on `agg` functions by running
#' `vignette("agg_by")`.
#'
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to aggregate all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return [`AggOp`][AggOp] to be used in a call to `agg_by()` or `agg_all_by()`.
#'
#' @examples
#' \dontrun{
#' library(rdeephaven)
#'
#' # connecting to Deephaven server
#' client <- Client$new("localhost:10000", auth_type = "psk", auth_token = "my_secret_token")
#'
#' # create data frame, push to server, retrieve TableHandle
#' df <- data.frame(
#'   X = c("A", "B", "A", "C", "B", "A", "B", "B", "C"),
#'   Y = c("M", "N", "O", "N", "P", "M", "O", "P", "M"),
#'   Number1 = c(100, -44, 49, 11, -66, 50, 29, 18, -70),
#'   Number2 = c(-55, 76, 20, 130, 230, -50, 73, 137, 214)
#' )
#' th <- client$import_table(df)
#'
#' # get last elements of all columns
#' th1 <- th$
#'   agg_by(agg_last(c("X", "Y", "Number1", "Number2")))
#'
#' # get last elements of Y, Number1, and Number2 grouped by X
#' th2 <- th$
#'   agg_by(agg_last(c("Y", "Number1", "Number2")), by = "X")
#'
#' # get last elements of Number1 and Number2 grouped by X and Y
#' th3 <- th$
#'   agg_by(agg_last(c("Number1", "Number2")), by = c("X", "Y"))
#'
#' client$close()
#' }
#'
#' @export
agg_last <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(AggOp$new(INTERNAL_agg_last, "agg_last", cols = cols))
}

#' @name
#' agg_min
#' @title
#' Minimum of specified columns by group
#' @md
#'
#' @description
#' Creates a Minimum aggregation that computes the minimum of each column in `cols` for each aggregation group.
#'
#' @details
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `agg_by()` or
#' `agg_all_by()` caller function. The aggregation groups are defined by the unique combinations of values in the `by`
#' columns. For example, if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of
#' values in the `A` and `B` columns.
#'
#' This function, like other Deephaven `agg` functions, is a generator function. That is, its output is another
#' function called an [`AggOp`][AggOp] intended to be used in a call to `agg_by()` or `agg_all_by()`. This detail is
#' typically hidden from the user. However, it is important to understand this detail for debugging purposes,
#' as the output of an `agg` function can otherwise seem unexpected.
#' 
#' For more information, see the vignette on `agg` functions by running
#' `vignette("agg_by")`.
#'
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to aggregate all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return [`AggOp`][AggOp] to be used in a call to `agg_by()` or `agg_all_by()`.
#'
#' @examples
#' \dontrun{
#' library(rdeephaven)
#'
#' # connecting to Deephaven server
#' client <- Client$new("localhost:10000", auth_type = "psk", auth_token = "my_secret_token")
#'
#' # create data frame, push to server, retrieve TableHandle
#' df <- data.frame(
#'   X = c("A", "B", "A", "C", "B", "A", "B", "B", "C"),
#'   Y = c("M", "N", "O", "N", "P", "M", "O", "P", "M"),
#'   Number1 = c(100, -44, 49, 11, -66, 50, 29, 18, -70),
#'   Number2 = c(-55, 76, 20, 130, 230, -50, 73, 137, 214)
#' )
#' th <- client$import_table(df)
#'
#' # get minimum elements of Number1 and Number2
#' th1 <- th$
#'   agg_by(agg_min(c("Number1", "Number2")))
#'
#' # get minimum elements of Number1 and Number2 grouped by X
#' th2 <- th$
#'   agg_by(agg_min(c("Number1", "Number2")), by = "X")
#'
#' # get minimum elements of Number1 and Number2 grouped by X and Y
#' th3 <- th$
#'   agg_by(agg_min(c("Number1", "Number2")), by = c("X", "Y"))
#'
#' client$close()
#' }
#'
#' @export
agg_min <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(AggOp$new(INTERNAL_agg_min, "agg_min", cols = cols))
}

#' @name
#' agg_max
#' @title
#' Maximum of specified columns by group
#' @md
#'
#' @description
#' Creates a Maximum aggregation that computes the maximum of each column in `cols` for each aggregation group.
#'
#' @details
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `agg_by()` or
#' `agg_all_by()` caller function. The aggregation groups are defined by the unique combinations of values in the `by`
#' columns. For example, if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of
#' values in the `A` and `B` columns.
#'
#' This function, like other Deephaven `agg` functions, is a generator function. That is, its output is another
#' function called an [`AggOp`][AggOp] intended to be used in a call to `agg_by()` or `agg_all_by()`. This detail is
#' typically hidden from the user. However, it is important to understand this detail for debugging purposes,
#' as the output of an `agg` function can otherwise seem unexpected.
#' 
#' For more information, see the vignette on `agg` functions by running
#' `vignette("agg_by")`.
#'
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to aggregate all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return [`AggOp`][AggOp] to be used in a call to `agg_by()` or `agg_all_by()`.
#'
#' @examples
#' \dontrun{
#' library(rdeephaven)
#'
#' # connecting to Deephaven server
#' client <- Client$new("localhost:10000", auth_type = "psk", auth_token = "my_secret_token")
#'
#' # create data frame, push to server, retrieve TableHandle
#' df <- data.frame(
#'   X = c("A", "B", "A", "C", "B", "A", "B", "B", "C"),
#'   Y = c("M", "N", "O", "N", "P", "M", "O", "P", "M"),
#'   Number1 = c(100, -44, 49, 11, -66, 50, 29, 18, -70),
#'   Number2 = c(-55, 76, 20, 130, 230, -50, 73, 137, 214)
#' )
#' th <- client$import_table(df)
#'
#' # get maximum elements of Number1 and Number2
#' th1 <- th$
#'   agg_by(agg_max(c("Number1", "Number2")))
#'
#' # get maximum elements of Number1 and Number2 grouped by X
#' th2 <- th$
#'   agg_by(agg_max(c("Number1", "Number2")), by = "X")
#'
#' # get maximum elements of Number1 and Number2 grouped by X and Y
#' th3 <- th$
#'   agg_by(agg_max(c("Number1", "Number2")), by = c("X", "Y"))
#'
#' client$close()
#' }
#'
#' @export
agg_max <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(AggOp$new(INTERNAL_agg_max, "agg_max", cols = cols))
}

#' @name
#' agg_sum
#' @title
#' Sum element of specified columns by group
#' @md
#'
#' @description
#' Creates a Sum aggregation that computes the sum of each column in `cols` for each aggregation group.
#'
#' @details
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `agg_by()` or
#' `agg_all_by()` caller function. The aggregation groups are defined by the unique combinations of values in the `by`
#' columns. For example, if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of
#' values in the `A` and `B` columns.
#'
#' This function, like other Deephaven `agg` functions, is a generator function. That is, its output is another
#' function called an [`AggOp`][AggOp] intended to be used in a call to `agg_by()` or `agg_all_by()`. This detail is
#' typically hidden from the user. However, it is important to understand this detail for debugging purposes,
#' as the output of an `agg` function can otherwise seem unexpected.
#'
#' For more information, see the vignette on `agg` functions by running
#' `vignette("agg_by")`.
#'
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to aggregate all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return [`AggOp`][AggOp] to be used in a call to `agg_by()` or `agg_all_by()`.
#'
#' @examples
#' \dontrun{
#' library(rdeephaven)
#'
#' # connecting to Deephaven server
#' client <- Client$new("localhost:10000", auth_type = "psk", auth_token = "my_secret_token")
#'
#' # create data frame, push to server, retrieve TableHandle
#' df <- data.frame(
#'   X = c("A", "B", "A", "C", "B", "A", "B", "B", "C"),
#'   Y = c("M", "N", "O", "N", "P", "M", "O", "P", "M"),
#'   Number1 = c(100, -44, 49, 11, -66, 50, 29, 18, -70),
#'   Number2 = c(-55, 76, 20, 130, 230, -50, 73, 137, 214)
#' )
#' th <- client$import_table(df)
#'
#' # compute sum of Number1 and Number2
#' th1 <- th$
#'   agg_by(agg_sum(c("Number1", "Number2")))
#'
#' # compute sum of Number1 and Number2 grouped by X
#' th2 <- th$
#'   agg_by(agg_sum(c("Number1", "Number2")), by = "X")
#'
#' # compute sum of Number1 and Number2 grouped by X and Y
#' th3 <- th$
#'   agg_by(agg_sum(c("Number1", "Number2")), by = c("X", "Y"))
#'
#' client$close()
#' }
#'
#' @export
agg_sum <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(AggOp$new(INTERNAL_agg_sum, "agg_sum", cols = cols))
}

#' @name
#' agg_abs_sum
#' @title
#' Absolute sum of specified columns by group
#' @md
#'
#' @description
#' Creates an Absolute Sum aggregation that computes the absolute sum of each column in `cols` for each aggregation group.
#'
#' @details
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `agg_by()` or
#' `agg_all_by()` caller function. The aggregation groups are defined by the unique combinations of values in the `by`
#' columns. For example, if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of
#' values in the `A` and `B` columns.
#'
#' This function, like other Deephaven `agg` functions, is a generator function. That is, its output is another
#' function called an [`AggOp`][AggOp] intended to be used in a call to `agg_by()` or `agg_all_by()`. This detail is
#' typically hidden from the user. However, it is important to understand this detail for debugging purposes,
#' as the output of an `agg` function can otherwise seem unexpected.
#' 
#' For more information, see the vignette on `agg` functions by running
#' `vignette("agg_by")`.
#'
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to aggregate all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return [`AggOp`][AggOp] to be used in a call to `agg_by()` or `agg_all_by()`.
#'
#' @examples
#' \dontrun{
#' library(rdeephaven)
#'
#' # connecting to Deephaven server
#' client <- Client$new("localhost:10000", auth_type = "psk", auth_token = "my_secret_token")
#'
#' # create data frame, push to server, retrieve TableHandle
#' df <- data.frame(
#'   X = c("A", "B", "A", "C", "B", "A", "B", "B", "C"),
#'   Y = c("M", "N", "O", "N", "P", "M", "O", "P", "M"),
#'   Number1 = c(100, -44, 49, 11, -66, 50, 29, 18, -70),
#'   Number2 = c(-55, 76, 20, 130, 230, -50, 73, 137, 214)
#' )
#' th <- client$import_table(df)
#'
#' # compute absolute sum of Number1 and Number2
#' th1 <- th$
#'   agg_by(agg_abs_sum(c("Number1", "Number2")))
#'
#' # compute absolute sum of Number1 and Number2 grouped by X
#' th2 <- th$
#'   agg_by(agg_abs_sum(c("Number1", "Number2")), by = "X")
#'
#' # compute absolute sum of Number1 and Number2 grouped by X and Y
#' th3 <- th$
#'   agg_by(agg_abs_sum(c("Number1", "Number2")), by = c("X", "Y"))
#'
#' client$close()
#' }
#'
#' @export
agg_abs_sum <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(AggOp$new(INTERNAL_agg_abs_sum, "agg_abs_sum", cols = cols))
}

#' @name
#' agg_avg
#' @title
#' Average of specified columns by group
#' @md
#'
#' @description
#' Creates an Average aggregation that computes the average of each column in `cols` for each aggregation group.
#'
#' @details
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `agg_by()` or
#' `agg_all_by()` caller function. The aggregation groups are defined by the unique combinations of values in the `by`
#' columns. For example, if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of
#' values in the `A` and `B` columns.
#'
#' This function, like other Deephaven `agg` functions, is a generator function. That is, its output is another
#' function called an [`AggOp`][AggOp] intended to be used in a call to `agg_by()` or `agg_all_by()`. This detail is
#' typically hidden from the user. However, it is important to understand this detail for debugging purposes,
#' as the output of an `agg` function can otherwise seem unexpected.
#' 
#' For more information, see the vignette on `agg` functions by running
#' `vignette("agg_by")`.
#'
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to aggregate all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return [`AggOp`][AggOp] to be used in a call to `agg_by()` or `agg_all_by()`.
#'
#' @examples
#' \dontrun{
#' library(rdeephaven)
#'
#' # connecting to Deephaven server
#' client <- Client$new("localhost:10000", auth_type = "psk", auth_token = "my_secret_token")
#'
#' # create data frame, push to server, retrieve TableHandle
#' df <- data.frame(
#'   X = c("A", "B", "A", "C", "B", "A", "B", "B", "C"),
#'   Y = c("M", "N", "O", "N", "P", "M", "O", "P", "M"),
#'   Number1 = c(100, -44, 49, 11, -66, 50, 29, 18, -70),
#'   Number2 = c(-55, 76, 20, 130, 230, -50, 73, 137, 214)
#' )
#' th <- client$import_table(df)
#'
#' # compute average of Number1 and Number2
#' th1 <- th$
#'   agg_by(agg_avg(c("Number1", "Number2")))
#'
#' # compute average of Number1 and Number2 grouped by X
#' th2 <- th$
#'   agg_by(agg_avg(c("Number1", "Number2")), by = "X")
#'
#' # compute average of Number1 and Number2 grouped by X and Y
#' th3 <- th$
#'   agg_by(agg_avg(c("Number1", "Number2")), by = c("X", "Y"))
#'
#' client$close()
#' }
#'
#' @export
agg_avg <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(AggOp$new(INTERNAL_agg_avg, "agg_avg", cols = cols))
}

#' @name
#' agg_w_avg
#' @title
#' Weighted average of specified columns by group
#' @md
#'
#' @description
#' Creates a Weighted Average aggregation that computes the weighted average of each column in `cols` for each aggregation group.
#'
#' @details
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `agg_by()` or
#' `agg_all_by()` caller function. The aggregation groups are defined by the unique combinations of values in the `by`
#' columns. For example, if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of
#' values in the `A` and `B` columns.
#'
#' This function, like other Deephaven `agg` functions, is a generator function. That is, its output is another
#' function called an [`AggOp`][AggOp] intended to be used in a call to `agg_by()` or `agg_all_by()`. This detail is
#' typically hidden from the user. However, it is important to understand this detail for debugging purposes,
#' as the output of an `agg` function can otherwise seem unexpected.
#' 
#' For more information, see the vignette on `agg` functions by running
#' `vignette("agg_by")`.
#'
#' @param wcol String denoting the column to use for weights. This must be a numeric column.
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to aggregate all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return [`AggOp`][AggOp] to be used in a call to `agg_by()` or `agg_all_by()`.
#'
#' @examples
#' \dontrun{
#' library(rdeephaven)
#'
#' # connecting to Deephaven server
#' client <- Client$new("localhost:10000", auth_type = "psk", auth_token = "my_secret_token")
#'
#' # create data frame, push to server, retrieve TableHandle
#' df <- data.frame(
#'   X = c("A", "B", "A", "C", "B", "A", "B", "B", "C"),
#'   Y = c("M", "N", "O", "N", "P", "M", "O", "P", "M"),
#'   Number1 = c(100, -44, 49, 11, -66, 50, 29, 18, -70),
#'   Number2 = c(-55, 76, 20, 130, 230, -50, 73, 137, 214)
#' )
#' th <- client$import_table(df)
#'
#' # compute weighted average of Number1, weighted by Number2
#' th1 <- th$
#'   agg_by(agg_w_avg(wcol = "Number2", cols = "Number1"))
#'
#' # compute weighted average of Number1, weighted by Number2, grouped by X
#' th2 <- th$
#'   agg_by(agg_w_avg(wcol = "Number2", cols = "Number1", by = "X"))
#'
#' # compute weighted average of Number1, weighted by Number2, grouped by X and Y
#' th3 <- th$
#'   agg_by(agg_w_avg(wcol = "Number2", cols = "Number1", by = c("X", "Y")))
#'
#' client$close()
#' }
#'
#' @export
agg_w_avg <- function(wcol, cols = character()) {
  verify_string("wcol", wcol, TRUE)
  verify_string("cols", cols, FALSE)
  return(AggOp$new(INTERNAL_agg_w_avg, "agg_w_avg", wcol = wcol, cols = cols))
}

#' @name
#' agg_median
#' @title
#' Median of specified columns by group
#' @md
#'
#' @description
#' Creates a Median aggregation that computes the median of each column in `cols` for each aggregation group.
#'
#' @details
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `agg_by()` or
#' `agg_all_by()` caller function. The aggregation groups are defined by the unique combinations of values in the `by`
#' columns. For example, if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of
#' values in the `A` and `B` columns.
#'
#' This function, like other Deephaven `agg` functions, is a generator function. That is, its output is another
#' function called an [`AggOp`][AggOp] intended to be used in a call to `agg_by()` or `agg_all_by()`. This detail is
#' typically hidden from the user. However, it is important to understand this detail for debugging purposes,
#' as the output of an `agg` function can otherwise seem unexpected.
#' 
#' For more information, see the vignette on `agg` functions by running
#' `vignette("agg_by")`.
#'
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to aggregate all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return [`AggOp`][AggOp] to be used in a call to `agg_by()` or `agg_all_by()`.
#'
#' @examples
#' \dontrun{
#' library(rdeephaven)
#'
#' # connecting to Deephaven server
#' client <- Client$new("localhost:10000", auth_type = "psk", auth_token = "my_secret_token")
#'
#' # create data frame, push to server, retrieve TableHandle
#' df <- data.frame(
#'   X = c("A", "B", "A", "C", "B", "A", "B", "B", "C"),
#'   Y = c("M", "N", "O", "N", "P", "M", "O", "P", "M"),
#'   Number1 = c(100, -44, 49, 11, -66, 50, 29, 18, -70),
#'   Number2 = c(-55, 76, 20, 130, 230, -50, 73, 137, 214)
#' )
#' th <- client$import_table(df)
#'
#' # compute median of Number1 and Number2
#' th1 <- th$
#'   agg_by(agg_median(c("Number1", "Number2")))
#'
#' # compute median of Number1 and Number2 grouped by X
#' th2 <- th$
#'   agg_by(agg_median(c("Number1", "Number2")), by = "X")
#'
#' # compute median of Number1 and Number2 grouped by X and Y
#' th3 <- th$
#'   agg_by(agg_median(c("Number1", "Number2")), by = c("X", "Y"))
#'
#' client$close()
#' }
#'
#' @export
agg_median <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(AggOp$new(INTERNAL_agg_median, "agg_median", cols = cols))
}

#' @name
#' agg_var
#' @title
#' Variance of specified columns by group
#' @md
#'
#' @description
#' Creates a Variance aggregation that computes the variance of each column in `cols` for each aggregation group.
#'
#' @details
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `agg_by()` or
#' `agg_all_by()` caller function. The aggregation groups are defined by the unique combinations of values in the `by`
#' columns. For example, if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of
#' values in the `A` and `B` columns.
#'
#' This function, like other Deephaven `agg` functions, is a generator function. That is, its output is another
#' function called an [`AggOp`][AggOp] intended to be used in a call to `agg_by()` or `agg_all_by()`. This detail is
#' typically hidden from the user. However, it is important to understand this detail for debugging purposes,
#' as the output of an `agg` function can otherwise seem unexpected.
#' 
#' For more information, see the vignette on `agg` functions by running
#' `vignette("agg_by")`.
#'
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to aggregate all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return [`AggOp`][AggOp] to be used in a call to `agg_by()` or `agg_all_by()`.
#'
#' @examples
#' \dontrun{
#' library(rdeephaven)
#'
#' # connecting to Deephaven server
#' client <- Client$new("localhost:10000", auth_type = "psk", auth_token = "my_secret_token")
#'
#' # create data frame, push to server, retrieve TableHandle
#' df <- data.frame(
#'   X = c("A", "B", "A", "C", "B", "A", "B", "B", "C"),
#'   Y = c("M", "N", "O", "N", "P", "M", "O", "P", "M"),
#'   Number1 = c(100, -44, 49, 11, -66, 50, 29, 18, -70),
#'   Number2 = c(-55, 76, 20, 130, 230, -50, 73, 137, 214)
#' )
#' th <- client$import_table(df)
#'
#' # compute variance of Number1 and Number2
#' th1 <- th$
#'   agg_by(agg_var(c("Number1", "Number2")))
#'
#' # compute variance of Number1 and Number2 grouped by X
#' th2 <- th$
#'   agg_by(agg_var(c("Number1", "Number2")), by = "X")
#'
#' # compute variance of Number1 and Number2 grouped by X and Y
#' th3 <- th$
#'   agg_by(agg_var(c("Number1", "Number2")), by = c("X", "Y"))
#'
#' client$close()
#' }
#'
#' @export
agg_var <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(AggOp$new(INTERNAL_agg_var, "agg_var", cols = cols))
}

#' @name
#' agg_std
#' @title
#' Standard deviation of specified columns by group
#' @md
#'
#' @description
#' Creates a Standard Deviation aggregation that computes the standard deviation of each column in `cols`, for each aggregation group.
#'
#' @details
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `agg_by()` or
#' `agg_all_by()` caller function. The aggregation groups are defined by the unique combinations of values in the `by`
#' columns. For example, if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of
#' values in the `A` and `B` columns.
#'
#' This function, like other Deephaven `agg` functions, is a generator function. That is, its output is another
#' function called an [`AggOp`][AggOp] intended to be used in a call to `agg_by()` or `agg_all_by()`. This detail is
#' typically hidden from the user. However, it is important to understand this detail for debugging purposes,
#' as the output of an `agg` function can otherwise seem unexpected.
#' 
#' For more information, see the vignette on `agg` functions by running
#' `vignette("agg_by")`.
#'
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to aggregate all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return [`AggOp`][AggOp] to be used in a call to `agg_by()` or `agg_all_by()`.
#'
#' @examples
#' \dontrun{
#' library(rdeephaven)
#'
#' # connecting to Deephaven server
#' client <- Client$new("localhost:10000", auth_type = "psk", auth_token = "my_secret_token")
#'
#' # create data frame, push to server, retrieve TableHandle
#' df <- data.frame(
#'   X = c("A", "B", "A", "C", "B", "A", "B", "B", "C"),
#'   Y = c("M", "N", "O", "N", "P", "M", "O", "P", "M"),
#'   Number1 = c(100, -44, 49, 11, -66, 50, 29, 18, -70),
#'   Number2 = c(-55, 76, 20, 130, 230, -50, 73, 137, 214)
#' )
#' th <- client$import_table(df)
#'
#' # compute standard deviation of Number1 and Number2
#' th1 <- th$
#'   agg_by(agg_std(c("Number1", "Number2")))
#'
#' # compute standard deviation of Number1 and Number2 grouped by X
#' th2 <- th$
#'   agg_by(agg_std(c("Number1", "Number2")), by = "X")
#'
#' # compute standard deviation of Number1 and Number2 grouped by X and Y
#' th3 <- th$
#'   agg_by(agg_std(c("Number1", "Number2")), by = c("X", "Y"))
#'
#' client$close()
#' }
#'
#' @export
agg_std <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(AggOp$new(INTERNAL_agg_std, "agg_std", cols = cols))
}

#' @name
#' agg_percentile
#' @title
#' p-th percentile of specified columns by group
#' @md
#'
#' @description
#' Creates a Percentile aggregation that computes the given percentile of each column in `cols` for each aggregation group.
#'
#' @details
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `agg_by()` or
#' `agg_all_by()` caller function. The aggregation groups are defined by the unique combinations of values in the `by`
#' columns. For example, if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of
#' values in the `A` and `B` columns.
#'
#' This function, like other Deephaven `agg` functions, is a generator function. That is, its output is another
#' function called an [`AggOp`][AggOp] intended to be used in a call to `agg_by()` or `agg_all_by()`. This detail is
#' typically hidden from the user. However, it is important to understand this detail for debugging purposes,
#' as the output of an `agg` function can otherwise seem unexpected.
#' 
#' For more information, see the vignette on `agg` functions by running
#' `vignette("agg_by")`.
#'
#' @param percentile Numeric scalar between 0 and 1 denoting the percentile to compute.
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to aggregate all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return [`AggOp`][AggOp] to be used in a call to `agg_by()` or `agg_all_by()`.
#'
#' @examples
#' \dontrun{
#' library(rdeephaven)
#'
#' # connecting to Deephaven server
#' client <- Client$new("localhost:10000", auth_type = "psk", auth_token = "my_secret_token")
#'
#' # create data frame, push to server, retrieve TableHandle
#' df <- data.frame(
#'   X = c("A", "B", "A", "C", "B", "A", "B", "B", "C"),
#'   Y = c("M", "N", "O", "N", "P", "M", "O", "P", "M"),
#'   Number1 = c(100, -44, 49, 11, -66, 50, 29, 18, -70),
#'   Number2 = c(-55, 76, 20, 130, 230, -50, 73, 137, 214)
#' )
#' th <- client$import_table(df)
#'
#' # compute 20th percentile of Number1 and Number2
#' th1 <- th$
#'   agg_by(agg_percentile(percentile = 0.2, cols = c("Number1", "Number2")))
#'
#' # compute 50th percentile of Number1 and Number2 grouped by X
#' th2 <- th$
#'   agg_by(agg_percentile(percentile = 0.5, cols = c("Number1", "Number2")), by = "X")
#'
#' # compute 75th percentile of Number1 and Number2 grouped by X and Y
#' th3 <- th$
#'   agg_by(agg_percentile(percentile = 0.75, cols = c("Number1", "Number2")), by = c("X", "Y"))
#'
#' client$close()
#' }
#'
#' @export
agg_percentile <- function(percentile, cols = character()) {
  verify_in_unit_interval("percentile", percentile, TRUE)
  verify_string("cols", cols, FALSE)
  return(AggOp$new(INTERNAL_agg_percentile, "agg_percentile", percentile = percentile, cols = cols))
}

#' @name
#' agg_count
#' @title
#' Number of observations by group
#' @md
#'
#' @description
#' Creates a Count aggregation that counts the number of rows in each aggregation group.
#'
#' @details
#' The aggregation groups that this function acts on are defined with the `by` parameter of the `agg_by()` caller
#' function. The aggregation groups are defined by the unique combinations of values in the `by` columns. For example,
#' if `by = c("A", "B")`, then the aggregation groups are defined by the unique combinations of values in the `A` and
#' `B` columns.
#'
#' This function, like other Deephaven `agg` functions, is a generator function. That is, its output is another
#' function called an [`AggOp`][AggOp] intended to be used in a call to `agg_by()` or `agg_all_by()`. This detail is
#' typically hidden from the user. However, it is important to understand this detail for debugging purposes,
#' as the output of an `agg` function can otherwise seem unexpected.
#' 
#' For more information, see the vignette on `agg` functions by running
#' `vignette("agg_by")`.
#'
#' Note that this operation is not supported in `agg_all_by()`.
#'
#' @param col String denoting the name of the new column to hold the counts of each aggregation group.
#' @return [`AggOp`][AggOp] to be used in a call to `agg_by()`.
#'
#' @examples
#' \dontrun{
#' library(rdeephaven)
#'
#' # connecting to Deephaven server
#' client <- Client$new("localhost:10000", auth_type = "psk", auth_token = "my_secret_token")
#'
#' # create data frame, push to server, retrieve TableHandle
#' df <- data.frame(
#'   X = c("A", "B", "A", "C", "B", "A", "B", "B", "C"),
#'   Y = c("M", "N", "O", "N", "P", "M", "O", "P", "M"),
#'   Number1 = c(100, -44, 49, 11, -66, 50, 29, 18, -70),
#'   Number2 = c(-55, 76, 20, 130, 230, -50, 73, 137, 214)
#' )
#' th <- client$import_table(df)
#'
#' # count number of elements in each group when grouped by X, name resulting column "count"
#' th1 <- th$
#'   agg_by(agg_count("count"), by = "X")
#'
#' # count number of elements in each group when grouped by X and Y, name resulting column "CountingCol"
#' th2 <- th$
#'   agg_by(agg_count("CountingCol"), by = c("X", "Y"))
#'
#' client$close()
#' }
#'
#' @export
agg_count <- function(col) {
  verify_string("col", col, TRUE)
  return(AggOp$new(INTERNAL_agg_count, "agg_count", col = col))
}
