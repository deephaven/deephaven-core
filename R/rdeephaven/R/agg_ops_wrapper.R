# An AggOp represents an aggregation operator that can be passed to agg_by() or agg_all_by(). This is the return type
# of all of the agg functions. It is a wrapper around an Rcpp_INTERNAL_AggOp, which itself is a wrapper around a
# C++ AggregateWrapper, which is finally a wrapper around a C++ Aggregate. See rdeephaven/src/client.cpp for details.
# Note that AggOps should not be instantiated directly by user code, but rather by provided agg functions.


#' @name
#' AggBy
#' @title
#' Aggregations in Deephaven
#' @md
#' @usage NULL
#' @format NULL
#' @docType class
#'
#' @description
#' Table aggregations are a quintessential feature of Deephaven. You can apply as many aggregations as
#' needed to static tables _or_ streaming tables, and if the parent tables are streaming, the resulting aggregated
#' tables will update alongside their parent tables. It is also very easy to perform _grouped_ aggregations, which
#' allow you to aggregate tables on a per-group basis.
#'
#' @section
#' Apply aggregations to a table:
#' There are two methods for performing aggregations on a table, `agg_by()` and `agg_all_by()`. `agg_by()` allows you to
#' perform many aggregations on specified columns, while `agg_all_by()` allows you to perform a single aggregation to
#' every non-grouping column in the table. Both methods have an optional `by` parameter that is used to specify grouping columns.
#' Here are some details on each method:
#'
#' - `TableHandle$agg_by(aggs, by)`: Creates a new table containing grouping columns and grouped data.
#'   The resulting grouped data is defined by the aggregation(s) specified.
#' - `TableHandle$agg_all_by(agg, by)`: Creates a new table containing grouping columns and grouped data.
#'   The resulting grouped data is defined by the aggregation specified. This method applies the aggregation to all
#'   non-grouping columns of the table, so it can only accept one aggregation at a time.
#'
#' The `agg_by()` and `agg_all_by()` methods themselves do not know anything about the columns on which you want to
#' perform aggregations. Rather, the desired columns are passed to individual `agg` functions, enabling you to apply
#' various kinds of aggregations to different columns or groups of columns as needed.
#'
#' @section
#' `agg` functions:
#' `agg` functions are used to perform aggregation calculations on grouped data by passing them to `agg_by()` or
#' `agg_all_by()`. These functions are _generators_, meaning they return _functions_ that the Deephaven engine knows
#' how to interpret. We call the functions that they return [`AggOp`][AggOp]s. These `AggOp`s are not R-level functions,
#' but Deephaven-specific data types that perform all of the intensive calculations. Here is a list of all `agg` functions
#' available in Deephaven:
#'
#' - [`agg_first()`][agg_first]
#' - [`agg_last()`][agg_last]
#' - [`agg_min()`][agg_min]
#' - [`agg_max()`][agg_max]
#' - [`agg_sum()`][agg_sum]
#' - [`agg_abs_sum()`][agg_abs_sum]
#' - [`agg_avg()`][agg_avg]
#' - [`agg_w_avg()`][agg_w_avg]
#' - [`agg_median()`][agg_median]
#' - [`agg_var()`][agg_var]
#' - [`agg_std()`][agg_std]
#' - [`agg_percentile()`][agg_percentile]
#' - [`agg_count()`][agg_count]
#'
#' For more details on each aggregation function, click on one of the methods above or see the reference documentation
#' by running `?agg_first`, `?agg_last`, etc.
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
#'   X = c("A", "B", "A", "C", "B", "A", "B", "B", "C"),
#'   Y = c("M", "N", "O", "N", "P", "M", "O", "P", "M"),
#'   Number1 = c(100, -44, 49, 11, -66, 50, 29, 18, -70),
#'   Number2 = c(-55, 76, 20, 130, 230, -50, 73, 137, 214)
#' )
#' th <- client$import_table(df)
#'
#' # get first and last elements of each column
#' th1 <- th$
#'   agg_by(agg_first(c("XFirst = X", "YFirst = Y", "Number1First = Number1", "Number2First = Number2")),
#'          agg_last(c("XLast = X", "YLast = Y", "Number1Last = Number1", "Number2Last = Number2")))
#'
#' # compute mean and standard deviation of Number1 and Number2, grouped by X
#' th2 <- th$
#'   agg_by(
#'     c(agg_avg(c("Number1Avg = Number1", "Number2Avg = Number2")),
#'       agg_std(c("Number1Std = Number1", "Number2Std = Number2"))),
#'     by="X")
#'
#' # compute maximum of all non-grouping columns, grouped by X and Y
#' th3 <- th$
#'   agg_all_by(agg_max(), by=c("X", "Y"))
#'
#' # compute minimum and maximum of Number1 and Number2 respectively grouped by Y
#' th4 <- th$
#'   agg_by(
#'     c(agg_min("Number1Min = Number1"),
#'       agg_max("Number2Max = Number2")),
#'     by="Y")
#'
#' client$close()
#' }
#'
NULL


#' Name AggOp
#' @title Deephaven AggOps
#' @md
#' @description
#' An `AggOp` is the return type of one of Deephaven's [`agg`][AggBy] functions. It is a function that performs the
#' computation specified by the `agg` function. These are intended to be passed directly to `agg_by()` or `agg_all_by()`,
#' and should never be instantiated directly be user code.
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
#' In this example, `aggregations` would be a vector of two `AggOp`s that can be reused in multiple calls to `agg_by()`.
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
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to aggregate all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return `AggOp` to be used in a call to `agg_by()` or `agg_all_by()`.
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
#'   agg_by(agg_first(c("Y", "Number1", "Number2")), by="X")
#'
#' # get first elements of Number1 and Number2 grouped by X and Y
#' th3 <- th
#'   agg_by(agg_first(c("Number1", "Number2")), by=c("X", "Y"))
#'
#' client$close()
#' }
#'
#' @export
agg_first <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(AggOp$new(INTERNAL_agg_first, "agg_first", cols=cols))
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
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to aggregate all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return `AggOp` to be used in a call to `agg_by()` or `agg_all_by()`.
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
#'   agg_by(agg_last(c("Y", "Number1", "Number2")), by="X")
#'
#' # get last elements of Number1 and Number2 grouped by X and Y
#' th3 <- th$
#'   agg_by(agg_last(c("Number1", "Number2")), by=c("X", "Y"))
#'
#' client$close()
#' }
#'
#' @export
agg_last <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(AggOp$new(INTERNAL_agg_last, "agg_last", cols=cols))
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
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to aggregate all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return `AggOp` to be used in a call to `agg_by()` or `agg_all_by()`.
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
#'   agg_by(agg_min(c("Number1", "Number2")), by="X")
#'
#' # get minimum elements of Number1 and Number2 grouped by X and Y
#' th3 <- th$
#'   agg_by(agg_min(c("Number1", "Number2")), by=c("X", "Y"))
#'
#' client$close()
#' }
#'
#' @export
agg_min <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(AggOp$new(INTERNAL_agg_min, "agg_min", cols=cols))
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
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to aggregate all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return `AggOp` to be used in a call to `agg_by()` or `agg_all_by()`.
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
#'   agg_by(agg_max(c("Number1", "Number2")), by="X")
#'
#' # get maximum elements of Number1 and Number2 grouped by X and Y
#' th3 <- th$
#'   agg_by(agg_max(c("Number1", "Number2")), by=c("X", "Y"))
#'
#' client$close()
#' }
#'
#' @export
agg_max <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(AggOp$new(INTERNAL_agg_max, "agg_max", cols=cols))
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
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to aggregate all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return `AggOp` to be used in a call to `agg_by()` or `agg_all_by()`.
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
#'   agg_by(agg_sum(c("Number1", "Number2")), by="X")
#'
#' # compute sum of Number1 and Number2 grouped by X and Y
#' th3 <- th$
#'   agg_by(agg_sum(c("Number1", "Number2")), by=c("X", "Y"))
#'
#' client$close()
#' }
#'
#' @export
agg_sum <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(AggOp$new(INTERNAL_agg_sum, "agg_sum", cols=cols))
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
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to aggregate all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return `AggOp` to be used in a call to `agg_by()` or `agg_all_by()`.
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
#'   agg_by(agg_abs_sum(c("Number1", "Number2")), by="X")
#'
#' # compute absolute sum of Number1 and Number2 grouped by X and Y
#' th3 <- th$
#'   agg_by(agg_abs_sum(c("Number1", "Number2")), by=c("X", "Y"))
#'
#' client$close()
#' }
#'
#' @export
agg_abs_sum <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(AggOp$new(INTERNAL_agg_abs_sum, "agg_abs_sum", cols=cols))
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
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to aggregate all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return `AggOp` to be used in a call to `agg_by()` or `agg_all_by()`.
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
#'   agg_by(agg_avg(c("Number1", "Number2")), by="X")
#'
#' # compute average of Number1 and Number2 grouped by X and Y
#' th3 <- th$
#'   agg_by(agg_avg(c("Number1", "Number2")), by=c("X", "Y"))
#'
#' client$close()
#' }
#'
#' @export
agg_avg <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(AggOp$new(INTERNAL_agg_avg, "agg_avg", cols=cols))
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
#' @param wcol String denoting the column to use for weights. This must be a numeric column.
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to aggregate all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return `AggOp` to be used in a call to `agg_by()` or `agg_all_by()`.
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
#'   X = c("A", "B", "A", "C", "B", "A", "B", "B", "C"),
#'   Y = c("M", "N", "O", "N", "P", "M", "O", "P", "M"),
#'   Number1 = c(100, -44, 49, 11, -66, 50, 29, 18, -70),
#'   Number2 = c(-55, 76, 20, 130, 230, -50, 73, 137, 214)
#' )
#' th <- client$import_table(df)
#'
#' # compute weighted average of Number1, weighted by Number2
#' th1 <- th$
#'   agg_by(agg_w_avg(wcol="Number2", cols="Number1"))
#'
#' # compute weighted average of Number1, weighted by Number2, grouped by X
#' th2 <- th$
#'   agg_by(agg_w_avg(wcol="Number2", cols="Number1", by="X"))
#'
#' # compute weighted average of Number1, weighted by Number2, grouped by X and Y
#' th3 <- th$
#'   agg_by(agg_w_avg(wcol="Number2", cols="Number1", by=c("X", "Y")))
#'
#' client$close()
#' }
#'
#' @export
agg_w_avg <- function(wcol, cols = character()) {
  verify_string("wcol", wcol, TRUE)
  verify_string("cols", cols, FALSE)
  return(AggOp$new(INTERNAL_agg_w_avg, "agg_w_avg", wcol=wcol, cols=cols))
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
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to aggregate all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return `AggOp` to be used in a call to `agg_by()` or `agg_all_by()`.
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
#'   agg_by(agg_median(c("Number1", "Number2")), by="X")
#'
#' # compute median of Number1 and Number2 grouped by X and Y
#' th3 <- th$
#'   agg_by(agg_median(c("Number1", "Number2")), by=c("X", "Y"))
#'
#' client$close()
#' }
#'
#' @export
agg_median <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(AggOp$new(INTERNAL_agg_median, "agg_median", cols=cols))
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
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to aggregate all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return `AggOp` to be used in a call to `agg_by()` or `agg_all_by()`.
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
#'   agg_by(agg_var(c("Number1", "Number2")), by="X")
#'
#' # compute variance of Number1 and Number2 grouped by X and Y
#' th3 <- th$
#'   agg_by(agg_var(c("Number1", "Number2")), by=c("X", "Y"))
#'
#' client$close()
#' }
#'
#' @export
agg_var <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(AggOp$new(INTERNAL_agg_var, "agg_var", cols=cols))
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
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to aggregate all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return `AggOp` to be used in a call to `agg_by()` or `agg_all_by()`.
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
#'   agg_by(agg_std(c("Number1", "Number2")), by="X")
#'
#' # compute standard deviation of Number1 and Number2 grouped by X and Y
#' th3 <- th$
#'   agg_by(agg_std(c("Number1", "Number2")), by=c("X", "Y"))
#'
#' client$close()
#' }
#'
#' @export
agg_std <- function(cols = character()) {
  verify_string("cols", cols, FALSE)
  return(AggOp$new(INTERNAL_agg_std, "agg_std", cols=cols))
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
#' @param percentile Numeric scalar between 0 and 1 denoting the percentile to compute.
#' @param cols String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
#' Default is to aggregate all non-grouping columns, which is only valid in the `agg_all_by()` operation.
#' @return `AggOp` to be used in a call to `agg_by()` or `agg_all_by()`.
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
#'   X = c("A", "B", "A", "C", "B", "A", "B", "B", "C"),
#'   Y = c("M", "N", "O", "N", "P", "M", "O", "P", "M"),
#'   Number1 = c(100, -44, 49, 11, -66, 50, 29, 18, -70),
#'   Number2 = c(-55, 76, 20, 130, 230, -50, 73, 137, 214)
#' )
#' th <- client$import_table(df)
#'
#' # compute 20th percentile of Number1 and Number2
#' th1 <- th$
#'   agg_by(agg_percentile(percentile=0.2, cols=c("Number1", "Number2")))
#'
#' # compute 50th percentile of Number1 and Number2 grouped by X
#' th2 <- th$
#'   agg_by(agg_percentile(percentile=0.5, cols=c("Number1", "Number2")), by="X")
#'
#' # compute 75th percentile of Number1 and Number2 grouped by X and Y
#' th3 <- th$
#'   agg_by(agg_percentile(percentile=0.75, cols=c("Number1", "Number2")), by=c("X", "Y"))
#'
#' client$close()
#' }
#'
#' @export
agg_percentile <- function(percentile, cols = character()) {
  verify_in_unit_interval("percentile", percentile, TRUE)
  verify_string("cols", cols, FALSE)
  return(AggOp$new(INTERNAL_agg_percentile, "agg_percentile", percentile=percentile, cols=cols))
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
#' Note that this operation is not supported in `agg_all_by()`.
#'
#' @param col String denoting the name of the new column to hold the counts of each aggregation group.
#' @return `AggOp` to be used in a call to `agg_by()`.
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
#'   X = c("A", "B", "A", "C", "B", "A", "B", "B", "C"),
#'   Y = c("M", "N", "O", "N", "P", "M", "O", "P", "M"),
#'   Number1 = c(100, -44, 49, 11, -66, 50, 29, 18, -70),
#'   Number2 = c(-55, 76, 20, 130, 230, -50, 73, 137, 214)
#' )
#' th <- client$import_table(df)
#'
#' # count number of elements in each group when grouped by X, name resulting column "count"
#' th1 <- th$
#'   agg_by(agg_count("count"), by="X")
#'
#' # count number of elements in each group when grouped by X and Y, name resulting column "CountingCol"
#' th2 <- th$
#'   agg_by(agg_count("CountingCol"), by=c("X", "Y"))
#'
#' client$close()
#' }
#'
#' @export
agg_count <- function(col) {
  verify_string("col", col, TRUE)
  return(AggOp$new(INTERNAL_agg_count, "agg_count", col=col))
}