#' @import Rcpp
#' @useDynLib rdeephaven, .registration = TRUE
#' @importFrom Rcpp evalCpp
#'
#' @importFrom arrow arrow_table as_arrow_table as_record_batch_reader
#' @importFrom dplyr as_tibble as_data_frame

#' @name
#' rdeephaven
#' @title
#' The Deephaven Community R Client
#' @md
#' @usage NULL
#' @format NULL
#'
#' @description
#' The Deephaven Community R Client provides an R interface to Deephaven's powerful real-time data engine, [_Deephaven Core_](https://deephaven.io/community/).
#' To use this package, you must have a Deephaven server running and be able to connect to it. For more information on
#' how to set up a Deephaven server, see the documentation [here](https://deephaven.io/core/docs/tutorials/quickstart/).
#' For a brief overview of the Deephaven R Client, see the [blog post](https://deephaven.io/blog/2023/10/04/r-client-updates/)
#' we published on the topic.
#'
#' @section
#' Building blocks of the Deephaven R Client:
#' There are two primary R classes that make up the Deephaven R Client, the [`Client`][Client] class and the
#' [`TableHandle`][TableHandle] class. The `Client` class is used to establish a connection to the Deephaven server with
#' its constructor `Client$new()`, and to send server requests, such as running a script via `run_script()`, or pushing
#' local data to the server via `open_table()`. Many of these server requests end up creating or modifying tables that
#' live on the server. To keep track of these tables, the R client retrieves references to them, and wraps these references
#' in `TableHandle` objects. These TableHandles have a host of methods that mirror server-side table operations, such as
#' `head()`, `tail()`, `update_by()`, and so on. So, you can typically use TableHandles _as if_ they are tables themselves,
#' and all of the corresponding methods that you call on them will be executed on the server. TableHandles also support
#' common functional methods for converting server-side Deephaven tables to R objects stored in local memory such as
#' `as.data.frame()`, `as_tibble()`, and `as_arrow_table()`. For more information on these classes and all of their methods,
#' see the reference documentation for [`Client`][Client] and [`TableHandle`][TableHandle] by clicking on their class names,
#' or by running `?Client` or `?TableHandle`.
#'
#' @section
#' Real-time data analysis:
#' Since TableHandles are references to tables living on the Deephaven server, they may refer to streaming tables, or
#' or tables that are receiving new data every second. R objects like data frames or Dplyr tibbles do not have this property -
#' they are always static objects stored in memory. However, a TableHandle referring to a streaming table may be converted
#' to a data frame or tibble at any time, and the resulting object will be a snapshot of the table at the time of conversion.
#' This means that you can use the Deephaven R Client to perform real-time data analysis on streaming data! Of course,
#' there are performance and memory considerations when pulling data from the server, so it is best to use the provided
#' TableHandle methods to perform as much of your analysis as possible on the server, and to only pull the data when
#' something _must_ be done in R, like plotting or writing to a local file.
#'
#' @section
#' Powerful table operations:
#' Much of the power of Deephaven's suite of table operations is achieved through the use of the [`update_by()`][UpdateBy]
#' and [`agg_by()`][AggBy] methods. These table methods are important enough to warrant their own documentation pages, accessible
#' by clicking on their names, or by running `?UpdateBy` or `?AggBy`. These methods come with their own suites of functions,
#' prefixed with `agg_` and `uby_` respectively, that are discoverable from their documentation pages. Running `ls("package:rdeephaven")`
#' will reveal that most of the functions included in this package are for these methods, so it is important to get acquainted
#' with them.
#'
#' @section
#' Getting help:
#' While we've done our best to provide good documentation for this package, you may find you need more help than what
#' this documentation has to offer. If this is the case, please visit the official Deephaven Community Core
#' [documentation](https://deephaven.io/core/docs/tutorials/quickstart/) to learn more about Deephaven and to find examples
#' that may help you. If this is not sufficient, feel free to reach out to us on the Deephaven [Community Slack channel](https://deephaven.io/slack).
#' We hope you find real-time data analysis in R to be as easy as possible.
#'
NULL

loadModule("DeephavenInternalModule", TRUE)
