% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/update_by_ops_wrapper.R
\name{uby_rolling_group_time}
\alias{uby_rolling_group_time}
\title{Rolling grouping with time as the windowing unit}
\arguments{
\item{ts_col}{String denoting the column to use as the timestamp.}

\item{cols}{String or list of strings denoting the column(s) to operate on. Can be renaming expressions, i.e. “new_col = col”.
Default is to compute the rolling group for all non-grouping columns.}

\item{rev_time}{ISO-8601-formatted string specifying the look-behind window size.}

\item{fwd_time}{ISO-8601-formatted string specifying the look-ahead window size. Default is 0 seconds.}
}
\value{
\code{UpdateByOp} to be used in a call to \code{update_by()}.
}
\description{
Creates a rolling group UpdateByOp for each column in \code{cols}, using time as the windowing unit.
}
\details{
This uses \href{https://en.wikipedia.org/wiki/ISO_8601}{ISO-8601} time strings as the reverse and forward window parameters.
Negative values are allowed and can be used to generate completely forward or completely reverse windows.
A row containing a null value in the timestamp column belongs to no window and will not be considered
in the windows of other rows; its output will be null.
Here are some examples of window values:
\itemize{
\item \verb{rev_time = "PT00:10:00", fwd_time = "PT00:10:00"} - contains rows from 10m before through 10m following
the current row timestamp (inclusive)
\item \verb{rev_time = "PT00:10:00", fwd_time = "-PT00:05:00"} - contains rows from 10m before through 5m before the
current row timestamp (inclusive), this is a purely backwards looking window
\item \verb{rev_time = "-PT00:05:00", fwd_time = "PT00:10:00"} - contains rows from 5m following through 10m
following the current row timestamp (inclusive), this is a purely forwards looking window
}

This function acts on aggregation groups specified with the \code{by} parameter of the \code{update_by()} caller function.
The aggregation groups are defined by the unique combinations of values in the \code{by} columns. For example,
if \code{by = c("A", "B")}, then the aggregation groups are defined by the unique combinations of values in the
\code{A} and \code{B} columns.

This function, like other Deephaven \code{uby} functions, is a generator function. That is, its output is another
function called an \code{\link{UpdateByOp}} intended to be used in a call to \code{update_by()}. This detail is typically
hidden from the user. However, it is important to understand this detail for debugging purposes, as the output of
a \code{uby} function can otherwise seem unexpected.

For more information, see the vignette on \code{uby} functions by running
\code{vignette("update_by")}.
}
\examples{
\dontrun{
library(rdeephaven)

# connecting to Deephaven server
client <- Client$new("localhost:10000", auth_type = "psk", auth_token = "my_secret_token")

# create data frame, push to server, retrieve TableHandle
df <- data.frame(
  timeCol = seq.POSIXt(as.POSIXct(Sys.Date()), as.POSIXct(Sys.Date() + 0.01), by = "1 sec")[1:500],
  boolCol = sample(c(TRUE, FALSE), 500, TRUE),
  col1 = sample(10000, size = 500, replace = TRUE),
  col2 = sample(10000, size = 500, replace = TRUE),
  col3 = 1:500
)
th <- client$import_table(df)

# compute rolling group of col1 and col2, grouped by boolCol, using previous 5 seconds, and following 5 seconds
th1 <- th$
  update_by(uby_rolling_group_time(ts_col = "timeCol", cols = c("col1RollGroup = col1", "col2RollGroup = col2"), rev_time = "PT5s", fwd_ticks = "PT5s"), by = "boolCol")

# compute rolling group of col1 and col2, grouped by boolCol and parity of col3, using following 10 seconds
th2 <- th$
  update("col3Parity = col3 \% 2")$
  update_by(uby_rolling_group_time(ts_col = "timeCol", cols = c("col1RollGroup = col1", "col2RollGroup = col2"), rev_time = "PT0s", fwd_time = "PT10s"), by = c("boolCol", "col3Parity"))

client$close()
}

}
