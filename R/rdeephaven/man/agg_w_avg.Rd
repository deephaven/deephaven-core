% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/agg_ops_wrapper.R
\name{agg_w_avg}
\alias{agg_w_avg}
\title{Weighted average of specified columns by group}
\arguments{
\item{wcol}{String denoting the column to use for weights. This must be a numeric column.}

\item{cols}{String or list of strings denoting the column(s) to aggregate. Can be renaming expressions, i.e. “new_col = col”.
Default is to aggregate all non-grouping columns, which is only valid in the \code{agg_all_by()} operation.}
}
\value{
\code{\link{AggOp}} to be used in a call to \code{agg_by()} or \code{agg_all_by()}.
}
\description{
Creates a Weighted Average aggregation that computes the weighted average of each column in \code{cols} for each aggregation group.
}
\details{
The aggregation groups that this function acts on are defined with the \code{by} parameter of the \code{agg_by()} or
\code{agg_all_by()} caller function. The aggregation groups are defined by the unique combinations of values in the \code{by}
columns. For example, if \code{by = c("A", "B")}, then the aggregation groups are defined by the unique combinations of
values in the \code{A} and \code{B} columns.

This function, like other Deephaven \code{agg} functions, is a generator function. That is, its output is another
function called an \code{\link{AggOp}} intended to be used in a call to \code{agg_by()} or \code{agg_all_by()}. This detail is
typically hidden from the user. However, it is important to understand this detail for debugging purposes,
as the output of an \code{agg} function can otherwise seem unexpected.

For more information, see the vignette on \code{agg} functions by running
\code{vignette("agg_by")}.
}
\examples{
\dontrun{
library(rdeephaven)

# connecting to Deephaven server
client <- Client$new("localhost:10000", auth_type = "psk", auth_token = "my_secret_token")

# create data frame, push to server, retrieve TableHandle
df <- data.frame(
  X = c("A", "B", "A", "C", "B", "A", "B", "B", "C"),
  Y = c("M", "N", "O", "N", "P", "M", "O", "P", "M"),
  Number1 = c(100, -44, 49, 11, -66, 50, 29, 18, -70),
  Number2 = c(-55, 76, 20, 130, 230, -50, 73, 137, 214)
)
th <- client$import_table(df)

# compute weighted average of Number1, weighted by Number2
th1 <- th$
  agg_by(agg_w_avg(wcol = "Number2", cols = "Number1"))

# compute weighted average of Number1, weighted by Number2, grouped by X
th2 <- th$
  agg_by(agg_w_avg(wcol = "Number2", cols = "Number1", by = "X"))

# compute weighted average of Number1, weighted by Number2, grouped by X and Y
th3 <- th$
  agg_by(agg_w_avg(wcol = "Number2", cols = "Number1", by = c("X", "Y")))

client$close()
}

}
