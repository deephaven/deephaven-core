% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/agg_ops_wrapper.R
\docType{class}
\name{AggOp}
\alias{AggOp}
\title{Deephaven AggOps}
\description{
An \code{AggOp} is the return type of one of Deephaven's \code{agg} functions. It is a function that performs the
computation specified by the \code{agg} function. These are intended to be passed directly to \code{agg_by()} or \code{agg_all_by()},
and should never be instantiated directly be user code. For more information, see the
vignette on \code{agg} functions with \code{vignette("agg_by")}.

If multiple tables have the same schema and the same aggregations need to be applied to each table, saving these
objects directly in a variable may be useful to avoid having to re-create them each time:

\if{html}{\out{<div class="sourceCode">}}\preformatted{aggregations <- c(agg_min("XMin = X", "YMin = Y"),
                  agg_max("XMax = X", "YMax = Y"))

result1 <- th1$agg_by(aggregations, by="Group")
result2 <- th2$agg_by(aggregations, by="Group")
}\if{html}{\out{</div>}}

In this example, \code{aggregations} would be a vector of two AggOps that can be reused in multiple calls to \code{agg_by()}.
}
\section{Methods}{
\subsection{Public methods}{
\itemize{
\item \href{#method-AggOp-new}{\code{AggOp$new()}}
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-AggOp-new"></a>}}
\if{latex}{\out{\hypertarget{method-AggOp-new}{}}}
\subsection{Method \code{new()}}{
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{AggOp$new(aggregation, agg_name, ...)}\if{html}{\out{</div>}}
}

}
}
