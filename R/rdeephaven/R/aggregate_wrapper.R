#' @export
Aggregation <- R6Class("Aggregation",
    public = list(

        #' @description
        #' Create an Aggregation instance.
        initialize = function(aggregation) {
            if (class(aggregation) != "Rcpp_INTERNAL_Aggregate") {
                stop("'aggregation' should be an internal Deephaven Aggregation. If you're seeing this,
                you are trying to call the constructor of an Aggregation directly, which is not advised.
                Please use one of the provided aggregation functions instead.")
            }
            self$internal_aggregation <- aggregation
        },

        internal_aggregation = NULL
    )
)