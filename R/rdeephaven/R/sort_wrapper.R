#' @export
Sorter <- R6Class("Sorter", cloneable = FALSE,
    public = list(

        #' @description
        #' Create a Sorter instance.
        initialize = function(sort_pair) {
            if (class(sort_pair) != "Rcpp_INTERNAL_SortPair") {
                stop("'sort_pair' should be an internal Deephaven SortPair. If you're seeing this,\n  you are trying to call the constructor of a Sorter directly, which is not advised.\n  Please use one of the provided sorting functions instead.")
            }
            self$internal_sorter <- sort_pair
        },

        internal_sorter = NULL
    )
)

### All of the functions below return an instance of the above class

#' @export
sort_asc <- function(column, abs=FALSE) {
    verify_string("column", column)
    verify_bool("abs", abs)
    return(Sorter$new(INTERNAL_sort_asc(column, abs)))
}

#' @export
sort_desc <- function(column, abs=FALSE) {
    verify_string("column", column)
    verify_bool("abs", abs)
    return(Sorter$new(INTERNAL_sort_desc(column, abs)))
}