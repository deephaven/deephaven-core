#' @import Rcpp
#' @useDynLib rdeephaven, .registration = TRUE
#' @importFrom Rcpp evalCpp

loadModule("ClientModule", TRUE)