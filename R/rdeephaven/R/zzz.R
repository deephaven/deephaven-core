#' Imports
#' @useDynLib Client, .registration = TRUE
#' @export Client
#' @import Rcpp
"_PACKAGE"

Rcpp::loadModule(module = "ClientCaller", TRUE)