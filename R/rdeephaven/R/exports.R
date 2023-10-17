#' @import Rcpp
#' @useDynLib rdeephaven, .registration = TRUE
#' @importFrom Rcpp evalCpp
#'
#' @importFrom arrow arrow_table as_arrow_table as_record_batch_reader
#' @importFrom R6 R6Class
#' @importFrom dplyr as_tibble as_data_frame
#' @importFrom utils head tail

loadModule("DeephavenInternalModule", TRUE)
