#' @import Rcpp
#' @useDynLib rdeephaven, .registration = TRUE
#' @importFrom Rcpp evalCpp
#'
#' @importFrom arrow arrow_table as_arrow_table as_record_batch_reader
#' @importFrom dplyr as_tibble as_data_frame

loadModule("DeephavenInternalModule", TRUE)
