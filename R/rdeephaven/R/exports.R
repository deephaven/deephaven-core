#' @import Rcpp
#' @useDynLib rdeephaven, .registration = TRUE
#' @importFrom Rcpp evalCpp
#' 
#' @importFrom magrittr %>%
#' @importFrom arrow arrow_table as_arrow_table as_record_batch_reader RecordBatchStreamReader
#' @importFrom dplyr as_tibble

loadModule("DeephavenInternalModule", TRUE)
