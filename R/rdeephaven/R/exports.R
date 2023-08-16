#' @import Rcpp
#' @useDynLib rdeephaven, .registration = TRUE
#' @importFrom Rcpp evalCpp
#'
#' @importFrom magrittr %>%
#' @importFrom arrow arrow_table as_arrow_table as_record_batch_reader Table RecordBatchReader RecordBatchStreamReader
#' @importFrom dplyr as_tibble as_data_frame

loadModule("DeephavenInternalModule", TRUE)
