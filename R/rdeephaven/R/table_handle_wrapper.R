#' @export
TableHandle <- R6Class("TableHandle",
  cloneable = FALSE,
  public = list(
    
    .internal_rcpp_object = NULL,
    
    initialize = function(table_handle) {
      if (class(table_handle)[[1]] != "Rcpp_INTERNAL_TableHandle") {
        stop("'table_handle' should be an internal Deephaven TableHandle. If you're seeing this,
                you are trying to call the constructor of TableHandle directly, which is not advised.")
      }
      self$.internal_rcpp_object <- table_handle
    },

    #' @description
    #' Whether the table referenced by this TableHandle is static or not.
    #' @return TRUE if the table is static, or FALSE if the table is ticking.
    is_static = function() {
      return(self$.internal_rcpp_object$is_static())
    },
    
    #' @description
    #' Binds the table referenced by this TableHandle to a variable on the server,
    #' enabling it to be accessed by that name from any Deephaven API.
    #' @param name Name for this table on the server.
    bind_to_variable = function(name) {
      verify_string("name", name, TRUE)
      self$.internal_rcpp_object$bind_to_variable(name)
    },
    
    ### BASE R METHODS, ALSO IMPLEMENTED FUNCTIONALLY
    
    head = function(n) {
      verify_positive_int("n", n, TRUE)
      return(TableHandle$new(self$.internal_rcpp_object$head(n)))
    },
    
    tail = function() {
      verify_positive_int("n", n, TRUE)
      return(TableHandle$new(self$.internal_rcpp_object$tail(n)))
    },

    #' @description
    #' Number of rows in the table referenced by this TableHandle.
    #' @return The number of rows in the table.
    nrow = function() {
      return(self$.internal_rcpp_object$num_rows())
    },
    
    ncol = function() {
      return(self$.internal_rcpp_object$num_cols())
    },
    
    dim = function() {
      return(c(self$nrow, self$ncol))
    },
    
    ### CONVERSION METHODS, ALSO IMPLEMENTED FUNCTIONALLY

    #' @description
    #' Imports the table referenced by this TableHandle into an Arrow RecordBatchStreamReader.
    #' @return A RecordBatchStreamReader containing the data from the table referenced by this TableHandle.
    as_record_batch_reader = function() {
      ptr <- self$.internal_rcpp_object$get_arrow_array_stream_ptr()
      rbsr <- RecordBatchStreamReader$import_from_c(ptr)
      return(rbsr)
    },

    #' @description
    #' Imports the table referenced by this TableHandle into an Arrow Table.
    #' @return A Table containing the data from the table referenced by this TableHandle.
    as_arrow_table = function() {
      rbsr <- self$to_arrow_record_batch_stream_reader()
      arrow_tbl <- rbsr$read_table()
      return(arrow_tbl)
    },

    #' @description
    #' Imports the table referenced by this TableHandle into a dplyr Tibble.
    #' @return A Tibble containing the data from the table referenced by this TableHandle.
    as_tibble = function() {
      rbsr <- self$to_arrow_record_batch_stream_reader()
      arrow_tbl <- rbsr$read_table()
      return(as_tibble(arrow_tbl))
    },

    #' @description
    #' Imports the table referenced by this TableHandle into an R Data Frame.
    #' @return A Data Frame containing the data from the table referenced by this TableHandle.
    as_data_frame = function() {
      arrow_tbl <- self$to_arrow_table()
      return(as.data.frame(as.data.frame(arrow_tbl))) # TODO: for some reason as.data.frame on arrow table returns a tibble, not a data frame
    },
    
    ### DEEPHAVEN TABLE OPERATIONS
    
    select = function(by = character()) {
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$select(by)))
    },
    
    view = function(by = character()) {
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$view(by)))
    },

    update = function(by = character()) {
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$update(by)))
    },
    
    update_view = function(by = character()) {
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$update_view(by)))
    },
    
    drop_columns = function(by = character()) {
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$drop_columns(by)))
    },
    
    where = function(filter) {
      verify_string("filter", filter, TRUE)
      return(TableHandle$new(self$.internal_rcpp_object$where(filter)))
    },
    
    group_by = function(by = character()) {
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$group_by(by)))
    },
    
    ungroup = function(by = character()) {
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$ungroup(by)))
    },
    
    agg_by = function(aggs, by = character()) {
      verify_type("aggs", aggs, "Aggregation", "Deephaven Aggregation", FALSE)
      verify_string("by", by, FALSE)
      aggs <- c(aggs)
      unwrapped_aggs <- lapply(aggs, strip_r6_wrapping)
      return(TableHandle$new(self$.internal_rcpp_object$agg_by(unwrapped_aggs, by)))
    },
    
    first_by = function(by = character()) {
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$first_by(by)))
    },
  
    last_by = function(by = character()) {
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$last_by(by)))
    },
  
    head_by = function(num_rows, by = character()) {
      verify_positive_int("num_rows", num_rows, TRUE)
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$head_by(num_rows, by)))
    },
    
    tail_by = function(num_rows, by = character()) {
      verify_positive_int("num_rows", num_rows, TRUE)
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$tail_by(num_rows, by)))
    },
    
    min_by = function(by = character()) {
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$min_by(by)))
    },
    
    max_by = function(by = character()) {
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$max_by(by)))
    },
    
    sum_by = function(by = character()) {
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$sum_by(by)))
    },
    
    abs_sum_by = function(by = character()) {
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$abs_sum_by(by)))
    },
    
    avg_by = function(by = character()) {
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$avg_by(by)))
    },
    
    w_avg_by = function(wcol, by = character()) {
      verify_string("wcol", wcol, TRUE)
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$w_avg_by(wcol, by)))
    },
    
    median_by = function(by = character()) {
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$median_by(by)))
    },
    
    var_by = function(by = character()) {
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$var_by(by)))
    },
    
    std_by = function(by = character()) {
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$std_by(by)))
    },
    
    percentile_by = function(percentile, by = character()) {
      verify_in_unit_interval("percentile", percentile, TRUE)
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$percentile_by(percentile, by)))
    },
    
    count_by = function(col = "n", by = character()) {
      verify_string("col", col, TRUE)
      verify_string("by", by, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$count_by(col, by)))
    },
    
    cross_join = function(right_side, columns_to_match = character(), columns_to_add = character()) {
      verify_string("columns_to_match", columns_to_match, FALSE)
      verify_string("columns_to_add", columns_to_add, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$cross_join(
        right_side$.internal_rcpp_object,
        columns_to_match, columns_to_add
      )))
    },
    
    natural_join = function(right_side, columns_to_match = character(), columns_to_add = character()) {
      verify_string("columns_to_match", columns_to_match, FALSE)
      verify_string("columns_to_add", columns_to_add, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$natural_join(
        right_side$.internal_rcpp_object,
        columns_to_match, columns_to_add
      )))
    },
    
    exact_join = function(right_side, columns_to_match = character(), columns_to_add = character()) {
      verify_string("columns_to_match", columns_to_match, FALSE)
      verify_string("columns_to_add", columns_to_add, FALSE)
      return(TableHandle$new(self$.internal_rcpp_object$exact_join(
        right_side$.internal_rcpp_object,
        columns_to_match, columns_to_add
      )))
    },
    
    sort = function(by, descending = FALSE, abs_sort = FALSE) {
      verify_string("by", by, FALSE)
      verify_bool("descending", descending, FALSE)
      verify_bool("abs_sort", abs_sort, FALSE)
      if ((length(descending) > 1) && length(descending) != length(by)) {
        stop(paste0("'descending' must be the same length as 'by' if more than one entry is supplied. Got 'by' with length ", length(by), " and 'descending' with length ", length(descending), "."))
      }
      if ((length(abs_sort) > 1) && length(abs_sort) != length(by)) {
        stop(paste0("'abs_sort' must be the same length as 'by' if more than one entry is supplied. Got 'by' with length ", length(by), " and 'abs_sort' with length ", length(abs_sort), "."))
      }
      return(TableHandle$new(self$.internal_rcpp_object$sort(by, descending, abs_sort)))
    },
    
    merge = function(...) {
      table_list <- unlist(c(...))
      if (length(table_list) == 0) {
        return(NULL)
      }
      verify_type("table_list", table_list, "TableHandle", "Deephaven TableHandle", FALSE)
      if (length(table_list) == 1) {
        return(table_list[[1]])
      }
      unwrapped_table_list <- lapply(table_list, strip_r6_wrapping)
      return(TableHandle$new(private$internal_table_object$merge(unwrapped_table_list)))
    }
  )
)
