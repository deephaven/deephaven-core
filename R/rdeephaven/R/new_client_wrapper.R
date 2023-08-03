#' @export
setClass(
  "S4Client",
  representation(
    .internal_rcpp_object = "Rcpp_INTERNAL_Client"
  )
)

setGeneric(
  "connect",
  function(target, client_options) {
    return(standardGeneric("connect"))
  },
  signature = c("target", "client_options")
)

#' @export
setMethod(
  "connect",
  signature = c(target = "character", client_options = "S4ClientOptions"),
  function(target, client_options) {
    internal_client <- new(INTERNAL_Client,
      target = target,
      client_options = client_options@.internal_rcpp_object
    )
    return(new("S4Client", .internal_rcpp_object = internal_client))
  }
)

### HELPER FUNCTIONS ###
# These functions return RC objects returned by Rcpp without wrapping them in S4

check_for_table <- function(client, name) {
  return(client@.internal_rcpp_object$check_for_table(name))
}

rbr_to_dh_table <- function(client, rbr) {
  ptr <- client@.internal_rcpp_object$new_arrow_array_stream_ptr()
  rbr$export_to_c(ptr)
  return(client@.internal_rcpp_object$new_table_from_arrow_array_stream_ptr(ptr))
}

arrow_to_dh_table <- function(client, arrow_tbl) {
  rbr <- as_record_batch_reader(arrow_tbl)
  return(rbr_to_dh_table(client, rbr))
}

tibble_to_dh_table <- function(client, tibbl) {
  arrow_tbl <- arrow_table(tibbl)
  return(arrow_to_dh_table(client, arrow_tbl))
}

df_to_dh_table <- function(client, data_frame) {
  arrow_tbl <- arrow_table(data_frame)
  return(arrow_to_dh_table(client, arrow_tbl))
}

### USER-FACING METHODS ###

setGeneric(
  "open_table",
  function(client_instance, name) {
    return(standardGeneric("open_table"))
  },
  signature = c("client_instance", "name")
)

#' @export
setMethod(
  "open_table",
  signature = c(client_instance = "S4Client"),
  function(client_instance, name) {
    verify_string("name", name, TRUE)
    if (!check_for_table(client_instance, name)) {
      stop(paste0("The table '", name, "' you're trying to pull does not exist on the server."))
    }
    return(new("S4TableHandle", .internal_rcpp_object = client_instance@.internal_rcpp_object$open_table(name)))
  }
)

setGeneric(
  "empty_table",
  function(client_instance, size) {
    return(standardGeneric("empty_table"))
  },
  signature = c("client_instance", "size")
)

#' @export
setMethod(
  "empty_table",
  signature = c(client_instance = "S4Client"),
  function(client_instance, size) {
    verify_positive_int("size", size, TRUE)
    return(new("S4TableHandle", .internal_rcpp_object = client_instance@.internal_rcpp_object$empty_table(size)))
  }
)

setGeneric(
  "time_table",
  function(client_instance, ...) {
    return(standardGeneric("time_table"))
  },
  signature = "client_instance"
)

#' @export
setMethod(
  "time_table",
  signature = c(client_instance = "S4Client"),
  function(client_instance, period_nanos, start_time_nanos = 0) {
    verify_any_int("period_nanos", period_nanos, TRUE)
    verify_any_int("start_time_nanos", start_time_nanos, TRUE)
    return(new("S4TableHandle", .internal_rcpp_object = client_instance@.internal_rcpp_object$time_table(start_time_nanos, period_nanos)))
  }
)

setGeneric(
  "import_table",
  function(client_instance, table_object) {
    return(standardGeneric("import_table"))
  },
  signature = c("client_instance", "table_object")
)

#' @export
setMethod(
  "import_table",
  signature = c(client_instance = "S4Client"),
  function(client_instance, table_object) {
    
    table_object_class <- class(table_object)
    
    if (table_object_class[[1]] == "data.frame") {
      rcpp_dh_table <- df_to_dh_table(client_instance, table_object)
    }
    else if (table_object_class[[1]] == "tbl_df") {
      rcpp_dh_table <- tibble_to_dh_table(client_instance, table_object)
    }
    else if (table_object_class[[1]] == "RecordBatchReader") {
      rcpp_dh_table <- rbr_to_dh_table(client_instance, table_object)
    }
    else if ((length(table_object_class) == 4 &&
              table_object_class[[1]] == "Table" &&
              table_object_class[[3]] == "ArrowObject")) {
      rcpp_dh_table <- arrow_to_dh_table(client_instance, table_object)
    }
    else {
      stop(paste0("'table_object' must be either an R Data Frame, a dplyr Tibble, an Arrow Table, or an Arrow Record Batch Reader. Got an object of class ", table_object_class[[1]], " instead."))
    }
    return(new("S4TableHandle", .internal_rcpp_object = rcpp_dh_table))
  }
)

setGeneric(
  "run_script",
  function(client_instance, script) {
    return(standardGeneric("run_script"))
  },
  signature = c("client_instance", "script")
)

#' @export
setMethod(
  "run_script",
  signature = c(client_instance = "S4Client"),
  function(client_instance, script) {
    verify_string("script", script, TRUE)
    client_instance@.internal_rcpp_object$run_script(script)
    return(NULL)
  }
)

# do not need to set generic for 'close', as it already exists as a generic
#' @export
setMethod(
  "close",
  signature = c(con = "S4Client"),
  function(con) {
    con@.internal_rcpp_object$close()
    return(NULL)
  }
)
