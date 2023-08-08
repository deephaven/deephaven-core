#' @export
setClass(
  "Client",
  representation(
    .internal_rcpp_object = "Rcpp_INTERNAL_Client"
  )
)

setGeneric(
  "connect",
  function(target, ...) {
    return(standardGeneric("connect"))
  },
  signature = c("target")
)

#' @export
setMethod(
  "connect",
  signature = c(target = "character"),
  function(target,
           auth_type = "anonymous",
           auth_token_pair = "",
           session_type = "python",
           use_tls = FALSE,
           tls_root_certs = "",
           int_option = "",
           string_option = "",
           extra_header = "") {
    options <- new(INTERNAL_ClientOptions)
    
    verify_string("target", target, TRUE)
    verify_bool("use_tls", use_tls, TRUE)

    # check if auth_type needs to be changed and set credentials accordingly
    if (auth_type == "anonymous") {
      options$set_default_authentication()
    }
    else if (auth_type == "basic") {
      if (auth_token_pair != "") {
        verify_string("auth_token_pair", auth_token_pair, TRUE)
        username_password <- strsplit(auth_token_pair, ":", fixed = TRUE)
        options$set_basic_authentication(username_password[1], username_password[2])
      } else {
        stop("Basic authentication was requested, but no 'auth_token_pair' was provided.")
      }
    }
    else if (auth_type == "custom") {
      if (auth_token_pair != "") {
        verify_string("auth_token_pair", auth_token_pair, TRUE)
        key_value <- strsplit(auth_token_pair, ":", fixed = TRUE)
        options$set_custom_authentication(key_value[1], key_value[2])
      } else {
        stop("Custom authentication was requested, but no 'auth_token_pair' was provided.")
      }
    }
    else {
      stop(paste0("'auth_type' must be 'anonymous', 'basic', or 'custom', but got ", auth_type, "."))
    }

    # set session type if a valid session type is provided
    if ((session_type == "python") || (session_type == "groovy")) {
      options$set_session_type(session_type)
    }
    else {
      stop(paste0("'session_type' must be 'python' or 'groovy', but got ", session_type, "."))
    }

    # if tls is requested, set it and set the root_certs if provided
    if (use_tls == TRUE) {
      options$set_use_tls()
      if (tls_root_certs != "") {
        verify_string("tls_root_certs", tls_root_certs, TRUE)
        options$set_tls_root_certs(tls_root_certs)
      }
    }

    # set extra header options if they are provided
    if (int_option != "") {
      verify_string("int_option", int_option, TRUE)
      new_int_option <- strsplit(int_option, ":", fixed = TRUE)
      options$add_int_option(new_int_option[1], as.numeric(new_int_option[2]))
    }

    if (string_option != "") {
      verify_string("string_option", string_option, TRUE)
      new_string_option <- strsplit(string_option, ":", fixed = TRUE)
      options$add_string_option(new_string_option[1], new_string_option[2])
    }

    if (extra_header != "") {
      verify_string("extra_header", extra_header, TRUE)
      new_extra_header <- strsplit(extra_header, ":", fixed = TRUE)
      options$add_extra_header(new_extra_header[1], new_extra_header[2])
    }

    internal_client <- new(INTERNAL_Client,
      target = target,
      client_options = options
    )
    return(new("Client", .internal_rcpp_object = internal_client))
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
  signature = c(client_instance = "Client"),
  function(client_instance, name) {
    verify_string("name", name, TRUE)
    if (!check_for_table(client_instance, name)) {
      stop(paste0("The table '", name, "' does not exist on the server."))
    }
    return(new("TableHandle", .internal_rcpp_object = client_instance@.internal_rcpp_object$open_table(name)))
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
  signature = c(client_instance = "Client"),
  function(client_instance, size) {
    verify_positive_int("size", size, TRUE)
    return(new("TableHandle", .internal_rcpp_object = client_instance@.internal_rcpp_object$empty_table(size)))
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
  signature = c(client_instance = "Client"),
  function(client_instance, period, start_time = 0) {
    verify_any_int("period", period, TRUE)
    verify_any_int("start_time", start_time, TRUE)
    return(new("TableHandle", .internal_rcpp_object = client_instance@.internal_rcpp_object$time_table(start_time, period)))
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
  signature = c(client_instance = "Client"),
  function(client_instance, table_object) {
    table_object_class <- class(table_object)

    if (table_object_class[[1]] == "data.frame") {
      rcpp_dh_table <- df_to_dh_table(client_instance, table_object)
    } else if (table_object_class[[1]] == "tbl_df") {
      rcpp_dh_table <- tibble_to_dh_table(client_instance, table_object)
    } else if (table_object_class[[1]] == "RecordBatchReader") {
      rcpp_dh_table <- rbr_to_dh_table(client_instance, table_object)
    } else if ((length(table_object_class) == 4 &&
      table_object_class[[1]] == "Table" &&
      table_object_class[[3]] == "ArrowObject")) {
      rcpp_dh_table <- arrow_to_dh_table(client_instance, table_object)
    } else {
      stop(paste0("'table_object' must be either an R Data Frame, a dplyr Tibble, an Arrow Table, or an Arrow Record Batch Reader. Got an object of class ", table_object_class[[1]], "."))
    }
    return(new("TableHandle", .internal_rcpp_object = rcpp_dh_table))
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
  signature = c(client_instance = "Client"),
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
  signature = c(con = "Client"),
  function(con) {
    con@.internal_rcpp_object$close()
    return(NULL)
  }
)
