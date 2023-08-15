#' @export
setClass(
  "Client",
  representation(
    .internal_rcpp_object = "Rcpp_INTERNAL_Client"
  )
)

setGeneric(
  "dhConnect",
  function(target, ...) {
    return(standardGeneric("dhConnect"))
  },
  signature = c("target")
)

#' @export
setMethod(
  "dhConnect",
  signature = c(target = "character"),
  function(target,
           auth_type = "anonymous",
           username = "",
           password = "",
           auth_token = "",
           session_type = "python",
           use_tls = FALSE,
           tls_root_certs = "",
           int_options = list(),
           string_options = list(),
           extra_headers = list()) {
    options <- new(INTERNAL_ClientOptions)

    verify_string("target", target, TRUE)
    verify_string("auth_type", auth_type, TRUE)
    if (auth_type == "") {
      stop("'auth_type' should be a non-empty string.")
    }
    verify_bool("use_tls", use_tls, TRUE)

    # check if auth_type needs to be changed and set credentials accordingly
    if (auth_type == "anonymous") {
      options$set_default_authentication()
    } else if (auth_type == "basic") {
      if (((username != "") && (password != "")) && (auth_token == "")) {
        verify_string("username", username, TRUE)
        verify_string("password", password, TRUE)
        user_pass_token = paste(username, ":", password, sep = "")
        options$set_basic_authentication(user_pass_token)
      } else if (((username == "") && (password == "")) && (auth_token != "")) {
        verify_string("auth_token", auth_token, TRUE)
        options$set_basic_authentication(auth_token)
      } else if (((username != "") || (password != "")) && (auth_token != "")) {
        stop("Basic authentication was requested, but 'auth_token' was provided, as well as least one of 'username' and 'password'. Please provide either 'username' and 'password', or 'auth_token'.")
      } else {
        stop("Basic authentication was requested, but 'auth_token' was not provided, and at most one of 'username' or 'password' was provided. Please provide either 'username' and 'password', or 'auth_token'.")
      }
    } else {
      if (auth_token != "") {
        verify_string("auth_token", auth_token, TRUE)
        options$set_custom_authentication(auth_type, auth_token)
      } else {
        stop("Custom authentication was requested, but no 'auth_token' was provided.")
      }
    }

    # set session type if a valid session type is provided
    if ((session_type == "python") || (session_type == "groovy")) {
      options$set_session_type(session_type)
    } else {
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
    if (length(int_options) != 0) {
      verify_list("int_options", int_options, TRUE)
      for (key in names(int_options)) {
        verify_string("key", key, TRUE)
        verify_int("value", int_options[[key]], TRUE)
        options$add_int_options(key, int_options[[key]])
      }
    }

    if (length(string_options) != 0) {
      verify_list("string_options", string_options, TRUE)
      for (key in names(string_options)) {
        verify_string("key", key, TRUE)
        verify_string("value", string_options[[key]], TRUE)
        options$add_string_options(key, string_options[[key]])
      }
    }

    if (length(extra_headers) != 0) {
      verify_list("extra_headers", extra_headers, TRUE)
      for (key in names(extra_headers)) {
        verify_string("key", key, TRUE)
        verify_string("value", extra_headers[[key]], TRUE)
        options$add_extra_headers(key, extra_headers[[key]])
      }
    }

    if ((auth_token != "") && (auth_type == "anonymous")) {
      warning("'auth_token' was set but it will not be used, as 'auth_type' is 'anonymous'.")
    }
    
    if (((username != "") || (password != "")) && auth_type != "basic") {
      warning("At least one of 'username' and 'password' were set but they will not be used, as 'auth_type' is not 'basic'.")
    }

    if ((tls_root_certs != "") && (use_tls == FALSE)) {
      warning("'tls_root_certs' was set but it will not be used, as 'use_tls' is FALSE.")
    }

    internal_client <- new(INTERNAL_Client,
      target = target,
      client_options = options
    )
    return(new("Client", .internal_rcpp_object = internal_client))
  }
)

### HELPER FUNCTIONS ###

check_for_table <- function(client, name) {
  return(client@.internal_rcpp_object$check_for_table(name))
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
  signature = c(client_instance = "Client", name = "character"),
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
  signature = c(client_instance = "Client", size = "numeric"),
  function(client_instance, size) {
    verify_nonnegative_int("size", size, TRUE)
    return(new("TableHandle", .internal_rcpp_object = client_instance@.internal_rcpp_object$empty_table(size)))
  }
)

setGeneric(
  "time_table",
  function(client_instance, period, ...) {
    return(standardGeneric("time_table"))
  },
  signature = c("client_instance", "period")
)

#' @export
setMethod(
  "time_table",
  signature = c(client_instance = "Client", period = "numeric"),
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
  signature = c(client_instance = "Client", table_object = "RecordBatchReader"),
  function(client_instance, table_object) {
    ptr <- client_instance@.internal_rcpp_object$new_arrow_array_stream_ptr()
    table_object$export_to_c(ptr)
    return(
      new("TableHandle",
        .internal_rcpp_object = client_instance@.internal_rcpp_object$new_table_from_arrow_array_stream_ptr(ptr)
      )
    )
  }
)

#' @export
setMethod(
  "import_table",
  signature = c(client_instance = "Client", table_object = "Table"),
  function(client_instance, table_object) {
    return(import_table(client_instance, as_record_batch_reader(table_object)))
  }
)

#' @export
setMethod(
  "import_table",
  signature = c(client_instance = "Client", table_object = "tbl_df"),
  function(client_instance, table_object) {
    return(import_table(client_instance, arrow_table(table_object)))
  }
)

#' @export
setMethod(
  "import_table",
  signature = c(client_instance = "Client", table_object = "data.frame"),
  function(client_instance, table_object) {
    return(import_table(client_instance, arrow_table(table_object)))
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
  signature = c(client_instance = "Client", script = "character"),
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
