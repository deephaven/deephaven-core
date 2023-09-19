#' @title The Deephaven Client
#' @description
#' A Client is the entry point for interacting with the Deephaven server. It is used to create new tables,
#' import data to and export data from the server, and run queries on the server.
#'
#' @usage NULL
#' @format NULL
#' @docType class
#' @export
Client <- R6Class("Client",
  cloneable = FALSE,
  public = list(
    .internal_rcpp_object = NULL,

    #' @description
    #' Calls `initialize_for_xptr` if the first argument is an external pointer, and `initialize_for_target` if the
    #' first argument is a string. In the latter case, the remaining keyword arguments are passed to `initialize_for_target`.
    #' @param ... Either an external pointer to an existing client connection, or a string denoting the address
    #' of a running Deephaven server followed by keyword arguments to `initialize_from_target`.
    initialize = function(...) {
      args <- list(...)
      if (length(args) == 1) {
        first_arg <- args[[1]]
        first_arg_class = first_class(first_arg)
        if (first_arg_class != "character" && first_arg_class != "list") {
          if (first_arg_class != "externalptr") {
            stop(paste0(
              "Client initialize first argument must be ",
              "either a string or an Rcpp::XPtr object."))
          }
          return(self$initialize_for_xptr(first_arg))
        }
      }
      return(do.call(self$initialize_for_target, args))
    },
    
    #' @description
    #' Initializes a Client object using a pointer to an existing client connection.
    #' @param xptr External pointer to an existing client connection.
    initialize_for_xptr = function(xptr) {
      verify_type("xptr", xptr, "externalptr", "XPtr", TRUE)
      self$.internal_rcpp_object = new(INTERNAL_Client, xptr)
    },

    #' @description
    #' Initializes a Client object and connects to a Deephaven server.
    #' @param target String denoting the address of a Deephaven server, formatted as `"ip:port"`.
    #' @param auth_type String denoting the authentication type. Can be `"anonymous"`, `"basic"`,
    #' or any custom-built authenticator supported by the server, such as `"io.deephaven.authentication.psk.PskAuthenticationHandler"`.
    #' Default is `anonymous`.
    #' @param username String denoting the username, which only applies if `auth_type` is `basic`.
    #' Username and password should not be used in conjunction with `auth_token`. Defaults to an empty string.
    #' @param password String denoting the password, which only applies if `auth_type` is `basic`.
    #' Username and password should not be used in conjunction with `auth_token`. Defaults to an empty string.
    #' @param auth_token String denoting the authentication token. When `auth_type`
    #' is `anonymous`, it will be ignored; when `auth_type` is `basic`, it must be
    #' `"user:password"` or left blank; when `auth_type` is a custom-built authenticator, it must
    #' conform to the specific requirement of that authenticator. This should not be used
    #' in conjunction with `username` and `password`. Defaults to an empty string.
    #' @param session_type String denoting the session type supported on the server.
    #' Currently, `python` and `groovy` are supported. Defaults to `python`.
    #' @param use_tls Whether or not to use a TLS connection. Defaults to `FALSE`.
    #' @param tls_root_certs String denoting PEM encoded root certificates to use for TLS connection,
    #' or `""` to use system defaults. Only used if `use_tls == TRUE`. Defaults to system defaults.
    #' @param int_options List of name-value pairs for int-valued options to the underlying
    #' grpc channel creation. Defaults to an empty list, which implies not using any channel options.
    #' @param string_options List of name-value pairs for string-valued options to the underlying
    #' grpc channel creation. Defaults to an empty list, which implies not using any channel options.
    #' @param extra_headers List of name-value pairs for additional headers and values
    #' to add to server requests. Defaults to an empty list, which implies not using any extra headers.
    initialize_for_target = function(
                          target,
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
          user_pass_token <- paste(username, ":", password, sep = "")
          options$set_basic_authentication(user_pass_token)
        } else if (((username == "") && (password == "")) && (auth_token != "")) {
          verify_string("auth_token", auth_token, TRUE)
          options$set_basic_authentication(auth_token)
        } else if (((username != "") || (password != "")) && (auth_token != "")) {
          stop("Basic authentication was requested, but 'auth_token' was provided, as well as least one of 'username' and 'password'. Please provide either 'username' and 'password', or 'auth_token'.")
        } else {
          stop("Basic authentication was requested, but 'auth_token' was not provided, and at most one of 'username' or 'password' was provided. Please provide either 'username' and 'password', or 'auth_token'.")
        }
      } else if (auth_type == "psk") {
        if (auth_token != "") {
          verify_string("auth_token", auth_token, TRUE)
          options$set_custom_authentication("io.deephaven.authentication.psk.PskAuthenticationHandler", auth_token)
        } else {
          stop("Pre-shared key authentication was requested, but no 'auth_token' was provided.")
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
        options$set_use_tls(TRUE)
        if (tls_root_certs != "") {
          verify_string("tls_root_certs", tls_root_certs, TRUE)
          options$set_tls_root_certs(tls_root_certs)
        }
      }

      # set extra header options if they are provided
      if (length(int_options) != 0) {
        verify_named_list("int_options", int_options)
        for (key in names(int_options)) {
          verify_string("key", key, TRUE)
          verify_any_int("value", int_options[[key]], TRUE)
          options$add_int_options(key, int_options[[key]])
        }
      }

      if (length(string_options) != 0) {
        verify_named_list("string_options", string_options)
        for (key in names(string_options)) {
          verify_string("key", key, TRUE)
          verify_string("value", string_options[[key]], TRUE)
          options$add_string_options(key, string_options[[key]])
        }
      }

      if (length(extra_headers) != 0) {
        verify_named_list("extra_headers", extra_headers)
        for (key in names(extra_headers)) {
          verify_string("key", key, TRUE)
          verify_string("value", extra_headers[[key]], TRUE)
          options$add_extra_header(key, extra_headers[[key]])
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

      self$.internal_rcpp_object <- new(INTERNAL_Client,
        target = target,
        client_options = options
      )
    },

    #' @description
    #' Creates an empty table on the server with 'size' rows and no columns.
    #' @param size Non-negative integer specifying the number of rows for the new table.
    #' @return TableHandle reference to the new table.
    empty_table = function(size) {
      verify_nonnegative_int("size", size, TRUE)
      return(TableHandle$new(self$.internal_rcpp_object$empty_table(size)))
    },

    #' @description
    #' Creates a ticking table on the server.
    #' @param period ISO-8601-formatted string specifying the update frequency of the new table.
    #' @param start_time Optional ISO-8601-formatted string specifying the start time of the table.
    #' Defaults to now.
    #' @return TableHandle reference to the new table.
    time_table = function(period, start_time = "now") {
      verify_string("period", period, TRUE)
      verify_string("start_time", start_time, TRUE)
      return(TableHandle$new(self$.internal_rcpp_object$time_table(period, start_time)))
    },

    #' @description
    #' Retrieves a reference to a named table on the server using its name.
    #' @param name String denoting the name of the table to retrieve.
    #' @return TableHandle reference to the named table.
    open_table = function(name) {
      verify_string("name", name, TRUE)
      if (!private$check_for_table(name)) {
        stop(paste0("The table '", name, "' does not exist on the server."))
      }
      return(TableHandle$new(self$.internal_rcpp_object$open_table(name)))
    },

    #' @description
    #' Imports a new table to the Deephaven server. Note that this new table is not automatically bound to
    #' a variable name on the server. See `?TableHandle` for more information.
    #' @param table_object R Data Frame, dplyr Tibble, Arrow Table, Arrow RecordBatchReader, or other supported table
    #' containing the data to import to the server.
    #' @return TableHandle reference to the new table.
    import_table = function(table_object) {
      table_object_class <- class(table_object)
      if (table_object_class[[1]] == "data.frame") {
        return(TableHandle$new(private$df_to_dh_table(table_object)))
      } else if (table_object_class[[1]] == "tbl_df") {
        return(TableHandle$new(private$tibble_to_dh_table(table_object)))
      } else if (table_object_class[[1]] == "RecordBatchReader") {
        return(TableHandle$new(private$rbr_to_dh_table(table_object)))
      } else if ((length(table_object_class) == 4 &&
        table_object_class[[1]] == "Table" &&
        table_object_class[[3]] == "ArrowObject")) {
        return(TableHandle$new(private$arrow_to_dh_table(table_object)))
      } else {
        stop(paste0("'table_object' must be a single data frame, tibble, arrow table, or record batch reader. Got an object of class ", table_object_class[[1]], "."))
      }
    },

    #' @description
    #' Retrieves a reference to a named table in the server using its Arrow Flight ticket.
    #' @param ticket String denoting the Arrow Flight ticket.
    #' @return TableHandle reference to the table.
    ticket_to_table = function(ticket) {
      verify_string("ticket", ticket, TRUE)
      return(TableHandle$new(self$.internal_rcpp_object$make_table_handle_from_ticket(ticket)))
    },

    #' @description
    #' Runs a script on the server. The script must be in the language that the server console was started with.
    #' @param script String containing the code to be executed on the server.
    run_script = function(script) {
      verify_string("script", script, TRUE)
      self$.internal_rcpp_object$run_script(script)
    },

    #' @description
    #' Closes the client connection. After this method is called, any further server calls will
    #' be undefined and will likely result in an error.
    close = function() {
      self$.internal_rcpp_object$close()
    }
  ),
  private = list(
    check_for_table = function(name) {
      return(self$.internal_rcpp_object$check_for_table(name))
    },
    rbr_to_dh_table = function(rbr) {
      ptr <- self$.internal_rcpp_object$new_arrow_array_stream_ptr()
      rbr$export_to_c(ptr)
      return(self$.internal_rcpp_object$new_table_from_arrow_array_stream_ptr(ptr))
    },
    arrow_to_dh_table = function(arrow_tbl) {
      rbr <- as_record_batch_reader(arrow_tbl)
      return(private$rbr_to_dh_table(rbr))
    },
    tibble_to_dh_table = function(tibbl) {
      arrow_tbl <- arrow_table(tibbl)
      return(private$arrow_to_dh_table(arrow_tbl))
    },
    df_to_dh_table = function(data_frame) {
      arrow_tbl <- arrow_table(data_frame)
      return(private$arrow_to_dh_table(arrow_tbl))
    }
  )
)
