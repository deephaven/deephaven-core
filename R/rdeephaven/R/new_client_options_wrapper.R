#' @export
setClass(
  "S4ClientOptions",
  representation(
    .internal_rcpp_object = "Rcpp_INTERNAL_ClientOptions"
  )
)

#' @export
setMethod(
  "initialize", "S4ClientOptions",
  function(.Object) {
    internal_client_options <- new(INTERNAL_ClientOptions)
    .Object <- callNextMethod(.Object,
      .internal_rcpp_object = internal_client_options)
    return(.Object)
  }
)

# ALL OF THE FOLLOWING METHODS ARE CURRENTLY ONLY CALLED FOR THEIR SIDE-EFFECTS

setGeneric(
  "set_default_authentication",
  function(client_options_instance) {
    return(standardGeneric("set_default_authentication"))
  },
  signature = c("client_options_instance")
)

#' @export
setMethod(
  "set_default_authentication",
  signature = c(client_options_instance = "S4ClientOptions"),
  function(client_options_instance) {
    return(client_options_instance@.internal_rcpp_object$set_default_authentication())
  }
)

setGeneric(
  "set_basic_authentication",
  function(client_options_instance, username, password) {
    return(standardGeneric("set_basic_authentication"))
  },
  signature = c("client_options_instance", "username", "password")
)

#' @export
setMethod(
  "set_basic_authentication",
  signature = c(client_options_instance = "S4ClientOptions"),
  function(client_options_instance, username, password) {
    verify_string("username", username, TRUE)
    verify_string("password", password, TRUE)
    return(client_options_instance@.internal_rcpp_object$set_basic_authentication(username, password))
  }
)

setGeneric(
  "set_custom_authentication",
  function(client_options_instance, auth_key, auth_value) {
    return(standardGeneric("set_custom_authentication"))
  },
  signature = c("client_options_instance", "auth_key", "auth_value")
)

#' @export
setMethod(
  "set_custom_authentication",
  signature = c(client_options_instance = "S4ClientOptions"),
  function(client_options_instance, auth_key, auth_value) {
    verify_string("auth_key", auth_key, TRUE)
    verify_string("auth_value", auth_value, TRUE)
    return(client_options_instance@.internal_rcpp_object$set_custom_authentication(auth_key, auth_value))
  }
)

setGeneric(
  "set_session_type",
  function(client_options_instance, session_type) {
    return(standardGeneric("set_session_type"))
  },
  signature = c("client_options_instance", "session_type")
)

#' @export
setMethod(
  "set_session_type",
  signature = c(client_options_instance = "S4ClientOptions"),
  function(client_options_instance, session_type) {
    verify_string("session_type", session_type, TRUE)
    return(client_options_instance@.internal_rcpp_object$set_session_type(session_type))
  }
)

setGeneric(
  "use_tls",
  function(client_options_instance, ...) {
    return(standardGeneric("use_tls"))
  },
  signature = "client_options_instance"
)

#' @export
setMethod(
  "use_tls",
  signature = c(client_options_instance = "S4ClientOptions"),
  function(client_options_instance, root_certs = "") {
    verify_string("root_certs", root_certs, TRUE)
    return(client_options_instance@.internal_rcpp_object$set_tls_root_certs(root_certs))
  }
)

setGeneric(
  "add_int_option",
  function(client_options_instance, opt, val) {
    return(standardGeneric("add_int_option"))
  },
  signature = c("client_options_instance", "opt", "val")
)

#' @export
setMethod(
  "add_int_option",
  signature = c(client_options_instance = "S4ClientOptions"),
  function(client_options_instance, opt, val) {
    verify_string("opt", opt, TRUE)
    verify_any_int("val", val, TRUE)
    return(client_options_instance@.internal_rcpp_object$add_int_option(opt, val))
  }
)

setGeneric(
  "add_string_option",
  function(client_options_instance, opt, val) {
    return(standardGeneric("add_string_option"))
  },
  signature = c("client_options_instance", "opt", "val")
)

#' @export
setMethod(
  "add_string_option",
  signature = c(client_options_instance = "S4ClientOptions"),
  function(client_options_instance, opt, val) {
    verify_string("opt", opt, TRUE)
    verify_string("val", val, TRUE)
    return(client_options_instance@.internal_rcpp_object$add_string_option(opt, val))
  }
)

setGeneric(
  "add_extra_header",
  function(client_options_instance, header_name, header_val) {
    return(standardGeneric("add_extra_header"))
  },
  signature = c("client_options_instance", "header_name", "header_val")
)

#' @export
setMethod(
  "add_extra_header",
  signature = c(client_options_instance = "S4ClientOptions"),
  function(client_options_instance, header_name, header_val) {
    verify_string("header_name", header_name, TRUE)
    verify_string("header_value", header_value, TRUE)
    return(client_options_instance@.internal_rcpp_object$add_extra_header(opt, val))
  }
)
