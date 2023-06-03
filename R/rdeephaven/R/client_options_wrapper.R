#' @title Deephaven ClientOptions
#' @description Client options provide a simple interface to the Deephaven server's authentication protocols.
#' This makes it easy to connect to a Deephaven server with any flavor of authentication, and shields the API from
#' any future changes to the underlying implementation.
#' 
#' Currently, we support three different kinds of authentication that your Deephaven server might be using:
#' 
#' - "default": Default (or anonymous) authentication does not require any username or password. If you are
#'    running the Deephaven server locally, this is probably the kind of authentication you are using.
#' 
#' - "basic": Basic authentication requires a standard username and password pair.
#' 
#' - "custom": Custom authentication requires general key-value pairs.
#' 
#' In addition to setting your authentication parameters when you connect to a client, you can also start
#' a console in one of our supported server languages. We currently support Python and Groovy, and if you
#' want start a console upon client connection, you must ensure that the server you're connecting to was
#' started with support for the language you want to use.
#' 
#' @section Methods
#' 
#' - `$set_default_authentication()`
#' - `$set_basic_authentication()`
#' - `$set_custom_authentication()`
#' 
#' @examples
#' 
#' # connect to the Deephaven server running on "localhost:10000" with anonymous 'default' authentication
#' client_options <- ClientOptions$new()
#' client_options$set_default_authentication()
#' client <- Client$new(target="localhost:10000", client_options=client_options)


ClientOptions <- R6Class("ClientOptions",
    public = list(

        initialize = function() {
            self$internal_client_options <- new(INTERNAL_ClientOptions)
        },

        set_default_authentication = function() {
            self$internal_client_options$set_default_authentication()
        },

        set_basic_authentication = function(username, password) {
            self$internal_client_options$set_basic_authentication(username, password)
        },

        set_custom_authentication = function(auth_key, auth_value) {
            self$internal_client_options$set_custom_authentication(auth_key, auth_value)
        },

        set_session_type = function(session_type) {
            self$internal_client_options$set_session_type(session_type)
        },

        internal_client_options = NULL
    )
)