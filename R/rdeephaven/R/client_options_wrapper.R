#' @title Deephaven ClientOptions
#' @description Client options provide a simple interface to the Deephaven server's authentication protocols.
#' This makes it easy to connect to a Deephaven server with any flavor of authentication, and shields the API from
#' any future changes to the underlying implementation.
#' 
#' Currently, three different kinds of authentication that a Deephaven server might be using are suported:
#' 
#' - "default": Default (or anonymous) authentication does not require any username or password. If
#'    running the Deephaven server locally, this is probably the kind of authentication needed.
#' 
#' - "basic": Basic authentication requires a standard username and password pair.
#' 
#' - "custom": Custom authentication requires general key-value pairs.
#' 
#' In addition to setting the authentication parameters when connecting to a client, a console can be
#' started in one of our supported server languages. Python and Groovy are currently supported, and the
#' user must ensure that the server being connected to was started with support for the desired console language.
#' 
#' @usage NULL
#' @format NULL
#' @docType class
#' 
#' @examples
#' 
#' # connect to a Deephaven server with a Python console running on "localhost:10000" using anonymous 'default' authentication
#' client_options <- ClientOptions$new()
#' client <- Client$new(target="localhost:10000", client_options=client_options)
#' 
#' # connect to a secure Deephaven server with a Groovy console using username/password authentication
#' client_options <- ClientOptions$new()
#' client_options$set_basic_authentication(username="user", password="p@ssw0rd123")
#' client_options$set_session_type("groovy")
#' client <- Client$new(target="url/to/secure/server", client_options=client_options)


ClientOptions <- R6Class("ClientOptions",
    public = list(

        #' @description
        #' Create a ClientOptions instance. This will default to using default (anonymous) authentication and a Python console.
        initialize = function() {
            self$internal_client_options <- new(INTERNAL_ClientOptions)
        },

        #' @description
        #' Use default (anonymous) authentication. If running a Deephaven server locally, this is likely the kind of authentication needed.
        set_default_authentication = function() {
            self$internal_client_options$set_default_authentication()
        },

        #' @description
        #' Use basic (username/password based) authentication.
        #' @param username Username of the account to use for authentication, supplied as a string.
        #' @param password Password of the account, supplied as a string.
        set_basic_authentication = function(username, password) {
            self$internal_client_options$set_basic_authentication(username, password)
        },

        #' @description
        #' Use custom (general key/value based) authentication.
        #' @param auth_key Key to use for authentication, supplied as a string.
        #' @param auth_value Value to use for authentication, supplied as a string.
        set_custom_authentication = function(auth_key, auth_value) {
            self$internal_client_options$set_custom_authentication(auth_key, auth_value)
        },

        #' @description
        #' Set the session type of the console (e.g., "python", "groovy", etc.). The session type must be supported on the server.
        #' @param session_type Desired language of the console. "python", "groovy", etc.
        set_session_type = function(session_type) {
            self$internal_client_options$set_session_type(session_type)
        },

        internal_client_options = NULL
    )
)
