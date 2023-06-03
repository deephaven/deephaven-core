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