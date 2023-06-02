NewClient <- R6Class("Client",
    public = list(

        initialize = function(target, sessionType="python", authType="default", credentials=list(key="", value="")) {

            # first, type checking
            private$verify_string(target, "target")
            private$verify_string(sessionType, "sessionType")
            private$verify_string(authType, "authType")

            if (!(class(credentials) == "list" && length(credentials) == 2 &&
             class(credentials[[1]]) == "character" && class(credentials[[2]]) == "character")) {
                stop("'credentials' must be passed as a list of two strings representing a key/value pair for authentication.")
            }

            # now value checking
            if (!(sessionType == "python" || sessionType == "groovy")) {
                warning("The session type you provided is not currently supported. Defaulting to 'python'.")
            }
            if (!(authType == "default" || authType == "basic" || authType == "custom")) {
                warning("The authentication type you provided is not currently supported. Defaulting to 'default'.")
            }

            private$internal_client <- new(INTERNAL_Client, target=target, sessionType=sessionType,
                                           authType=authType, key=credentials[[1]], value=credentials[[2]])
        },

        # retrieve a TableHandle reference to a named table on the server
        open_table = function(tableName) {
            private$verify_string(tableName, "tableName")
            if (!private$check_for_table(tableName)) {
                stop("The table you're trying to pull does not exist on the server.")
            }
            this_dh_table_handle = private$internal_client$open_table(tableName)
            return(NewTableHandle$new(this_dh_table_handle))
        },

        # import an R object to a table to the server, and retrieve a TableHandle reference to that table
        import_table = function(tableObject) {
            if (!(class(tableObject)[[1]] == "data.frame" ||
                  class(tableObject)[[1]] == "RecordBatchReader" ||
                  (length(class(tableObject)) == 4 &&
                   class(tableObject)[[1]] == "Table" &&
                   class(tableObject)[[3]] == "ArrowObject"))) {
                stop("'tableObject' must be either an R Data Frame, an Arrow Table, or an Arrow Record Batch Reader.")
            }
            if (class(tableObject)[[1]] == "data.frame") {
                this_dh_table_handle = private$df_to_dh_table(tableObject)
            }
            else if (class(tableObject)[[1]] == "RecordBatchReader") {
                this_dh_table_handle = private$rbr_to_dh_table(tableObject)
            }
            else if ((length(class(tableObject)) == 4 &&
                      class(tableObject)[[1]] == "Table" &&
                      class(tableObject)[[3]] == "ArrowObject")) {
                this_dh_table_handle = private$arrow_to_dh_table(tableObject)
            }
            else {
                stop("An unexpected type was passed to 'push' and made it through our stop handling. 'tableObject' must be either an R Data Frame, an Arrow Table, or an Arrow Record Batch Reader.")
            }
            return(NewTableHandle$new(this_dh_table_handle))
        },

        run_script = function(script) {
            private$verify_string(script, "script")
            private$internal_client$run_script(script)
        }
    ),
    private = list(

        internal_client = NULL,

        check_for_table = function(tableName) {
            return(private$internal_client$check_for_table(tableName))
        },

        verify_string = function(stringCandidate, argName) {
            if (class(stringCandidate) != "character") {
                stop(paste0("'", argName, "' must be passed as a string."))
            }
        },

        rbr_to_dh_table = function(rbr) {
            ptr = private$internal_client$new_arrowArrayStream_ptr()
            rbr$export_to_c(ptr)
            dh_table = private$internal_client$new_table_from_arrowArrayStream_ptr(ptr)
            return(dh_table)
        },

        arrow_to_dh_table = function(arrowTable) {
            rbr = as_record_batch_reader(arrowTable)
            dh_table = private$rbr_to_dh_table(rbr)
            return(dh_table)
        },

        df_to_dh_table = function(dataFrame) {
            arrow_tbl = arrow_table(dataFrame)
            dh_table = private$arrow_to_dh_table(arrow_tbl)
            return(dh_table)
        }
    )
)