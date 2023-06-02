#' @title Deephaven Client class
#' @description The Deephaven Client class is responsible for establishing and maintaining
#' a connection to a running Deephaven server and facilitating basic server requests. This is
#' the primary interface for interacting with the Deephaven server from R.
#' 
#' @section Connecting to the Server via Client
#' 
#' To connect to a Deephaven server, you must create a new Client object with the `new` function,
#' accompanied by these five string arguments:
#' 
#' - `target`: The URL that the Deephaven server is running on.
#' - `session_type`: If you started a Python or Groovy server, you can start up a corresponding
#'    console here with "python" or "groovy". Otherwise, use "none".
#' - `auth_type`: The type of authentication your server is using. Can be "default", "basic", or "custom".
#' - `key`: The key credential in a key/value pair. For basic authentication, this is a username.
#'    For custom authentication, it is a general key. For default (anonymous) authentication, use "".
#' - `value`: The value credential in a key/value pair. For basic authentication, this is a password.
#'    For custom authentication, it is a  general value. For default (anonymous) authentication, use "".
#' 
#' It is important that every one of these arguments be provided explicitly. We will provide optional and
#' default arguments in a coming release, but for the moment we require the user to provide all arguments.
#' Once the server connection has been established, you have a handful of key methods to handle basic server
#' requests.
#' 
#' @section Methods
#' 
#' - `$open_table(tableName)`: Looks for a table named 'tableName' on the server, and returns a Deephaven TableHandle
#'    reference to that table if it exists.
#' - `$delete_table(tableName)`: Looks for a table named 'tableName' on the server, and deletes it if it exists.
#'    Importantly, this is only effective 
#' - `check_for_table`: 
#' - `run_script`: