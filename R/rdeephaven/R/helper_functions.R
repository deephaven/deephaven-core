.verify_string <- function(arg_name, string_candidate) {
    if (class(string_candidate)[[1]] != "character") {
        stop(paste0("'", arg_name, "' must be passed as a single string. Got object of class ", class(string_candidate)[[1]], " instead."))
    } else if (length(string_candidate) != 1) {
        stop(paste0("'", arg_name, "' must be passed as a single string. Got character vector of length ", length(string_candidate), " instead."))
    }
}