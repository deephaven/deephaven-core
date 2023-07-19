first_class = function(arg) {
    return(class(arg)[[1]])
}

verify_string <- function(arg_name, string_candidate) {
    if (first_class(string_candidate) != "character") {
        stop(paste0("'", arg_name, "' must be passed as a single string. Got an object of class ", first_class(string_candidate), " instead."))
    }
    else if (length(string_candidate) != 1) {
        stop(paste0("'", arg_name, "' must be passed as a single string. Got a character vector of length ", length(string_candidate), " instead."))
    }
}

verify_string_vector <- function(arg_name, string_vector_candidate) {
    if (first_class(string_candidate) != "character") {
        stop(paste0("'", arg_name, "' must be passed as a string or a vector of strings. Got an object of class ", first_class(string_candidate), " instead."))
}

verify_int <- function(arg_name, int_candidate) {
    if (class(int_candidate)[[1]] != "numeric") {
        stop(paste0("'", arg_name, "' must be an integer. Got an object of class ", first_class(int_candidate), " instead."))
    }
    else if (all.equal(int_candidate, as.integer(int_candidate)) != TRUE) {
        # must use != TRUE as the result of all.equal() is not strictly boolean
        stop(paste0("'", arg_name, "' must be an integer. Got a non-integer numeric type instead."))
    }
    else if (length(int_candidate) != 1) {
        stop(paste0("'", arg_name, "' must be an integer. Got a numeric vector of length ", length(int_candidate), " instead."))
    }
}