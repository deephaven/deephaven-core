first_class <- function(arg) class(arg)[[1]]

verify_internal_type <- function(desired_type, arg_name, candidate) {
  if ((first_class(candidate) == "list") && (any(lapply(candidate, first_class) != desired_type))) {
    stop(paste0("'", arg_name, "' must be a Deephaven ", desired_type, ", or a vector of ", desired_type, "s. Got a vector with at least one element that is not a Deephaven ", desired_type, " instead."))
  } else if ((first_class(candidate) != "list") && (first_class(candidate) != desired_type)) {
    stop(paste0("'", arg_name, "' must be a Deephaven ", desired_type, ", or a vector of ", desired_type, "s. Got an object of class ", first_class(candidate), " instead."))
  }
}

verify_bool <- function(arg_name, bool_candidate) {
  if (first_class(bool_candidate) != "logical") {
    stop(paste0("'", arg_name, "' must be passed as a single boolean. Got an object of class ", first_class(bool_candidate), " instead."))
  } else if (length(bool_candidate) != 1) {
    stop(paste0("'", arg_name, "' must be passed as a single boolean. Got a boolean vector of length ", length(bool_candidate), " instead."))
  }
}

verify_bool_vector <- function(arg_name, bool_candidate) {
  if (first_class(bool_candidate) != "logical") {
    stop(paste0("'", arg_name, "' must be passed as a boolean or a vector of booleans. Got an object of class ", first_class(bool_candidate), " instead."))
  }
}

verify_int <- function(arg_name, int_candidate, type = "any") {
  if (type == "any") {
    message <- " an "
  } else if (type == "nonnegative") {
    message <- " a non-negative "
  } else if (type == "positive") {
    message <- " a positive "
  }
  if (class(int_candidate)[[1]] != "numeric") {
    stop(paste0("'", arg_name, "' must be", message, "integer. Got an object of class ", first_class(int_candidate), " instead."))
  } else if (all.equal(int_candidate, as.integer(int_candidate)) != TRUE) {
    # must use != TRUE as the result of all.equal() is not strictly boolean
    stop(paste0("'", arg_name, "' must be", message, "integer. Got a non-integer numeric type instead."))
  } else if (length(int_candidate) != 1) {
    stop(paste0("'", arg_name, "' must be", message, "integer. Got a numeric vector of length ", length(int_candidate), " instead."))
  }
  if (((type == "nonnegative") && (int_candidate < 0)) || ((type == "positive") && (int_candidate <= 0))) {
    stop(paste0("'", arg_name, "' must be", message, "integer. Got ", int_candidate, " instead."))
  }
}

verify_proportion <- function(arg_name, prop_candidate) {
  if (first_class(prop_candidate) != "numeric") {
    stop(paste0("'", arg_name, "' must be a numeric type between 0 and 1 inclusive. Got an object of class ", first_class(prop_candidate), " instead."))
  } else if (length(prop_candidate) != 1) {
    stop(paste0("'", arg_name, "' must be a numeric type between 0 and 1 inclusive. Got a numeric vector of length ", length(prop_candidate), " instead."))
  } else if ((prop_candidate < 0.0) || (prop_candidate > 1.0)) {
    stop(paste0("'", arg_name, "' must be a numeric type between 0 and 1 inclusive. Got a value of ", prop_candidate, " instead."))
  }
}

verify_string <- function(arg_name, string_candidate) {
  if (first_class(string_candidate) != "character") {
    stop(paste0("'", arg_name, "' must be passed as a single string. Got an object of class ", first_class(string_candidate), " instead."))
  } else if (length(string_candidate) != 1) {
    stop(paste0("'", arg_name, "' must be passed as a single string. Got a character vector of length ", length(string_candidate), " instead."))
  }
}

verify_string_vector <- function(arg_name, string_vector_candidate) {
  if (first_class(string_vector_candidate) != "character") {
    stop(paste0("'", arg_name, "' must be passed as a string or a vector of strings. Got an object of class ", first_class(string_vector_candidate), " instead."))
  }
}

strip_r6_wrapping_from_aggregation <- function(r6_aggregation) {
  return(r6_aggregation$internal_aggregation)
}
