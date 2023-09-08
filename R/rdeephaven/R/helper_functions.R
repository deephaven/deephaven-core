first_class <- function(arg) {
  return(class(arg)[[1]])
}

# if required_type is a list, this will not behave correctly because of R's type coercion rules
verify_type <- function(arg_name, candidate, required_type, message_type_name, is_scalar) {
  if (!is_scalar && (first_class(candidate) == "list")) {
    if (any(lapply(candidate, first_class) != required_type)) {
      stop(paste0("'", arg_name, "' must be a ", message_type_name, ", or a vector of ", message_type_name, "s. Got a vector with at least one element that is not a ", message_type_name, "."))
    }
  } else if (is_scalar && (first_class(candidate) == "list")) {
    if (first_class(candidate[[1]]) != required_type) {
      stop(paste0("'", arg_name, "' must be a single ", message_type_name, ". Got an object of class ", first_class(candidate), "."))
    } else if (length(candidate) != 1) {
      stop(paste0("'", arg_name, "' must be a single ", message_type_name, ". Got a vector of length ", length(candidate), "."))
    }
  } else if (first_class(candidate) != required_type) {
    if (!is_scalar) {
      stop(paste0("'", arg_name, "' must be a ", message_type_name, " or a vector of ", message_type_name, "s. Got an object of class ", first_class(candidate), "."))
    } else {
      stop(paste0("'", arg_name, "' must be a single ", message_type_name, ". Got an object of class ", first_class(candidate), "."))
    }
  } else if (is_scalar && (length(c(candidate)) != 1)) {
    stop(paste0("'", arg_name, "' must be a single ", message_type_name, ". Got a vector of length ", length(candidate), "."))
  }
}

# does not attempt to verify that candidate is numeric
verify_in_range <- function(arg_name, candidate, message, lb, ub, lb_open, ub_open) {
  if (((!is.null(lb)) && ((any(candidate <= lb) && (lb_open)) || (any(candidate < lb) && (!lb_open)))) ||
    ((!is.null(ub)) && ((any(candidate >= ub) && (ub_open)) || (any(candidate > ub) && (!ub_open))))) {
    if (length(candidate) == 1) {
      stop(paste0("'", arg_name, "' must be ", message, ". Got '", arg_name, "' = ", candidate, "."))
    } else {
      stop(paste0("Every element of '", arg_name, "' must be ", message, ". Got at least one element that is not ", message, "."))
    }
  }
}

# does not attempt to verify that candidate is numeric
verify_int <- function(arg_name, candidate) {
  if (candidate != as.integer(candidate)) {
    if (length(candidate == 1)) {
      stop(paste0("'", arg_name, "' must be an integer. Got '", arg_name, "' = ", candidate, "."))
    } else {
      stop(paste0("Every element of '", arg_name, "' must be an integer. Got at least one non-integer element."))
    }
  }
}

verify_string <- function(arg_name, candidate, is_scalar) {
  verify_type(arg_name, candidate, "character", "string", is_scalar)
}

verify_bool <- function(arg_name, candidate, is_scalar) {
  verify_type(arg_name, candidate, "logical", "boolean", is_scalar)
}

verify_numeric <- function(arg_name, candidate, is_scalar) {
  verify_type(arg_name, candidate, "numeric", "numeric", is_scalar)
}

verify_named_list <- function(arg_name, candidate) {
  if (first_class(candidate) != "list") {
    stop(paste0("'", arg_name, "' must be a named list. Got an object of class ", first_class(candidate), "."))
  } else if (length(names(candidate)) != length(candidate)) {
    stop(paste0("'", arg_name, "' must be a named list. Got a list with ", length(candidate), " elements and ", length(names(candidate)), " names."))
  }
}

verify_in_unit_interval <- function(arg_name, candidate, is_scalar) {
  verify_numeric(arg_name, candidate, is_scalar)
  verify_in_range(arg_name, candidate, message = "between 0 and 1 inclusive", lb = 0, ub = 1, lb_open = FALSE, ub_open = FALSE)
}

verify_any_int <- function(arg_name, candidate, is_scalar) {
  verify_numeric(arg_name, candidate, is_scalar)
  verify_int(arg_name, candidate)
}

verify_nonnegative_int <- function(arg_name, candidate, is_scalar) {
  verify_numeric(arg_name, candidate, is_scalar)
  verify_int(arg_name, candidate)
  verify_in_range(arg_name, candidate, message = "a nonnegative integer", lb = 0, ub = NULL, lb_open = FALSE, ub_open = TRUE)
}

verify_positive_int <- function(arg_name, candidate, is_scalar) {
  verify_numeric(arg_name, candidate, is_scalar)
  verify_int(arg_name, candidate)
  verify_in_range(arg_name, candidate, message = "a positive integer", lb = 0, ub = NULL, lb_open = TRUE, ub_open = TRUE)
}

strip_r6_wrapping <- function(r6_object) {
  return(r6_object$.internal_rcpp_object)
}
