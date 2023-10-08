first_class <- function(arg) {
  return(class(arg)[[1]])
}

vector_wrong_element_type_or_value <- function(arg_name, candidate, type_message, descriptor_message) {
  stripped_type_message = sub(".*? ", "", type_message)
  return(paste0("'", arg_name, "' must be ", type_message, ", or a vector of ", stripped_type_message, "s", descriptor_message, ". Got a vector with at least one element that is not ", type_message, descriptor_message, "."))
}
vector_wrong_type <- function(arg_name, candidate, type_message, descriptor_message) {
  stripped_type_message = sub(".*? ", "", type_message)
  return(paste0("'", arg_name, "' must be ", type_message, " or a vector of ", stripped_type_message, "s", descriptor_message, ". Got an object of class ", first_class(candidate), "."))
}
vector_needed_scalar <- function(arg_name, candidate, type_message, descriptor_message) {
  stripped_type_message = sub(".*? ", "", type_message)
  return(paste0("'", arg_name, "' must be a single ", stripped_type_message, descriptor_message, ". Got a vector of length ", length(candidate), "."))
}
scalar_wrong_type <- function(arg_name, candidate, type_message, descriptor_message) {
  stripped_type_message = sub(".*? ", "", type_message)
  return(paste0("'", arg_name, "' must be a single ", stripped_type_message, descriptor_message, ". Got an object of class ", first_class(candidate), "."))
}
scalar_wrong_value <- function(arg_name, candidate, type_message, descriptor_message) {
  stripped_type_message = sub(".*? ", "", type_message)
  return(paste0("'", arg_name, "' must be a single ", stripped_type_message, descriptor_message, ". Got '", arg_name, "' = ", candidate, "."))
}

# if required_type is a list, this will not behave correctly because of R's type coercion rules
verify_type <- function(arg_name, candidate, is_scalar, required_type, type_message, descriptor_message = "") {

  if (!is_scalar && (first_class(candidate) == "list")) {
    if (any(lapply(candidate, first_class) != required_type)) {
      stop(vector_wrong_element_type_or_value(arg_name, candidate, type_message, descriptor_message))
    }
  } else if (is_scalar && (first_class(candidate) == "list")) {
    if (first_class(candidate[[1]]) != required_type) {
      stop(scalar_wrong_type(arg_name, candidate, type_message, descriptor_message))
    } else if (length(candidate) != 1) {
      stop(vector_needed_scalar(arg_name, candidate, type_message, descriptor_message))
    }
  } else if (first_class(candidate) != required_type) {
    if (!is_scalar) {
      stop(vector_wrong_type(arg_name, candidate, type_message, descriptor_message))
    } else {
      stop(scalar_wrong_type(arg_name, candidate, type_message, descriptor_message))
    }
  } else if (is_scalar && (length(c(candidate)) != 1)) {
    stop(vector_needed_scalar(arg_name, candidate, type_message, descriptor_message))
  }
}

# does not attempt to verify that candidate is numeric, intended to be used after `verify_type()`
verify_int <- function(arg_name, candidate, is_scalar, type_message, descriptor_message = "") {

  if (is_scalar && (length(c(candidate)) != 1)) {
    stop(vector_needed_scalar(arg_name, candidate, type_message, descriptor_message))
  } else if (candidate != as.integer(candidate)) {
    if (!is_scalar) {
      stop(vector_wrong_element_type_or_value(arg_name, candidate, type_message, descriptor_message))
    } else {
      stop(scalar_wrong_value(arg_name, candidate, type_message, descriptor_message))
    }
  }
}

# does not attempt to verify that candidate is numeric, intended to be used after `verify_type()`
verify_in_range <- function(arg_name, candidate, is_scalar, type_message, descriptor_message, lb, ub, lb_open, ub_open) {

  if (is_scalar && (length(c(candidate)) != 1)) {
    stripped_type_message = sub(".*? ", "", type_message)
    stop(paste0("Every element of '", arg_name, "' must be ", stripped_type_message, range_message, ". Got at least one element that is not ", stripped_type_message, range_message, "."))
  }
  else if (((!is.null(lb)) && ((any(candidate <= lb) && (lb_open)) || (any(candidate < lb) && (!lb_open)))) ||
    ((!is.null(ub)) && ((any(candidate >= ub) && (ub_open)) || (any(candidate > ub) && (!ub_open))))) {
    if (!is_scalar) {
      stop(vector_wrong_element_type_or_value(arg_name, candidate, type_message, descriptor_message))
    } else {
      stop(scalar_wrong_value(arg_name, candidate, type_message, descriptor_message))
    }
  }
}

verify_real <- function(arg_name, candidate, is_scalar, descriptor_message = "") {
  verify_type(arg_name, candidate, is_scalar, "numeric", "a real number", descriptor_message)
}

verify_any_int <- function(arg_name, candidate, is_scalar) {
  verify_type(arg_name, candidate, is_scalar, "numeric", "an integer")
  verify_int(arg_name, candidate, is_scalar, "an integer")
}

verify_nonnegative_int <- function(arg_name, candidate, is_scalar) {
  verify_type(arg_name, candidate, is_scalar, "numeric", "a non-negative integer")
  verify_int(arg_name, candidate, is_scalar, "a non-negative integer")
  verify_in_range(arg_name, candidate, is_scalar, "a non-negative integer", "", lb = 0, ub = NULL, lb_open = FALSE, ub_open = TRUE)
}

verify_positive_int <- function(arg_name, candidate, is_scalar) {
  verify_type(arg_name, candidate, is_scalar, "numeric", "a positive integer")
  verify_int(arg_name, candidate, is_scalar, "a positive integer")
  verify_in_range(arg_name, candidate, is_scalar, "a positive integer", "", lb = 0, ub = NULL, lb_open = TRUE, ub_open = TRUE)
}

verify_in_unit_interval <- function(arg_name, candidate, is_scalar) {
  verify_real(arg_name, candidate, is_scalar, " between 0 and 1 inclusive")
  verify_in_range(arg_name, candidate, is_scalar, "a real number", " between 0 and 1 inclusive", lb = 0, ub = 1, lb_open = FALSE, ub_open = FALSE)
}

verify_named_list <- function(arg_name, candidate) {
  if (first_class(candidate) != "list") {
    stop(paste0("'", arg_name, "' must be a named list. Got an object of class ", first_class(candidate), "."))
  } else if (length(names(candidate)) != length(candidate)) {
    stop(paste0("'", arg_name, "' must be a named list. Got a list with ", length(candidate), " elements and ", length(names(candidate)), " names."))
  }
}

verify_string <- function(arg_name, candidate, is_scalar) {
  verify_type(arg_name, candidate, is_scalar, "character", "a string")
}

verify_bool <- function(arg_name, candidate, is_scalar) {
  verify_type(arg_name, candidate, is_scalar, "logical", "a boolean")
}

strip_r6_wrapping <- function(r6_object) {
  return(r6_object$.internal_rcpp_object)
}
