first_class <- function(arg) { return(class(arg)[[1]]) }

verify_type <- function(arg_name, candidate, required_type, message_type_name, is_scalar) {
  if(first_class(candidate) == "list") {
    if(any(lapply(candidate, first_class) != required_type)) {
      stop(paste0("'", arg_name, "' must be a ", message_type_name, ", or a vector of ", message_type_name, "s. Got a vector with at least one element that is not a ", message_type_name, " instead."))
    }
  }
  else {
    if(first_class(candidate) != required_type) {
      if(is_scalar) {
        stop(paste0("'", arg_name, "' must be passed as a single ", message_type_name, ". Got an object of class ", first_class(candidate), " instead."))
      }
      else {
        stop(paste0("'", arg_name, "' must be passed as a ", message_type_name, " or a vector of ", message_type_name, "s. Got an object of class ", first_class(candidate), " instead."))
      }
    }
  }
  if(is_scalar) {
    if(length(candidate) != 1) {
      stop(paste0("'", arg_name, "' must be passed as a single ", message_type_name, ". Got a ", message_type_name, " vector of length ", length(candidate), " instead."))
    }
  }
}

# does not attempt to verify that candidate is numeric
verify_in_range <- function(arg_name, candidate, a = NULL, b = NULL, a_open = TRUE, b_open = TRUE) {

  printed_interval = paste0(ifelse(a_open, "(", "["), ifelse(is.null(a), "-inf", as.character(a)),
                            ", ", ifelse(is.null(b), "inf", as.character(b)), ifelse(b_open, ")", "]"))

  if(((!is.null(a)) && ((any(candidate <= a) && (a_open)) || (any(candidate < a) && (!a_open)))) ||
     ((!is.null(b)) && ((any(candidate >= b) && (b_open)) || (any(candidate > b) && (!b_open))))) {

    if(length(candidate) == 1) {
      stop(paste0("'", arg_name, "' must be in the interval ", printed_interval, ". Got '", arg_name, "' = ", candidate, " instead."))
    }
    else {
      stop(paste0("Every element of '", arg_name, "' must be in the interval ", printed_interval, ". Got at least one element outside of this interval instead."))
    }
  }
}

# does not attempt to verify that candidate is numeric
verify_int <- function(arg_name, candidate) {
  if(candidate != as.integer(candidate)) {
    if(length(candidate == 1)) {
      stop(paste0("'", arg_name, "' must be an integer. Got '", arg_name, "' = ", candidate, " instead."))
    }
    else {
      stop(paste0("Every element of '", arg_name, "' must be an integer. Got at least one non-integer element instead."))
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

verify_in_unit_interval <- function(arg_name, candidate, is_scalar) {
  verify_numeric(arg_name, candidate, is_scalar)
  verify_in_range(arg_name, candidate, a = 0, b = 1, a_open = FALSE, b_open = FALSE)
}

verify_any_int <- function(arg_name, candidate, is_scalar) {
  verify_numeric(arg_name, candidate, is_scalar)
  verify_int(arg_name, candidate)
}

verify_nonnegative_int <- function(arg_name, candidate, is_scalar) {
  verify_numeric(arg_name, candidate, is_scalar)
  verify_int(arg_name, candidate)
  verify_in_range(arg_name, candidate, a = 0, a_open = FALSE)
}

verify_positive_int <- function(arg_name, candidate, is_scalar) {
  verify_numeric(arg_name, candidate, is_scalar)
  verify_int(arg_name, candidate)
  verify_in_range(arg_name, candidate, a = 0)
}

strip_r6_wrapping_from_aggregation <- function(r6_aggregation) {
  return(r6_aggregation$internal_aggregation)
}

strip_r6_wrapping_from_table_handle <- function(r6_table_handle) {
  return(r6_table_handle$internal_table_handle)
}
