library(testthat)
library(rdeephaven)

##### TESTING GOOD INPUTS #####

test_that("initializing ClientOptions does not throw an error", {
  expect_no_error(client_options <- ClientOptions$new())
})

test_that("setting default authentication does not throw an error", {
  client_options <- ClientOptions$new()
  expect_no_error(client_options$set_default_authentication())
})

test_that("setting basic authentication with string inputs does not throw an error", {
  client_options <- ClientOptions$new()
  expect_no_error(client_options$set_basic_authentication("my_username", "my_password"))
})

test_that("setting custom authentication with string inputs does not throw an error", {
  client_options <- ClientOptions$new()
  expect_no_error(client_options$set_custom_authentication("my_key", "my_value"))
})

test_that("setting session type to python does not throw an error", {
  client_options <- ClientOptions$new()
  expect_no_error(client_options$set_session_type("python"))
})

test_that("setting session type to groovy does not throw an error", {
  client_options <- ClientOptions$new()
  expect_no_error(client_options$set_session_type("groovy"))
})

##### TESTING BAD INPUTS #####

test_that("setting basic authentication with bad input types fails nicely", {
  client_options <- ClientOptions$new()
  expect_error(
    client_options$set_basic_authentication(12345, "my_password"),
    "'username' must be passed as a single string. Got an object of class numeric instead."
  )
  expect_error(
    client_options$set_basic_authentication("my_username", 12345),
    "'password' must be passed as a single string. Got an object of class numeric instead."
  )
  expect_error(
    client_options$set_basic_authentication(c("I", "am", "a", "string"), "my_password"),
    "'username' must be passed as a single string. Got a character vector of length 4 instead."
  )
  expect_error(
    client_options$set_basic_authentication("my_username", c("I", "am", "a", "string")),
    "'password' must be passed as a single string. Got a character vector of length 4 instead."
  )
})

test_that("setting custom authentication with bad input types fails nicely", {
  client_options <- ClientOptions$new()
  expect_error(
    client_options$set_custom_authentication(12345, "my_auth_value"),
    "'auth_key' must be passed as a single string. Got an object of class numeric instead."
  )
  expect_error(
    client_options$set_custom_authentication("my_auth_key", 12345),
    "'auth_value' must be passed as a single string. Got an object of class numeric instead."
  )
  expect_error(
    client_options$set_custom_authentication(c("I", "am", "a", "string"), "my_auth_value"),
    "'auth_key' must be passed as a single string. Got a character vector of length 4 instead."
  )
  expect_error(
    client_options$set_custom_authentication("my_auth_key", c("I", "am", "a", "string")),
    "'auth_value' must be passed as a single string. Got a character vector of length 4 instead."
  )
})

test_that("setting bad session type fails nicely", {
  client_options <- ClientOptions$new()
  expect_error(
    client_options$set_session_type(12345),
    "'session_type' must be passed as a single string. Got an object of class numeric instead."
  )
  expect_error(
    client_options$set_session_type(c("I", "am", "string")),
    "'session_type' must be passed as a single string. Got a character vector of length 3 instead."
  )
})

test_that("using tls with bad input type fails nicely", {
  client_options <- ClientOptions$new()
  expect_error(
    client_options$use_tls(12345),
    "'root_certs' must be passed as a single string. Got an object of class numeric instead."
  )
  expect_error(
    client_options$use_tls(c("double", "string")),
    "'root_certs' must be passed as a single string. Got a character vector of length 2 instead."
  )
})

test_that("add_int_option with bad types fails nicely", {
  client_options <- ClientOptions$new()
  expect_error(
    client_options$add_int_option(12345, 12345),
    "'opt' must be passed as a single string. Got an object of class numeric instead."
  )
  expect_error(
    client_options$add_int_option(c("several", "strings"), 12345),
    "'opt' must be passed as a single string. Got a character vector of length 2 instead."
  )
  expect_error(
    client_options$add_int_option("option_key", "blah blah"),
    "'val' must be an integer. Got an object of class character instead."
  )
  expect_error(
    client_options$add_int_option("option_key", 12345.6789),
    "'val' must be an integer. Got a non-integer numeric type instead."
  )
  expect_error(
    client_options$add_int_option("option_key", c(1, 2, 3, 4, 5)),
    "'val' must be an integer. Got a numeric vector of length 5 instead."
  )
})

test_that("add_string_option with bad types fails nicely", {
  client_options <- ClientOptions$new()
  expect_error(
    client_options$add_string_option(12345, "option_val"),
    "'opt' must be passed as a single string. Got an object of class numeric instead."
  )
  expect_error(
    client_options$add_string_option(c("several", "strings"), "option_val"),
    "'opt' must be passed as a single string. Got a character vector of length 2 instead."
  )
  expect_error(
    client_options$add_string_option("option_key", 12345),
    "'val' must be passed as a single string. Got an object of class numeric instead."
  )
  expect_error(
    client_options$add_string_option("option_key", c("several", "many", "strings")),
    "'val' must be passed as a single string. Got a character vector of length 3 instead."
  )
})

test_that("add_extra_header with bad types fails nicely", {
  client_options <- ClientOptions$new()
  expect_error(
    client_options$add_extra_header(12345, "header_value"),
    "'header_name' must be passed as a single string. Got an object of class numeric instead."
  )
  expect_error(
    client_options$add_extra_header(c("several", "strings"), "header_value"),
    "'header_name' must be passed as a single string. Got a character vector of length 2 instead."
  )
  expect_error(
    client_options$add_extra_header("header_name", 12345),
    "'header_value' must be passed as a single string. Got an object of class numeric instead."
  )
  expect_error(
    client_options$add_extra_header("header_name", c("several", "many", "strings")),
    "'header_value' must be passed as a single string. Got a character vector of length 3 instead."
  )
})
