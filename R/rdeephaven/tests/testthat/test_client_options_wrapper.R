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
    expect_error(client_options$set_basic_authentication(12345, "my_password"),
        "'username' must be passed as a single string. Got object of class numeric instead.")
    expect_error(client_options$set_basic_authentication("my_username", 12345),
        "'password' must be passed as a single string. Got object of class numeric instead.")
    expect_error(client_options$set_basic_authentication(c("I", "am", "a", "string"), "my_password"),
        "'username' must be passed as a single string. Got character vector of length 4 instead.")
    expect_error(client_options$set_basic_authentication("my_username", c("I", "am", "a", "string")),
        "'password' must be passed as a single string. Got character vector of length 4 instead.")
})

test_that("setting custom authentication with bad input types fails nicely", {

    client_options <- ClientOptions$new()
    expect_error(client_options$set_custom_authentication(12345, "my_auth_value"),
        "'auth_key' must be passed as a single string. Got object of class numeric instead.")
    expect_error(client_options$set_custom_authentication("my_auth_key", 12345),
        "'auth_value' must be passed as a single string. Got object of class numeric instead.")
    expect_error(client_options$set_custom_authentication(c("I", "am", "a", "string"), "my_auth_value"),
        "'auth_key' must be passed as a single string. Got character vector of length 4 instead.")
    expect_error(client_options$set_custom_authentication("my_auth_key", c("I", "am", "a", "string")),
        "'auth_value' must be passed as a single string. Got character vector of length 4 instead.")
})

test_that("setting bad session type fails nicely", {

    client_options <- ClientOptions$new()
    expect_error(client_options$set_session_type(12345),
        "'session_type' must be passed as a single string. Got object of class numeric instead.")
    expect_error(client_options$set_session_type(c("I", "am", "string")),
        "'session_type' must be passed as a single string. Got character vector of length 3 instead.")
})