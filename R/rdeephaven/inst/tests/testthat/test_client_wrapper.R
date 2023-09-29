library(testthat)
library(rdeephaven)

target <- "localhost:10000"

setup <- function() {
  df1 <- data.frame(
    string_col = c("I", "am", "a", "string", "column"),
    int_col = c(0, 1, 2, 3, 4),
    dbl_col = c(1.65, 3.1234, 100000.5, 543.234567, 0.00)
  )

  df2 <- data.frame(
    col1 = rep(3.14, 100),
    col2 = rep("hello!", 100),
    col3 = rnorm(100)
  )

  df3 <- data.frame(matrix(rnorm(10 * 1000), nrow = 10))

  df4 <- data.frame(
    time_col = seq.POSIXt(as.POSIXct(Sys.Date()), as.POSIXct(Sys.Date() + 30), by = "1 sec")[250000],
    bool_col = sample(c(TRUE, FALSE), 250000, TRUE),
    int_col = sample(0:10000, 250000, TRUE)
  )

  return(list("df1" = df1, "df2" = df2, "df3" = df3, "df4" = df4))
}

##### TESTING GOOD INPUTS #####

test_that("client dhConnection works in the simple case of anonymous authentication", {

  # TODO: assumes server is actually running on localhost:10000, this is probably bad for CI
  expect_no_error(client <- Client$new(target = target))
  
})

test_that("import_table does not fail with data frame inputs of simple column types", {
  data <- setup()

  client <- Client$new(target = target)

  expect_no_error(client$import_table(data$df1))
  expect_no_error(client$import_table(data$df2))
  expect_no_error(client$import_table(data$df3))
  expect_no_error(client$import_table(data$df4))

  client$close()
})

test_that("import_table does not fail with tibble inputs of simple column types", {
  data <- setup()

  client <- Client$new(target = target)

  expect_no_error(client$import_table(as_tibble(data$df1)))
  expect_no_error(client$import_table(as_tibble(data$df2)))
  expect_no_error(client$import_table(as_tibble(data$df3)))
  expect_no_error(client$import_table(as_tibble(data$df4)))

  client$close()
})

test_that("import_table does not fail with arrow table inputs of simple column types", {
  data <- setup()

  client <- Client$new(target = target)

  expect_no_error(client$import_table(as_arrow_table(data$df1)))
  expect_no_error(client$import_table(as_arrow_table(data$df2)))
  expect_no_error(client$import_table(as_arrow_table(data$df3)))
  expect_no_error(client$import_table(as_arrow_table(data$df4)))

  client$close()
})

test_that("import_table does not fail with record batch reader inputs of simple column types", {
  data <- setup()

  client <- Client$new(target = target)

  expect_no_error(client$import_table(as_record_batch_reader(data$df1)))
  expect_no_error(client$import_table(as_record_batch_reader(data$df2)))
  expect_no_error(client$import_table(as_record_batch_reader(data$df3)))
  expect_no_error(client$import_table(as_record_batch_reader(data$df4)))

  client$close()
})

# The following tests assume the correctness of import_table(...) AND bind_to_variable(),
# as we have to create data, push it to the server, and name it in order to test open_table().
# Additionally, we assume the correctness of as.data.frame() to make concrete comparisons.

test_that("open_table opens the correct table from the server", {
  data <- setup()

  client <- Client$new(target = target)

  th1 <- client$import_table(data$df1)
  th1$bind_to_variable("table1")
  expect_equal(as.data.frame(client$open_table("table1")), as.data.frame(th1))

  th2 <- client$import_table(data$df2)
  th2$bind_to_variable("table2")
  expect_equal(as.data.frame(client$open_table("table2")), as.data.frame(th2))

  th3 <- client$import_table(data$df3)
  th3$bind_to_variable("table3")
  expect_equal(as.data.frame(client$open_table("table3")), as.data.frame(th3))

  th4 <- client$import_table(data$df4)
  th4$bind_to_variable("table4")
  expect_equal(as.data.frame(client$open_table("table4")), as.data.frame(th4))

  client$close()
})

test_that("empty_table correctly creates tables on the server", {
  client <- Client$new(target = target)

  th1 <- client$empty_table(10)$update("X = i")
  df1 <- data.frame(X = c(0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
  expect_equal(as.data.frame(th1), df1)

  client$close()
})

# TODO: Test time_table good inputs

test_that("run_script correctly runs a python script", {
  client <- Client$new(target = target)

  expect_no_error(client$run_script(
    '
from deephaven import new_table
from deephaven.column import string_col, int_col

static_table_from_python_script = new_table([
string_col("Name_String_Col", ["Data String 1", "Data String 2", "Data String 3"]),
int_col("Name_Int_Col", [44, 55, 66])
])
'
  ))

  expect_no_error(client$open_table("static_table_from_python_script"))

  client$close()
})

##### TESTING BAD INPUTS #####

test_that("client constructor fails nicely with bad inputs", {
  
  expect_error(
    Client$new(target = target, auth_type = "basic"),
    "Basic authentication was requested, but 'auth_token' was not provided, and at most one of 'username' or 'password' was provided. Please provide either 'username' and 'password', or 'auth_token'."
  )
  expect_error(
    Client$new(target = target, auth_type = "basic", username = "user"),
    "Basic authentication was requested, but 'auth_token' was not provided, and at most one of 'username' or 'password' was provided. Please provide either 'username' and 'password', or 'auth_token'."
  )
  expect_error(
    Client$new(target = target, auth_type = "basic", password = "pass"),
    "Basic authentication was requested, but 'auth_token' was not provided, and at most one of 'username' or 'password' was provided. Please provide either 'username' and 'password', or 'auth_token'."
  )
  expect_error(
    Client$new(target = target, auth_type = "basic", username = "user", auth_token = "token"),
    "Basic authentication was requested, but 'auth_token' was provided, as well as least one of 'username' and 'password'. Please provide either 'username' and 'password', or 'auth_token'."
  )
  expect_error(
    Client$new(target = target, auth_type = "basic", password = "pass", auth_token = "token"),
    "Basic authentication was requested, but 'auth_token' was provided, as well as least one of 'username' and 'password'. Please provide either 'username' and 'password', or 'auth_token'."
  )
  expect_error(
    Client$new(target = target, auth_type = "basic", username = "user", password = "pass", auth_token = "token"),
    "Basic authentication was requested, but 'auth_token' was provided, as well as least one of 'username' and 'password'. Please provide either 'username' and 'password', or 'auth_token'."
  )
  expect_error(
    Client$new(target = target, auth_type = "psk"),
    "Pre-shared key authentication was requested, but no 'auth_token' was provided."
  )
  expect_error(
    Client$new(target = target, auth_type = "custom"),
    "Custom authentication was requested, but no 'auth_token' was provided."
  )
  expect_error(
    Client$new(target = target, auth_type = ""),
    "'auth_type' should be a non-empty string."
  )
  expect_error(
    Client$new(target = target, auth_type = "basic", auth_token = 1234),
    "'auth_token' must be a single string. Got an object of class numeric."
  )
  expect_error(
    Client$new(target = target, session_type = "blahblah"),
    "'session_type' must be 'python' or 'groovy', but got blahblah."
  )
  expect_error(
    Client$new(target = target, session_type = 1234),
    "'session_type' must be 'python' or 'groovy', but got 1234."
  )
  expect_error(
    Client$new(target = target, use_tls = "banana"),
    "'use_tls' must be a single boolean. Got an object of class character."
  )
  expect_error(
    Client$new(target = target, int_options = 1234),
    "'int_options' must be a named list. Got an object of class numeric."
  )
  expect_error(
    Client$new(target = target, int_options = list(1, 2, 3)),
    "'int_options' must be a named list. Got a list with 3 elements and 0 names."
  )
  expect_error(
    Client$new(target = target, int_options = list(a = 12.34)),
    "'value' must be a single integer. Got 'value' = 12.34."
  )
  expect_error(
    Client$new(target = target, string_options = 1234),
    "'string_options' must be a named list. Got an object of class numeric."
  )
  expect_error(
    Client$new(target = target, string_options = list("a", "b", "c")),
    "'string_options' must be a named list. Got a list with 3 elements and 0 names."
  )
  expect_error(
    Client$new(target = target, string_options = list(a = 123)),
    "'value' must be a single string. Got an object of class numeric."
  )
  expect_error(
    Client$new(target = target, extra_headers = 1234),
    "'extra_headers' must be a named list. Got an object of class numeric."
  )
  expect_error(
    Client$new(target = target, extra_headers = list("a", "b", "c")),
    "'extra_headers' must be a named list. Got a list with 3 elements and 0 names."
  )
  expect_error(
    Client$new(target = target, extra_headers = list(a = 123)),
    "'value' must be a single string. Got an object of class numeric."
  )
  
})

test_that("import_table fails nicely with bad inputs", {
  library(datasets)

  client <- Client$new(target = target)

  expect_error(client$import_table(12345), "'table_object' must be a single data frame, tibble, arrow table, or record batch reader. Got an object of class numeric.")
  expect_error(client$import_table(c("I", "am", "a", "string")), "'table_object' must be a single data frame, tibble, arrow table, or record batch reader. Got an object of class character.")

  data(HairEyeColor)
  expect_error(client$import_table(HairEyeColor), "'table_object' must be a single data frame, tibble, arrow table, or record batch reader. Got an object of class table.")

  client$close()
})

test_that("open_table fails nicely with bad inputs", {
  client <- Client$new(target = target)

  expect_error(client$open_table(""), "The table '' does not exist on the server.")
  expect_error(client$open_table(12345), "'name' must be a single string. Got an object of class numeric.")
  expect_error(client$open_table(c("I", "am", "string")), "'name' must be a single string. Got a vector of length 3.")

  client$close()
})

test_that("empty_table fails nicely with bad inputs", {
  client <- Client$new(target = target)

  expect_error(client$empty_table(-3), "'size' must be a single non-negative integer. Got 'size' = -3.")
  expect_error(client$empty_table(1.2345), "'size' must be a single non-negative integer. Got 'size' = 1.2345.")
  expect_error(client$empty_table("hello!"), "'size' must be a single non-negative integer. Got an object of class character.")
  expect_error(client$empty_table(c(1, 2, 3, 4)), "'size' must be a single non-negative integer. Got a vector of length 4.")

  client$close()
})

test_that("time_table fails nicely with bad inputs", {
  client <- Client$new(target = target)

  expect_error(client$time_table(1000), "'period' must be a single string. Got an object of class numeric.")
  expect_error(client$time_table("PT1s", 10000), "'start_time' must be a single string. Got an object of class numeric.")
  expect_error(client$time_table(c("PT1s", "PT2s")), "'period' must be a single string. Got a vector of length 2.")
  expect_error(client$time_table("PT1s", c("PT1s", "PT2s")), "'start_time' must be a single string. Got a vector of length 2.")

  client$close()
})

test_that("run_script fails nicely with bad input types", {
  client <- Client$new(target = target)

  expect_error(client$run_script(12345), "'script' must be a single string. Got an object of class numeric.")
  expect_error(client$run_script(c("I", "am", "a", "string")), "'script' must be a single string. Got a vector of length 4.")

  client$close()
})

test_that("Running Client$new with wrong argument types gives good errors", {
  expect_error(Client$new(12345),
    "Client initialize first argument must be either a string or an Rcpp::XPtr object.")
})

test_that("A Client created from an Rcpp::XPtr is functional.", {
  client <- Client$new(target = target)
  client$empty_table(1)$update("A = 42")$bind_to_variable("t")
  client_xptr <- client$.internal_rcpp_object$internal_client()
  client2 <- Client$new(client_xptr)
  t <- client2$open_table("t")
  df <- t$as_data_frame()
  expect_true(df[1,1] == 42)
  client$close()
})

test_that("ticket_to_table works.", {
  client <- Client$new(target = target)
  client$empty_table(1)$update("A = 43")$bind_to_variable("t")
  t <- client$ticket_to_table("s/t")
  df <- t$as_data_frame()
  expect_true(df[1,1] == 43)
  client$close()
})

rm(target)
