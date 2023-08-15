library(testthat)
library(rdeephaven)

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
  expect_no_error(client <- dhConnect(target = "localhost:10000"))
  
})

test_that("import_table does not fail with data frame inputs of simple column types", {
  data <- setup()

  client <- dhConnect(target = "localhost:10000")

  expect_no_error(import_table(client, data$df1))
  expect_no_error(import_table(client, data$df2))
  expect_no_error(import_table(client, data$df3))
  expect_no_error(import_table(client, data$df4))

  close(client)
})

test_that("import_table does not fail with tibble inputs of simple column types", {
  data <- setup()

  client <- dhConnect(target = "localhost:10000")

  expect_no_error(import_table(client, as_tibble(data$df1)))
  expect_no_error(import_table(client, as_tibble(data$df2)))
  expect_no_error(import_table(client, as_tibble(data$df3)))
  expect_no_error(import_table(client, as_tibble(data$df4)))

  close(client)
})

test_that("import_table does not fail with arrow table inputs of simple column types", {
  data <- setup()

  client <- dhConnect(target = "localhost:10000")

  expect_no_error(import_table(client, as_arrow_table(data$df1)))
  expect_no_error(import_table(client, as_arrow_table(data$df2)))
  expect_no_error(import_table(client, as_arrow_table(data$df3)))
  expect_no_error(import_table(client, as_arrow_table(data$df4)))

  close(client)
})

test_that("import_table does not fail with record batch reader inputs of simple column types", {
  data <- setup()

  client <- dhConnect(target = "localhost:10000")

  expect_no_error(import_table(client, as_record_batch_reader(data$df1)))
  expect_no_error(import_table(client, as_record_batch_reader(data$df2)))
  expect_no_error(import_table(client, as_record_batch_reader(data$df3)))
  expect_no_error(import_table(client, as_record_batch_reader(data$df4)))

  close(client)
})

# The following tests assume the correctness of import_table(...) AND bind_to_variable(),
# as we have to create data, push it to the server, and name it in order to test open_table().
# Additionally, we assume the correctness of as.data.frame() to make concrete comparisons.

test_that("open_table opens the correct table from the server using %>%", {
  data <- setup()

  client <- dhConnect(target = "localhost:10000")

  th1 <- import_table(client, data$df1)
  th1 %>% bind_to_variable("table1")
  expect_equal(as.data.frame(open_table(client, "table1")), as.data.frame(th1))

  th2 <- import_table(client, data$df2)
  th2 %>% bind_to_variable("table2")
  expect_equal(as.data.frame(open_table(client, "table2")), as.data.frame(th2))

  th3 <- import_table(client, data$df3)
  th3 %>% bind_to_variable("table3")
  expect_equal(as.data.frame(open_table(client, "table3")), as.data.frame(th3))

  th4 <- import_table(client, data$df4)
  th4 %>% bind_to_variable("table4")
  expect_equal(as.data.frame(open_table(client, "table4")), as.data.frame(th4))

  close(client)
})

test_that("open_table opens the correct table from the server using |>", {
  data <- setup()

  client <- dhConnect(target = "localhost:10000")

  th1 <- import_table(client, data$df1)
  th1 |> bind_to_variable("table1")
  expect_equal(as.data.frame(open_table(client, "table1")), as.data.frame(th1))

  th2 <- import_table(client, data$df2)
  th2 |> bind_to_variable("table2")
  expect_equal(as.data.frame(open_table(client, "table2")), as.data.frame(th2))

  th3 <- import_table(client, data$df3)
  th3 |> bind_to_variable("table3")
  expect_equal(as.data.frame(open_table(client, "table3")), as.data.frame(th3))

  th4 <- import_table(client, data$df4)
  th4 |> bind_to_variable("table4")
  expect_equal(as.data.frame(open_table(client, "table4")), as.data.frame(th4))

  close(client)
})

test_that("empty_table correctly creates tables on the server using %>%", {
  client <- dhConnect(target = "localhost:10000")

  th1 <- empty_table(client, 10) %>% update("X = i")
  df1 <- data.frame(X = c(0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
  expect_equal(as.data.frame(th1), df1)

  close(client)
})

test_that("empty_table correctly creates tables on the server using |>", {
  client <- dhConnect(target = "localhost:10000")

  th1 <- empty_table(client, 10) |> update("X = i")
  df1 <- data.frame(X = c(0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
  expect_equal(as.data.frame(th1), df1)

  close(client)
})

# TODO: Test time_table good inputs

test_that("run_script correctly runs a python script", {
  client <- dhConnect(target = "localhost:10000")

  expect_no_error(run_script(client, 
    '
from deephaven import new_table
from deephaven.column import string_col, int_col

static_table_from_python_script = new_table([
string_col("Name_String_Col", ["Data String 1", "Data String 2", "Data String 3"]),
int_col("Name_Int_Col", [44, 55, 66])
])
'
  ))

  expect_no_error(open_table(client, "static_table_from_python_script"))

  close(client)
})

##### TESTING BAD INPUTS #####

test_that("dhConnect fails nicely with bad inputs", {
  
  expect_error(
    dhConnect(target = "localhost:10000", auth_type = "basic"),
    "Basic authentication was requested, but 'auth_token' was not provided, and at most one of 'username' or 'password' was provided. Please provide either 'username' and 'password', or 'auth_token'."
  )
  expect_error(
    dhConnect(target = "localhost:10000", auth_type = "basic", username = "user"),
    "Basic authentication was requested, but 'auth_token' was not provided, and at most one of 'username' or 'password' was provided. Please provide either 'username' and 'password', or 'auth_token'."
  )
  expect_error(
    dhConnect(target = "localhost:10000", auth_type = "basic", password = "pass"),
    "Basic authentication was requested, but 'auth_token' was not provided, and at most one of 'username' or 'password' was provided. Please provide either 'username' and 'password', or 'auth_token'."
  )
  expect_error(
    dhConnect(target = "localhost:10000", auth_type = "basic", username = "user", auth_token = "token"),
    "Basic authentication was requested, but 'auth_token' was provided, as well as least one of 'username' and 'password'. Please provide either 'username' and 'password', or 'auth_token'."
  )
  expect_error(
    dhConnect(target = "localhost:10000", auth_type = "basic", password = "pass", auth_token = "token"),
    "Basic authentication was requested, but 'auth_token' was provided, as well as least one of 'username' and 'password'. Please provide either 'username' and 'password', or 'auth_token'."
  )
  expect_error(
    dhConnect(target = "localhost:10000", auth_type = "basic", username = "user", password = "pass", auth_token = "token"),
    "Basic authentication was requested, but 'auth_token' was provided, as well as least one of 'username' and 'password'. Please provide either 'username' and 'password', or 'auth_token'."
  )
  expect_error(
    dhConnect(target = "localhost:10000", auth_type = "custom"),
    "Custom authentication was requested, but no 'auth_token' was provided."
  )
  expect_error(
    dhConnect(target = "localhost:10000", auth_type = ""),
    "'auth_type' should be a non-empty string."
  )
  expect_error(
    dhConnect(target = "localhost:10000", auth_type = "basic", auth_token = 1234),
    "'auth_token' must be a single string. Got an object of class numeric."
  )
  expect_error(
    dhConnect(target = "localhost:10000", session_type = "blahblah"),
    "'session_type' must be 'python' or 'groovy', but got blahblah."
  )
  expect_error(
    dhConnect(target = "localhost:10000", session_type = 1234),
    "'session_type' must be 'python' or 'groovy', but got 1234."
  )
  expect_error(
    dhConnect(target = "localhost:10000", use_tls = "banana"),
    "'use_tls' must be a single boolean. Got an object of class character."
  )
  expect_error(
    dhConnect(target = "localhost:10000", int_options = 1234),
    "'int_options' must be a single list. Got an object of class numeric."
  )
  expect_error(
    dhConnect(target = "localhost:10000", string_options = 1234),
    "'string_options' must be a single list. Got an object of class numeric."
  )
  expect_error(
    dhConnect(target = "localhost:10000", extra_headers = 1234),
    "'extra_headers' must be a single list. Got an object of class numeric."
  )
  
})

test_that("import_table fails nicely with bad inputs", {
  library(datasets)

  client <- dhConnect(target = "localhost:10000")

  expect_error(import_table(client, 12345), cat("unable to find an inherited method for function ‘import_table’ for signature ‘\"Client\", \"numeric\"’"))
  expect_error(import_table(client, "hello!"), cat("unable to find an inherited method for function ‘import_table’ for signature ‘\"Client\", \"character\"’"))

  # TODO: this needs better error handling, but it is unclear whether that happens on the server side or the R side.
  data(iris)
  expect_error(import_table(client, iris))

  data(HairEyeColor)
  expect_error(import_table(client, HairEyeColor), cat("unable to find an inherited method for function ‘import_table’ for signature ‘\"Client\", \"table\"’"))

  close(client)
})

test_that("open_table fails nicely with bad inputs", {
  client <- dhConnect(target = "localhost:10000")

  expect_error(open_table(client, ""), "The table '' does not exist on the server.")
  expect_error(open_table(client, 12345), cat("unable to find an inherited method for function ‘open_table’ for signature ‘\"Client\", \"numeric\"’"))
  expect_error(open_table(client, c("I", "am", "string")), "'name' must be a single string. Got a vector of length 3.")

  close(client)
})

test_that("empty_table fails nicely with bad inputs", {
  client <- dhConnect(target = "localhost:10000")

  expect_error(empty_table(client, -3), "'size' must be a nonnegative integer. Got 'size' = -3.")
  expect_error(empty_table(client, 1.2345), "'size' must be an integer. Got 'size' = 1.2345.")
  expect_error(empty_table(client, "hello!"), cat("unable to find an inherited method for function ‘empty_table’ for signature ‘\"Client\", \"character\"’"))
  expect_error(empty_table(client, c(1, 2, 3, 4)), "'size' must be a single numeric. Got a vector of length 4.")

  close(client)
})

test_that("time_table fails nicely with bad inputs", {
  client <- dhConnect(target = "localhost:10000")

  expect_error(time_table(client, 1.23, 1000), "'period' must be an integer. Got 'period' = 1.23.")
  expect_error(time_table(client, 1000, 1.23), "'start_time' must be an integer. Got 'start_time' = 1.23.")
  expect_error(time_table(client, c(1, 2, 3), 1000), "'period' must be a single numeric. Got a vector of length 3.")
  expect_error(time_table(client, 1000, c(1, 2, 3)), "'start_time' must be a single numeric. Got a vector of length 3.")
  expect_error(time_table(client, "hello!", 1000), cat("unable to find an inherited method for function ‘time_table’ for signature ‘\"Client\", \"character\", \"numeric\"’"))
  expect_error(time_table(client, 1000, "hello!"), cat("unable to find an inherited method for function ‘time_table’ for signature ‘\"Client\", \"numeric\", \"character\"’"))

  close(client)
})

test_that("run_script fails nicely with bad input types", {
  client <- dhConnect(target = "localhost:10000")

  expect_error(run_script(client, 12345), cat("unable to find an inherited method for function ‘run_script’ for signature ‘\"Client\", \"numeric\"’"))
  expect_error(run_script(client, c("I", "am", "a", "string")), "'script' must be a single string. Got a vector of length 4.")

  close(client)
})
