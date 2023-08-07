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

test_that("client connection works in the simple case of anonymous authentication", {
  # assumes correctness of client options
  client_options <- ClientOptions$new()

  # TODO: assumes server is actually running on localhost:10000, this is probably bad for CI
  expect_no_error(client <- Client$new(target = "localhost:10000", client_options = client_options))
})

# All of the following tests assume the correctness of Client$new(...) to make the connection.

test_that("import_table does not fail with data frame inputs of simple column types", {
  data <- setup()

  client_options <- ClientOptions$new()
  client <- Client$new(target = "localhost:10000", client_options = client_options)

  expect_no_error(client$import_table(data$df1))
  expect_no_error(client$import_table(data$df2))
  expect_no_error(client$import_table(data$df3))
  expect_no_error(client$import_table(data$df4))
})

test_that("import_table does not fail with tibble inputs of simple column types", {
  data <- setup()

  client_options <- ClientOptions$new()
  client <- Client$new(target = "localhost:10000", client_options = client_options)

  expect_no_error(client$import_table(as_tibble(data$df1)))
  expect_no_error(client$import_table(as_tibble(data$df2)))
  expect_no_error(client$import_table(as_tibble(data$df3)))
  expect_no_error(client$import_table(as_tibble(data$df4)))
})

test_that("import_table does not fail with arrow table inputs of simple column types", {
  data <- setup()

  client_options <- ClientOptions$new()
  client <- Client$new(target = "localhost:10000", client_options = client_options)

  expect_no_error(client$import_table(arrow_table(data$df1)))
  expect_no_error(client$import_table(arrow_table(data$df2)))
  expect_no_error(client$import_table(arrow_table(data$df3)))
  expect_no_error(client$import_table(arrow_table(data$df4)))
})

test_that("import_table does not fail with record batch reader inputs of simple column types", {
  data <- setup()

  client_options <- ClientOptions$new()
  client <- Client$new(target = "localhost:10000", client_options = client_options)

  expect_no_error(client$import_table(as_record_batch_reader(arrow_table(data$df1))))
  expect_no_error(client$import_table(as_record_batch_reader(arrow_table(data$df2))))
  expect_no_error(client$import_table(as_record_batch_reader(arrow_table(data$df3))))
  expect_no_error(client$import_table(as_record_batch_reader(arrow_table(data$df4))))
})

# The following tests additionally assume the correctness of client$import_table(...) AND table_handle$bind_to_variable(),
# as we have to create data, push it to the server, and name it in order to test client$open_table().
# Additionally, we assume the correctness of table_handle$to_data_frame() to make concrete comparisons.

test_that("open_table opens the correct table from the server", {
  data <- setup()

  client_options <- ClientOptions$new()
  client <- Client$new(target = "localhost:10000", client_options = client_options)

  th1 <- client$import_table(data$df1)
  th1$bind_to_variable("table1")
  expect_equal(client$open_table("table1")$to_data_frame(), th1$to_data_frame())

  th2 <- client$import_table(data$df2)
  th2$bind_to_variable("table2")
  expect_equal(client$open_table("table2")$to_data_frame(), th2$to_data_frame())

  th3 <- client$import_table(data$df3)
  th3$bind_to_variable("table3")
  expect_equal(client$open_table("table3")$to_data_frame(), th3$to_data_frame())

  th4 <- client$import_table(data$df4)
  th4$bind_to_variable("table4")
  expect_equal(client$open_table("table4")$to_data_frame(), th4$to_data_frame())
})

test_that("run_script correctly runs a python script", {
  client_options <- ClientOptions$new()
  client <- Client$new(target = "localhost:10000", client_options = client_options)

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
})

##### TESTING BAD INPUTS #####

test_that("client connection fails nicely with bad target but good client_options", {
  # assumes correctness of client options
  client_options <- ClientOptions$new()

  # TODO: Bad address needs better error handling from the R side
  expect_error(client <- Client$new(target = "bad address", client_options = client_options))
  expect_error(
    client <- Client$new(target = 12345, client_options = client_options),
    "'target' must be passed as a single string. Got an object of class numeric instead."
  )
  expect_error(
    client <- Client$new(target = c("hello", "my", "name", "is"), client_options = client_options),
    "'target' must be passed as a single string. Got a character vector of length 4 instead."
  )
})

test_that("client connection fails nicely with good target but bad client_options", {
  # TODO: these all assume that the server is actually running on localhost:10000, probably bad for CI
  expect_error(
    client <- Client$new(target = "localhost:10000", client_options = "bad"),
    "'client_options' should be a Deephaven ClientOptions object. Got an object of type character instead."
  )
  expect_error(
    client <- Client$new(target = "localhost:10000", client_options = 12345),
    "'client_options' should be a Deephaven ClientOptions object. Got an object of type numeric instead."
  )

  # TODO: Invalid auth details needs better error handling from the R side
  bad_client_options1 <- ClientOptions$new()
  bad_client_options1$set_basic_authentication("my_username", "my_password")
  expect_error(client <- Client$new(target = "localhost:10000", client_options = bad_client_options1))

  bad_client_options2 <- ClientOptions$new()
  bad_client_options2$set_session_type("groovy")
  expect_error(client <- Client$new(target = "localhost:10000", client_options = bad_client_options2))
})

test_that("import_table fails nicely with bad inputs", {
  library(datasets)

  client_options <- ClientOptions$new()
  client <- Client$new(target = "localhost:10000", client_options = client_options)

  expect_error(
    client$import_table(12345),
    "'table_object' must be either an R Data Frame, a dplyr Tibble, an Arrow Table, or an Arrow Record Batch Reader. Got an object of class numeric instead."
  )
  expect_error(
    client$import_table("hello!"),
    "'table_object' must be either an R Data Frame, a dplyr Tibble, an Arrow Table, or an Arrow Record Batch Reader. Got an object of class character instead."
  )

  # TODO: this needs better error handling, but it is unclear whether that happens on the server side or the R side.
  data(iris)
  expect_error(client$import_table(iris))

  data(HairEyeColor)
  expect_error(
    client$import_table(HairEyeColor),
    "'table_object' must be either an R Data Frame, a dplyr Tibble, an Arrow Table, or an Arrow Record Batch Reader. Got an object of class table instead."
  )
})

test_that("open_table fails nicely with bad inputs", {
  client_options <- ClientOptions$new()
  client <- Client$new(target = "localhost:10000", client_options = client_options)

  expect_error(client$open_table(""), "The table '' you're trying to pull does not exist on the server.")
  expect_error(client$open_table(12345), "'name' must be passed as a single string. Got an object of class numeric instead.")
  expect_error(client$open_table(client_options), "'name' must be passed as a single string. Got an object of class ClientOptions instead.")
  expect_error(client$open_table(c("I", "am", "string")), "'name' must be passed as a single string. Got a character vector of length 3 instead.")
})

test_that("run_script fails nicely with bad input types", {
  client_options <- ClientOptions$new()
  client <- Client$new(target = "localhost:10000", client_options = client_options)

  expect_error(client$run_script(12345), "'script' must be passed as a single string. Got an object of class numeric instead.")
  expect_error(client$run_script(c("I", "am", "a", "string")), "'script' must be passed as a single string. Got a character vector of length 4 instead.")
})
