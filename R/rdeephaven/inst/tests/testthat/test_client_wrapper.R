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

  # TODO: assumes server is actually running on localhost:10000, this is probably bad for CI
  expect_no_error(client <- connect(target = "localhost:10000"))
  
})

# All of the following tests assume the correctness of new(...) to make the connection.

test_that("import_table does not fail with data frame inputs of simple column types", {
  data <- setup()

  client <- connect(target = "localhost:10000")

  expect_no_error(import_table(client, data$df1))
  expect_no_error(import_table(client, data$df2))
  expect_no_error(import_table(client, data$df3))
  expect_no_error(import_table(client, data$df4))

  close(client)
})

test_that("import_table does not fail with tibble inputs of simple column types", {
  data <- setup()

  client <- connect(target = "localhost:10000")

  expect_no_error(import_table(client, as_tibble(data$df1)))
  expect_no_error(import_table(client, as_tibble(data$df2)))
  expect_no_error(import_table(client, as_tibble(data$df3)))
  expect_no_error(import_table(client, as_tibble(data$df4)))

  close(client)
})

test_that("import_table does not fail with arrow table inputs of simple column types", {
  data <- setup()

  client <- connect(target = "localhost:10000")

  expect_no_error(import_table(client, as_arrow_table(data$df1)))
  expect_no_error(import_table(client, as_arrow_table(data$df2)))
  expect_no_error(import_table(client, as_arrow_table(data$df3)))
  expect_no_error(import_table(client, as_arrow_table(data$df4)))

  close(client)
})

test_that("import_table does not fail with record batch reader inputs of simple column types", {
  data <- setup()

  client <- connect(target = "localhost:10000")

  expect_no_error(import_table(client, as_record_batch_reader(data$df1)))
  expect_no_error(import_table(client, as_record_batch_reader(data$df2)))
  expect_no_error(import_table(client, as_record_batch_reader(data$df3)))
  expect_no_error(import_table(client, as_record_batch_reader(data$df4)))

  close(client)
})

# The following tests additionally assume the correctness of import_table(...) AND table_handle$bind_to_variable(),
# as we have to create data, push it to the server, and name it in order to test open_table().
# Additionally, we assume the correctness of table_handle$to_data_frame() to make concrete comparisons.

test_that("open_table opens the correct table from the server", {
  data <- setup()

  client <- connect(target = "localhost:10000")

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

test_that("empty_table correctly creates tables on the server", {
  client <- connect(target = "localhost:10000")

  th1 <- empty_table(client, 10) %>% update("X = i")
  df1 <- data.frame(X = c(0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
  expect_equal(as.data.frame(th1), df1)

  close(client)
})

# TODO: Test time_table good inputs

test_that("run_script correctly runs a python script", {
  client <- connect(target = "localhost:10000")

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

# FAILS
# TODO: Test all of the new client connections stuff

test_that("import_table fails nicely with bad inputs", {
  library(datasets)

  client <- connect(target = "localhost:10000")

  expect_error(
    import_table(client, 12345),
    "'table_object' must be either an R Data Frame, a dplyr Tibble, an Arrow Table, or an Arrow Record Batch Reader. Got an object of class numeric instead."
  )
  expect_error(
    import_table(client, "hello!"),
    "'table_object' must be either an R Data Frame, a dplyr Tibble, an Arrow Table, or an Arrow Record Batch Reader. Got an object of class character instead."
  )

  # TODO: this needs better error handling, but it is unclear whether that happens on the server side or the R side.
  data(iris)
  expect_error(import_table(client, iris))

  data(HairEyeColor)
  expect_error(
    import_table(client, HairEyeColor),
    "'table_object' must be either an R Data Frame, a dplyr Tibble, an Arrow Table, or an Arrow Record Batch Reader. Got an object of class table instead."
  )

  close(client)
})

test_that("open_table fails nicely with bad inputs", {
  client <- connect(target = "localhost:10000")

  expect_error(open_table(client, ""), "The table '' you're trying to pull does not exist on the server.")
  expect_error(open_table(client, 12345), "'name' must be passed as a single string. Got an object of class numeric instead.")
  expect_error(open_table(client, c("I", "am", "string")), "'name' must be passed as a single string. Got a string vector of length 3 instead.")

  close(client)
})

test_that("empty_table fails nicely with bad inputs", {
  client <- connect(target = "localhost:10000")

  expect_error(empty_table(client, 0), "'size' must be a positive integer. Got 'size' = 0 instead.")
  expect_error(empty_table(client, -3), "'size' must be a positive integer. Got 'size' = -3 instead.")
  expect_error(empty_table(client, 1.2345), "'size' must be an integer. Got 'size' = 1.2345 instead.")
  expect_error(empty_table(client, "hello!"), "'size' must be passed as a single numeric. Got an object of class character instead.")
  expect_error(empty_table(client, c(1, 2, 3, 4)), "'size' must be passed as a single numeric. Got a numeric vector of length 4 instead.")

  close(client)
})

test_that("time_table fails nicely with bad inputs", {
  client <- connect(target = "localhost:10000")

  expect_error(time_table(client, 1.23, 1000), "'period_nanos' must be an integer. Got 'period_nanos' = 1.23 instead.")
  expect_error(time_table(client, 1000, 1.23), "'start_time_nanos' must be an integer. Got 'start_time_nanos' = 1.23 instead.")
  expect_error(time_table(client, c(1, 2, 3), 1000), "'period_nanos' must be passed as a single numeric. Got a numeric vector of length 3 instead.")
  expect_error(time_table(client, 1000, c(1, 2, 3)), "'start_time_nanos' must be passed as a single numeric. Got a numeric vector of length 3 instead.")
  expect_error(time_table(client, "hello!", 1000), "'period_nanos' must be passed as a single numeric. Got an object of class character instead.")
  expect_error(time_table(client, 1000, "hello!"), "'start_time_nanos' must be passed as a single numeric. Got an object of class character instead.")

  close(client)
})

test_that("run_script fails nicely with bad input types", {
  client <- connect(target = "localhost:10000")

  expect_error(run_script(client, 12345), "'script' must be passed as a single string. Got an object of class numeric instead.")
  expect_error(run_script(client, c("I", "am", "a", "string")), "'script' must be passed as a single string. Got a string vector of length 4 instead.")

  close(client)
})
