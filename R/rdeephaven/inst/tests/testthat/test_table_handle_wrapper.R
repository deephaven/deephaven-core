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

  # set up client
  client <- dhConnect(target = "localhost:10000")

  # move dataframes to server and get TableHandles for testing
  th1 <- import_table(client, df1)
  th2 <- import_table(client, df2)
  th3 <- import_table(client, df3)
  th4 <- import_table(client, df4)

  # time table to test is_static()
  th5 <- time_table(client, 1000000000) %>% update("X = ii")

  return(list(
    "client" = client,
    "df1" = df1, "df2" = df2, "df3" = df3, "df4" = df4,
    "th1" = th1, "th2" = th2, "th3" = th3, "th4" = th4, "th5" = th5
  ))
}

##### TESTING GOOD INPUTS #####

test_that("is_static returns the correct value", {
  data <- setup()

  expect_true(is_static(data$th1))
  expect_true(is_static(data$th2))
  expect_true(is_static(data$th3))
  expect_true(is_static(data$th4))
  expect_false(is_static(data$th5))

  close(data$client)
})

test_that("nrow returns the correct number of rows", {
  data <- setup()

  expect_equal(nrow(data$th1), nrow(data$df1))
  expect_equal(nrow(data$th2), nrow(data$df2))
  expect_equal(nrow(data$th3), nrow(data$df3))
  expect_equal(nrow(data$th4), nrow(data$df4))

  close(data$client)
})

test_that("ncol returns the correct number of columns", {
  data <- setup()
  
  expect_equal(ncol(data$th1), ncol(data$df1))
  expect_equal(ncol(data$th2), ncol(data$df2))
  expect_equal(ncol(data$th3), ncol(data$df3))
  expect_equal(ncol(data$th4), ncol(data$df4))
  
  close(data$client)
})

test_that("dim returns the correct dimension", {
  data <- setup()
  
  expect_equal(dim(data$th1), dim(data$df1))
  expect_equal(dim(data$th2), dim(data$df2))
  expect_equal(dim(data$th3), dim(data$df3))
  expect_equal(dim(data$th4), dim(data$df4))
  
  close(data$client)
})

test_that("bind_to_variable binds the table to a variable", {
  data <- setup()

  data$th1 %>% bind_to_variable("table1")
  expect_no_error(open_table(data$client, "table1"))

  data$th2 %>% bind_to_variable("table2")
  expect_no_error(open_table(data$client, "table2"))

  data$th3 %>% bind_to_variable("table3")
  expect_no_error(open_table(data$client, "table3"))

  data$th4 %>% bind_to_variable("table4")
  expect_no_error(open_table(data$client, "table4"))

  close(data$client)
})

test_that("as_record_batch_reader returns an identical stream reader", {
  data <- setup()

  # actual equality of RecordBatchStreamReaders is not expected, as they contain underlying pointers to relevant data,
  # as well as metadata from the Deephaven server, which we do not expect rbr created fully in R to have.
  # We care about equality in the sense that coercing to dataframes should yield identical dataframes, so we cast to dataframes and compare.
  # Additionally, as.data.frame() does not convert arrow tables to data frames, but to Tibbles. Need another as.data.frame to get a data frame.

  rbr1 <- as_record_batch_reader(data$th1)
  expect_equal(as.data.frame(as.data.frame(rbr1$read_table())), data$df1)

  rbr2 <- as_record_batch_reader(data$th2)
  expect_equal(as.data.frame(as.data.frame(rbr2$read_table())), data$df2)

  rbr3 <- as_record_batch_reader(data$th3)
  expect_equal(as.data.frame(as.data.frame(rbr3$read_table())), data$df3)

  rbr4 <- as_record_batch_reader(data$th4)
  expect_equal(as.data.frame(as.data.frame(rbr4$read_table())), data$df4)

  close(data$client)
})

test_that("as_arrow_table returns the correct Arrow table", {
  data <- setup()

  # The rationale for casting RecordBatchStreamReaders to dataframes for comparison also applies to Arrow Tables.
  # Additionally, as.data.frame() does not convert arrow tables to data frames, but to Tibbles. Need another as.data.frame to get a data frame.

  arrow_tbl1 <- as_arrow_table(data$th1)
  expect_equal(as.data.frame(as.data.frame(arrow_tbl1)), data$df1)

  arrow_tbl2 <- as_arrow_table(data$th2)
  expect_equal(as.data.frame(as.data.frame(arrow_tbl2)), data$df2)

  arrow_tbl3 <- as_arrow_table(data$th3)
  expect_equal(as.data.frame(as.data.frame(arrow_tbl3)), data$df3)

  arrow_tbl4 <- as_arrow_table(data$th4)
  expect_equal(as.data.frame(as.data.frame(arrow_tbl4)), data$df4)

  close(data$client)
})

test_that("as_tibble returns the correct Tibble", {
  data <- setup()

  tibble1 <- as_tibble(data$th1)
  expect_equal(tibble1, as_tibble(data$df1))

  tibble2 <- as_tibble(data$th2)
  expect_equal(tibble2, as_tibble(data$df2))

  tibble3 <- as_tibble(data$th3)
  expect_equal(tibble3, as_tibble(data$df3))

  tibble4 <- as_tibble(data$th4)
  expect_equal(tibble4, as_tibble(data$df4))

  close(data$client)
})

test_that("as.data.frame returns the correct data frame", {
  data <- setup()

  data_frame1 <- as.data.frame(data$th1)
  expect_equal(data_frame1, data$df1)

  data_frame2 <- as.data.frame(data$th2)
  expect_equal(data_frame2, data$df2)

  data_frame3 <- as.data.frame(data$th3)
  expect_equal(data_frame3, data$df3)

  data_frame4 <- as.data.frame(data$th4)
  expect_equal(data_frame4, data$df4)

  close(data$client)
})

test_that("as_data_frame returns the correct data frame", {
  data <- setup()
  
  data_frame1 <- as_data_frame(data$th1)
  expect_equal(data_frame1, data$df1)
  
  data_frame2 <- as_data_frame(data$th2)
  expect_equal(data_frame2, data$df2)
  
  data_frame3 <- as_data_frame(data$th3)
  expect_equal(data_frame3, data$df3)
  
  data_frame4 <- as_data_frame(data$th4)
  expect_equal(data_frame4, data$df4)
  
  close(data$client)
})

##### TESTING BAD INPUTS #####

test_that("bind_to_variable fails nicely on bad inputs", {
  data <- setup()

  expect_error(
    data$th1 %>% bind_to_variable(12345),
    cat("unable to find an inherited method for function ‘bind_to_variable’ for signature ‘\"TableHandle\", \"numeric\"’")
  )

  expect_error(
    data$th1 %>% bind_to_variable(c("multiple", "strings")),
    "'name' must be a single string. Got a vector of length 2."
  )

  expect_error(
    data$th1 %>% bind_to_variable(data$df1),
    cat("unable to find an inherited method for function 'bind_to_variable' for signature '\"TableHandle\", \"data.frame\"'")
  )

  expect_error(
    data$th1 %>% bind_to_variable(list("list", "of", "strings")),
    cat("unable to find an inherited method for function ‘bind_to_variable’ for signature ‘\"TableHandle\", \"list\"’")
  )

  close(data$client)
})
