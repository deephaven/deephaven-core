library(testthat)
library(dplyr)
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

  df5 <- data.frame(
    X = c("A", "B", "A", "C", "B", "A", "B", "B", "C"),
    Y = c("M", "N", "O", "N", "P", "M", "O", "P", "M"),
    Number1 = c(100, -44, 49, 11, -66, 50, 29, 18, -70),
    Number2 = c(-55, 76, 20, 130, 230, -50, 73, 137, 214)
  )

  df6 <- data.frame(
    X = c("B", "C", "B", "A", "A", "C", "B", "C", "B", "A"),
    Y = c("N", "N", "M", "P", "O", "P", "O", "N", "O", "O"),
    Number1 = c(55, 72, 86, -45, 1, 0, 345, -65, 99, -5),
    Number2 = c(76, 4, -6, 34, 12, -76, 45, -5, 34, 6)
  )

  # set up client
  client <- Client$new(target = get_dh_target())

  # move dataframes to server and get TableHandles for testing
  th1 <- client$import_table(df1)
  th2 <- client$import_table(df2)
  th3 <- client$import_table(df3)
  th4 <- client$import_table(df4)
  th5 <- client$import_table(df5)
  th6 <- client$import_table(df6)

  return(list(
    "client" = client,
    "df1" = df1, "df2" = df2, "df3" = df3, "df4" = df4, "df5" = df5, "df6" = df6,
    "th1" = th1, "th2" = th2, "th3" = th3, "th4" = th4, "th5" = th5, "th6" = th6
  ))
}

##### TESTING GOOD INPUTS #####

test_that("agg_first behaves as expected", {
  data <- setup()

  new_tb1 <- data$df1 %>%
    group_by(string_col) %>%
    summarise(int_col = first(int_col))
  new_th1 <- data$th1$
    agg_by(agg_first("int_col"), "string_col")$
    sort("string_col")
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))

  new_tb2 <- data$df2 %>%
    group_by(col1, col2) %>%
    summarise(col3 = first(col3))
  new_th2 <- data$th2$
    agg_by(agg_first("col3"), c("col1", "col2"))$
    sort(c("col1", "col2"))
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))

  new_tb3 <- data$df3 %>%
    group_by(X1, X2, X3, X4) %>%
    summarise(X5 = first(X5), X6 = first(X6), X7 = first(X7), X8 = first(X8))
  new_th3 <- data$th3$
    agg_by(agg_first(c("X5", "X6", "X7", "X8")), c("X1", "X2", "X3", "X4"))$
    sort(c("X1", "X2", "X3", "X4"))
  expect_equal(as.data.frame(new_th3), as.data.frame(new_tb3))

  new_tb4 <- data$df4 %>%
    group_by(bool_col) %>%
    summarise(time_col = first(time_col), int_col = first(int_col))
  new_th4 <- data$th4$
    agg_by(c(agg_first("time_col"), agg_first("int_col")), "bool_col")$
    sort("bool_col")
  expect_equal(as.data.frame(new_th4), as.data.frame(new_tb4))

  new_tb5 <- data$df5 %>%
    group_by(X) %>%
    summarise(Number1 = first(Number1), Number2 = first(Number2))
  new_th5 <- data$th5$
    agg_by(agg_first(c("Number1", "Number2")), "X")$
    sort("X")
  expect_equal(as.data.frame(new_th5), as.data.frame(new_tb5))

  new_tb6 <- data$df6 %>%
    group_by(X, Y) %>%
    summarise(Number1 = first(Number1), Number2 = first(Number2))
  new_th6 <- data$th6$
    agg_by(agg_first(c("Number1", "Number2")), c("X", "Y"))$
    sort(c("X", "Y"))
  expect_equal(as.data.frame(new_th6), as.data.frame(new_tb6))

  new_tb7 <- data$df5 %>%
    group_by(X) %>%
    summarise(across(everything(), first))
  new_th7 <- data$th5$
    agg_all_by(agg_first(), "X")
  expect_equal(as.data.frame(new_th7), as.data.frame(new_tb7))

  new_th8 <- rbind(data$df5, data$df6, data$df6, data$df5) %>%
    group_by(X, Y) %>%
    summarise(across(everything(), first))
  new_tb8 <- merge_tables(data$th5, data$th6, data$th6, data$th5)$
    agg_all_by(agg_first(), c("X", "Y"))$
    sort(c("X", "Y"))
  expect_equal(as.data.frame(new_th8), as.data.frame(new_tb8))

  data$client$close()
})

test_that("agg_last behaves as expected", {
  data <- setup()

  new_tb1 <- data$df1 %>%
    group_by(string_col) %>%
    summarise(int_col = last(int_col))
  new_th1 <- data$th1$
    agg_by(agg_last("int_col"), "string_col")$
    sort("string_col")
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))

  new_tb2 <- data$df2 %>%
    group_by(col1, col2) %>%
    summarise(col3 = last(col3))
  new_th2 <- data$th2$
    agg_by(agg_last("col3"), c("col1", "col2"))$
    sort(c("col1", "col2"))
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))

  new_tb3 <- data$df3 %>%
    group_by(X1, X2, X3, X4) %>%
    summarise(X5 = last(X5), X6 = last(X6), X7 = last(X7), X8 = last(X8))
  new_th3 <- data$th3$
    agg_by(agg_last(c("X5", "X6", "X7", "X8")), c("X1", "X2", "X3", "X4"))$
    sort(c("X1", "X2", "X3", "X4"))
  expect_equal(as.data.frame(new_th3), as.data.frame(new_tb3))

  new_tb4 <- data$df4 %>%
    group_by(bool_col) %>%
    summarise(time_col = last(time_col), int_col = last(int_col))
  new_th4 <- data$th4$
    agg_by(c(agg_last("time_col"), agg_last("int_col")), "bool_col")$
    sort("bool_col")
  expect_equal(as.data.frame(new_th4), as.data.frame(new_tb4))

  new_tb5 <- data$df5 %>%
    group_by(X) %>%
    summarise(Number1 = last(Number1), Number2 = last(Number2))
  new_th5 <- data$th5$
    agg_by(agg_last(c("Number1", "Number2")), "X")$
    sort("X")
  expect_equal(as.data.frame(new_th5), as.data.frame(new_tb5))

  new_tb6 <- data$df6 %>%
    group_by(X, Y) %>%
    summarise(Number1 = last(Number1), Number2 = last(Number2))
  new_th6 <- data$th6$
    agg_by(agg_last(c("Number1", "Number2")), c("X", "Y"))$
    sort(c("X", "Y"))
  expect_equal(as.data.frame(new_th6), as.data.frame(new_tb6))

  new_tb7 <- data$df5 %>%
    group_by(X) %>%
    summarise(across(everything(), last))
  new_th7 <- data$th5$
    agg_all_by(agg_last(), "X")
  expect_equal(as.data.frame(new_th7), as.data.frame(new_tb7))

  new_th8 <- rbind(data$df5, data$df6, data$df6, data$df5) %>%
    group_by(X, Y) %>%
    summarise(across(everything(), last))
  new_tb8 <- merge_tables(data$th5, data$th6, data$th6, data$th5)$
    agg_all_by(agg_last(), c("X", "Y"))$
    sort(c("X", "Y"))
  expect_equal(as.data.frame(new_th8), as.data.frame(new_tb8))

  data$client$close()
})

test_that("agg_min behaves as expected", {
  data <- setup()

  new_tb1 <- data$df1 %>%
    group_by(string_col) %>%
    summarise(int_col = min(int_col))
  new_th1 <- data$th1$
    agg_by(agg_min("int_col"), "string_col")$
    sort("string_col")
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))

  new_tb2 <- data$df2 %>%
    group_by(col1, col2) %>%
    summarise(col3 = min(col3))
  new_th2 <- data$th2$
    agg_by(agg_min("col3"), c("col1", "col2"))$
    sort(c("col1", "col2"))
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))

  new_tb3 <- data$df3 %>%
    group_by(X1, X2, X3, X4) %>%
    summarise(X5 = min(X5), X6 = min(X6), X7 = min(X7), X8 = min(X8))
  new_th3 <- data$th3$
    agg_by(agg_min(c("X5", "X6", "X7", "X8")), c("X1", "X2", "X3", "X4"))$
    sort(c("X1", "X2", "X3", "X4"))
  expect_equal(as.data.frame(new_th3), as.data.frame(new_tb3))

  new_tb4 <- data$df4 %>%
    group_by(bool_col) %>%
    summarise(time_col = min(time_col), int_col = min(int_col))
  new_th4 <- data$th4$
    agg_by(c(agg_min("time_col"), agg_min("int_col")), "bool_col")$
    sort("bool_col")
  expect_equal(as.data.frame(new_th4), as.data.frame(new_tb4))

  new_tb5 <- data$df5 %>%
    group_by(X) %>%
    summarise(Number1 = min(Number1), Number2 = min(Number2))
  new_th5 <- data$th5$
    agg_by(agg_min(c("Number1", "Number2")), "X")$
    sort("X")
  expect_equal(as.data.frame(new_th5), as.data.frame(new_tb5))

  new_tb6 <- data$df6 %>%
    group_by(X, Y) %>%
    summarise(Number1 = min(Number1), Number2 = min(Number2))
  new_th6 <- data$th6$
    agg_by(agg_min(c("Number1", "Number2")), c("X", "Y"))$
    sort(c("X", "Y"))
  expect_equal(as.data.frame(new_th6), as.data.frame(new_tb6))

  new_tb7 <- data$df5 %>%
    group_by(X) %>%
    summarise(across(everything(), min))
  new_th7 <- data$th5$
    agg_all_by(agg_min(), "X")
  expect_equal(as.data.frame(new_th7), as.data.frame(new_tb7))

  new_th8 <- rbind(data$df5, data$df6, data$df6, data$df5) %>%
    group_by(X, Y) %>%
    summarise(across(everything(), min))
  new_tb8 <- merge_tables(data$th5, data$th6, data$th6, data$th5)$
    agg_all_by(agg_min(), c("X", "Y"))$
    sort(c("X", "Y"))
  expect_equal(as.data.frame(new_th8), as.data.frame(new_tb8))

  data$client$close()
})

test_that("agg_max behaves as expected", {
  data <- setup()

  new_tb1 <- data$df1 %>%
    group_by(string_col) %>%
    summarise(int_col = max(int_col))
  new_th1 <- data$th1$
    agg_by(agg_max("int_col"), "string_col")$
    sort("string_col")
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))

  new_tb2 <- data$df2 %>%
    group_by(col1, col2) %>%
    summarise(col3 = max(col3))
  new_th2 <- data$th2$
    agg_by(agg_max("col3"), c("col1", "col2"))$
    sort(c("col1", "col2"))
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))

  new_tb3 <- data$df3 %>%
    group_by(X1, X2, X3, X4) %>%
    summarise(X5 = max(X5), X6 = max(X6), X7 = max(X7), X8 = max(X8))
  new_th3 <- data$th3$
    agg_by(agg_max(c("X5", "X6", "X7", "X8")), c("X1", "X2", "X3", "X4"))$
    sort(c("X1", "X2", "X3", "X4"))
  expect_equal(as.data.frame(new_th3), as.data.frame(new_tb3))

  new_tb4 <- data$df4 %>%
    group_by(bool_col) %>%
    summarise(time_col = max(time_col), int_col = max(int_col))
  new_th4 <- data$th4$
    agg_by(c(agg_max("time_col"), agg_max("int_col")), "bool_col")$
    sort("bool_col")
  expect_equal(as.data.frame(new_th4), as.data.frame(new_tb4))

  new_tb5 <- data$df5 %>%
    group_by(X) %>%
    summarise(Number1 = max(Number1), Number2 = max(Number2))
  new_th5 <- data$th5$
    agg_by(agg_max(c("Number1", "Number2")), "X")$
    sort("X")
  expect_equal(as.data.frame(new_th5), as.data.frame(new_tb5))

  new_tb6 <- data$df6 %>%
    group_by(X, Y) %>%
    summarise(Number1 = max(Number1), Number2 = max(Number2))
  new_th6 <- data$th6$
    agg_by(agg_max(c("Number1", "Number2")), c("X", "Y"))$
    sort(c("X", "Y"))
  expect_equal(as.data.frame(new_th6), as.data.frame(new_tb6))

  new_tb7 <- data$df5 %>%
    group_by(X) %>%
    summarise(across(everything(), max))
  new_th7 <- data$th5$
    agg_all_by(agg_max(), "X")
  expect_equal(as.data.frame(new_th7), as.data.frame(new_tb7))

  new_th8 <- rbind(data$df5, data$df6, data$df6, data$df5) %>%
    group_by(X, Y) %>%
    summarise(across(everything(), max))
  new_tb8 <- merge_tables(data$th5, data$th6, data$th6, data$th5)$
    agg_all_by(agg_max(), c("X", "Y"))$
    sort(c("X", "Y"))
  expect_equal(as.data.frame(new_th8), as.data.frame(new_tb8))

  data$client$close()
})

test_that("agg_sum behaves as expected", {
  data <- setup()

  new_tb1 <- data$df1 %>%
    group_by(string_col) %>%
    summarise(int_col = sum(int_col))
  new_th1 <- data$th1$
    agg_by(agg_sum("int_col"), "string_col")$
    sort("string_col")
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))

  new_tb2 <- data$df2 %>%
    group_by(col1, col2) %>%
    summarise(col3 = sum(col3))
  new_th2 <- data$th2$
    agg_by(agg_sum("col3"), c("col1", "col2"))$
    sort(c("col1", "col2"))
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))

  new_tb3 <- data$df3 %>%
    group_by(X1, X2, X3, X4) %>%
    summarise(X5 = sum(X5), X6 = sum(X6), X7 = sum(X7), X8 = sum(X8))
  new_th3 <- data$th3$
    agg_by(agg_sum(c("X5", "X6", "X7", "X8")), c("X1", "X2", "X3", "X4"))$
    sort(c("X1", "X2", "X3", "X4"))
  expect_equal(as.data.frame(new_th3), as.data.frame(new_tb3))

  new_tb4 <- data$df4 %>%
    group_by(bool_col) %>%
    summarise(int_col = sum(int_col))
  new_th4 <- data$th4$
    agg_by(agg_sum("int_col"), "bool_col")$
    sort("bool_col")
  expect_equal(as.data.frame(new_th4), as.data.frame(new_tb4))

  new_tb5 <- data$df5 %>%
    group_by(X) %>%
    summarise(Number1 = sum(Number1), Number2 = sum(Number2))
  new_th5 <- data$th5$
    agg_by(agg_sum(c("Number1", "Number2")), "X")$
    sort("X")
  expect_equal(as.data.frame(new_th5), as.data.frame(new_tb5))

  new_tb6 <- data$df6 %>%
    group_by(X, Y) %>%
    summarise(Number1 = sum(Number1), Number2 = sum(Number2))
  new_th6 <- data$th6$
    agg_by(agg_sum(c("Number1", "Number2")), c("X", "Y"))$
    sort(c("X", "Y"))
  expect_equal(as.data.frame(new_th6), as.data.frame(new_tb6))

  new_tb7 <- data$df5 %>%
    select(-Y) %>%
    group_by(X) %>%
    summarise(across(everything(), sum))
  new_th7 <- data$th5$
    drop_columns("Y")$
    agg_all_by(agg_sum(), "X")
  expect_equal(as.data.frame(new_th7), as.data.frame(new_tb7))

  new_th8 <- rbind(data$df5, data$df6, data$df6, data$df5) %>%
    group_by(X, Y) %>%
    summarise(across(everything(), sum))
  new_tb8 <- merge_tables(data$th5, data$th6, data$th6, data$th5)$
    agg_all_by(agg_sum(), c("X", "Y"))$
    sort(c("X", "Y"))
  expect_equal(as.data.frame(new_th8), as.data.frame(new_tb8))

  data$client$close()
})

test_that("agg_abs_sum behaves as expected", {
  data <- setup()

  new_tb1 <- data$df1 %>%
    group_by(string_col) %>%
    summarise(int_col = sum(abs(int_col)))
  new_th1 <- data$th1$
    agg_by(agg_abs_sum("int_col"), "string_col")$
    sort("string_col")
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))

  new_tb2 <- data$df2 %>%
    group_by(col1, col2) %>%
    summarise(col3 = sum(abs(col3)))
  new_th2 <- data$th2$
    agg_by(agg_abs_sum("col3"), c("col1", "col2"))$
    sort(c("col1", "col2"))
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))

  new_tb3 <- data$df3 %>%
    group_by(X1, X2, X3, X4) %>%
    summarise(X5 = sum(abs(X5)), X6 = sum(abs(X6)), X7 = sum(abs(X7)), X8 = sum(abs(X8)))
  new_th3 <- data$th3$
    agg_by(agg_abs_sum(c("X5", "X6", "X7", "X8")), c("X1", "X2", "X3", "X4"))$
    sort(c("X1", "X2", "X3", "X4"))
  expect_equal(as.data.frame(new_th3), as.data.frame(new_tb3))

  new_tb4 <- data$df4 %>%
    group_by(bool_col) %>%
    summarise(int_col = sum(abs(int_col)))
  new_th4 <- data$th4$
    agg_by(agg_abs_sum("int_col"), "bool_col")$
    sort("bool_col")
  expect_equal(as.data.frame(new_th4), as.data.frame(new_tb4))

  new_tb5 <- data$df5 %>%
    group_by(X) %>%
    summarise(Number1 = sum(abs(Number1)), Number2 = sum(abs(Number2)))
  new_th5 <- data$th5$
    agg_by(agg_abs_sum(c("Number1", "Number2")), "X")$
    sort("X")
  expect_equal(as.data.frame(new_th5), as.data.frame(new_tb5))

  new_tb6 <- data$df6 %>%
    group_by(X, Y) %>%
    summarise(Number1 = sum(abs(Number1)), Number2 = sum(abs(Number2)))
  new_th6 <- data$th6$
    agg_by(agg_abs_sum(c("Number1", "Number2")), c("X", "Y"))$
    sort(c("X", "Y"))
  expect_equal(as.data.frame(new_th6), as.data.frame(new_tb6))

  new_tb7 <- data$df5 %>%
    select(-Y) %>%
    group_by(X) %>%
    summarise(across(everything(), ~ sum(abs(.x))))
  new_th7 <- data$th5$
    drop_columns("Y")$
    agg_all_by(agg_abs_sum(), "X")
  expect_equal(as.data.frame(new_th7), as.data.frame(new_tb7))

  new_th8 <- rbind(data$df5, data$df6, data$df6, data$df5) %>%
    group_by(X, Y) %>%
    summarise(across(everything(), ~ sum(abs(.x))))
  new_tb8 <- merge_tables(data$th5, data$th6, data$th6, data$th5)$
    agg_all_by(agg_abs_sum(), c("X", "Y"))$
    sort(c("X", "Y"))
  expect_equal(as.data.frame(new_th8), as.data.frame(new_tb8))

  data$client$close()
})

test_that("agg_avg behaves as expected", {
  data <- setup()

  new_tb1 <- data$df1 %>%
    group_by(string_col) %>%
    summarise(int_col = mean(int_col))
  new_th1 <- data$th1$
    agg_by(agg_avg("int_col"), "string_col")$
    sort("string_col")
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))

  new_tb2 <- data$df2 %>%
    group_by(col1, col2) %>%
    summarise(col3 = mean(col3))
  new_th2 <- data$th2$
    agg_by(agg_avg("col3"), c("col1", "col2"))$
    sort(c("col1", "col2"))
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))

  new_tb3 <- data$df3 %>%
    group_by(X1, X2, X3, X4) %>%
    summarise(X5 = mean(X5), X6 = mean(X6), X7 = mean(X7), X8 = mean(X8))
  new_th3 <- data$th3$
    agg_by(agg_avg(c("X5", "X6", "X7", "X8")), c("X1", "X2", "X3", "X4"))$
    sort(c("X1", "X2", "X3", "X4"))
  expect_equal(as.data.frame(new_th3), as.data.frame(new_tb3))

  new_tb4 <- data$df4 %>%
    group_by(bool_col) %>%
    summarise(int_col = mean(int_col))
  new_th4 <- data$th4$
    agg_by(agg_avg("int_col"), "bool_col")$
    sort("bool_col")
  expect_equal(as.data.frame(new_th4), as.data.frame(new_tb4))

  new_tb5 <- data$df5 %>%
    group_by(X) %>%
    summarise(Number1 = mean(Number1), Number2 = mean(Number2))
  new_th5 <- data$th5$
    agg_by(agg_avg(c("Number1", "Number2")), "X")$
    sort("X")
  expect_equal(as.data.frame(new_th5), as.data.frame(new_tb5))

  new_tb6 <- data$df6 %>%
    group_by(X, Y) %>%
    summarise(Number1 = mean(Number1), Number2 = mean(Number2))
  new_th6 <- data$th6$
    agg_by(agg_avg(c("Number1", "Number2")), c("X", "Y"))$
    sort(c("X", "Y"))
  expect_equal(as.data.frame(new_th6), as.data.frame(new_tb6))

  new_tb7 <- data$df5 %>%
    select(-Y) %>%
    group_by(X) %>%
    summarise(across(everything(), mean))
  new_th7 <- data$th5$
    drop_columns("Y")$
    agg_all_by(agg_avg(), "X")
  expect_equal(as.data.frame(new_th7), as.data.frame(new_tb7))

  new_th8 <- rbind(data$df5, data$df6, data$df6, data$df5) %>%
    group_by(X, Y) %>%
    summarise(across(everything(), mean))
  new_tb8 <- merge_tables(data$th5, data$th6, data$th6, data$th5)$
    agg_all_by(agg_avg(), c("X", "Y"))$
    sort(c("X", "Y"))
  expect_equal(as.data.frame(new_th8), as.data.frame(new_tb8))

  data$client$close()
})

test_that("agg_w_avg behaves as expected", {
  data <- setup()

  new_tb1 <- data$df1 %>%
    group_by(string_col) %>%
    summarise(int_col = weighted.mean(int_col, dbl_col))
  new_th1 <- data$th1$
    agg_by(agg_w_avg("dbl_col", "int_col"), "string_col")$
    sort("string_col")
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))

  new_tb2 <- data$df2 %>%
    group_by(col1, col2) %>%
    summarise(col3 = weighted.mean(col3, col1))
  new_th2 <- data$th2$
    agg_by(agg_w_avg("col1", "col3"), c("col1", "col2"))$
    sort(c("col1", "col2"))
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))

  new_tb3 <- data$df3 %>%
    group_by(X1, X2, X3, X4) %>%
    summarise(
      X5 = weighted.mean(X5, X9), X6 = weighted.mean(X6, X9),
      X7 = weighted.mean(X7, X9), X8 = weighted.mean(X8, X9)
    )
  new_th3 <- data$th3$
    agg_by(agg_w_avg("X9", c("X5", "X6", "X7", "X8")), c("X1", "X2", "X3", "X4"))$
    sort(c("X1", "X2", "X3", "X4"))
  expect_equal(as.data.frame(new_th3), as.data.frame(new_tb3))

  new_tb4 <- data$df4 %>%
    group_by(bool_col) %>%
    summarise(int_col = weighted.mean(int_col, int_col))
  new_th4 <- data$th4$
    agg_by(agg_w_avg("int_col", "int_col"), "bool_col")$
    sort("bool_col")
  expect_equal(as.data.frame(new_th4), as.data.frame(new_tb4))

  new_tb5 <- data$df5 %>%
    group_by(X) %>%
    mutate(weights = Number1 * Number2) %>%
    summarise(
      Number1 = weighted.mean(Number1, weights),
      Number2 = weighted.mean(Number2, weights)
    )
  new_th5 <- data$th5$
    update("weights = Number1 * Number2")$
    agg_by(agg_w_avg("weights", c("Number1", "Number2")), "X")$
    sort("X")
  expect_equal(as.data.frame(new_th5), as.data.frame(new_tb5))

  new_tb6 <- data$df6 %>%
    group_by(X, Y) %>%
    mutate(weights = Number1 * Number2) %>%
    summarise(Number1 = weighted.mean(Number1, weights), Number2 = weighted.mean(Number2, weights))
  new_th6 <- data$th6$
    update("weights = Number1 * Number2")$
    agg_by(agg_w_avg("weights", c("Number1", "Number2")), c("X", "Y"))$
    sort(c("X", "Y"))
  expect_equal(as.data.frame(new_th6), as.data.frame(new_tb6))

  new_tb7 <- data$df5 %>%
    select(-Y) %>%
    group_by(X) %>%
    summarise(across(everything(), ~ weighted.mean(.x, Number2))) %>%
    select(-Number2)
  new_th7 <- data$th5$
    drop_columns("Y")$
    agg_all_by(agg_w_avg("Number2"), "X")
  expect_equal(as.data.frame(new_th7), as.data.frame(new_tb7))

  new_th8 <- rbind(data$df5, data$df6, data$df6, data$df5) %>%
    group_by(X, Y) %>%
    summarise(across(everything(), ~ weighted.mean(.x, Number2))) %>%
    select(-Number2)
  new_tb8 <- merge_tables(data$th5, data$th6, data$th6, data$th5)$
    agg_all_by(agg_w_avg("Number2"), c("X", "Y"))$
    sort(c("X", "Y"))
  expect_equal(as.data.frame(new_th8), as.data.frame(new_tb8))

  data$client$close()
})

test_that("agg_median behaves as expected", {
  data <- setup()

  new_tb1 <- data$df1 %>%
    group_by(string_col) %>%
    summarise(int_col = median(int_col))
  new_th1 <- data$th1$
    agg_by(agg_median("int_col"), "string_col")$
    sort("string_col")
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))

  new_tb2 <- data$df2 %>%
    group_by(col1, col2) %>%
    summarise(col3 = median(col3))
  new_th2 <- data$th2$
    agg_by(agg_median("col3"), c("col1", "col2"))$
    sort(c("col1", "col2"))
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))

  new_tb3 <- data$df3 %>%
    group_by(X1, X2, X3, X4) %>%
    summarise(X5 = median(X5), X6 = median(X6), X7 = median(X7), X8 = median(X8))
  new_th3 <- data$th3$
    agg_by(agg_median(c("X5", "X6", "X7", "X8")), c("X1", "X2", "X3", "X4"))$
    sort(c("X1", "X2", "X3", "X4"))
  expect_equal(as.data.frame(new_th3), as.data.frame(new_tb3))

  new_tb4 <- data$df4 %>%
    group_by(bool_col) %>%
    summarise(int_col = median(int_col))
  new_th4 <- data$th4$
    agg_by(agg_median("int_col"), "bool_col")$
    sort("bool_col")
  expect_equal(as.data.frame(new_th4), as.data.frame(new_tb4))

  new_tb5 <- data$df5 %>%
    group_by(X) %>%
    summarise(Number1 = median(Number1), Number2 = median(Number2))
  new_th5 <- data$th5$
    agg_by(agg_median(c("Number1", "Number2")), "X")$
    sort("X")
  expect_equal(as.data.frame(new_th5), as.data.frame(new_tb5))

  new_tb6 <- data$df6 %>%
    group_by(X, Y) %>%
    summarise(Number1 = median(Number1), Number2 = median(Number2))
  new_th6 <- data$th6$
    agg_by(agg_median(c("Number1", "Number2")), c("X", "Y"))$
    sort(c("X", "Y"))
  expect_equal(as.data.frame(new_th6), as.data.frame(new_tb6))

  new_tb7 <- data$df5 %>%
    select(-Y) %>%
    group_by(X) %>%
    summarise(across(everything(), median))
  new_th7 <- data$th5$
    drop_columns("Y")$
    agg_all_by(agg_median(), "X")
  expect_equal(as.data.frame(new_th7), as.data.frame(new_tb7))

  new_th8 <- rbind(data$df5, data$df6, data$df6, data$df5) %>%
    group_by(X, Y) %>%
    summarise(across(everything(), median))
  new_tb8 <- merge_tables(data$th5, data$th6, data$th6, data$th5)$
    agg_all_by(agg_median(), c("X", "Y"))$
    sort(c("X", "Y"))
  expect_equal(as.data.frame(new_th8), as.data.frame(new_tb8))

  data$client$close()
})

test_that("agg_var behaves as expected", {
  data <- setup()

  new_tb1 <- data$df1 %>%
    group_by(string_col) %>%
    summarise(int_col = var(int_col))
  new_th1 <- data$th1$
    agg_by(agg_var("int_col"), "string_col")$
    sort("string_col")
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))

  new_tb2 <- data$df2 %>%
    group_by(col1, col2) %>%
    summarise(col3 = var(col3))
  new_th2 <- data$th2$
    agg_by(agg_var("col3"), c("col1", "col2"))$
    sort(c("col1", "col2"))
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))

  new_tb3 <- data$df3 %>%
    group_by(X1, X2, X3, X4) %>%
    summarise(X5 = var(X5), X6 = var(X6), X7 = var(X7), X8 = var(X8))
  new_th3 <- data$th3$
    agg_by(agg_var(c("X5", "X6", "X7", "X8")), c("X1", "X2", "X3", "X4"))$
    sort(c("X1", "X2", "X3", "X4"))
  expect_equal(as.data.frame(new_th3), as.data.frame(new_tb3))

  new_tb4 <- data$df4 %>%
    group_by(bool_col) %>%
    summarise(int_col = var(int_col))
  new_th4 <- data$th4$
    agg_by(agg_var("int_col"), "bool_col")$
    sort("bool_col")
  expect_equal(as.data.frame(new_th4), as.data.frame(new_tb4))

  new_tb5 <- data$df5 %>%
    group_by(X) %>%
    summarise(Number1 = var(Number1), Number2 = var(Number2))
  new_th5 <- data$th5$
    agg_by(agg_var(c("Number1", "Number2")), "X")$
    sort("X")
  expect_equal(as.data.frame(new_th5), as.data.frame(new_tb5))

  new_tb6 <- data$df6 %>%
    group_by(X, Y) %>%
    summarise(Number1 = var(Number1), Number2 = var(Number2))
  new_th6 <- data$th6$
    agg_by(agg_var(c("Number1", "Number2")), c("X", "Y"))$
    sort(c("X", "Y"))
  expect_equal(as.data.frame(new_th6), as.data.frame(new_tb6))

  new_tb7 <- data$df5 %>%
    select(-Y) %>%
    group_by(X) %>%
    summarise(across(everything(), var))
  new_th7 <- data$th5$
    drop_columns("Y")$
    agg_all_by(agg_var(), "X")
  expect_equal(as.data.frame(new_th7), as.data.frame(new_tb7))

  new_th8 <- rbind(data$df5, data$df6, data$df6, data$df5) %>%
    group_by(X, Y) %>%
    summarise(across(everything(), var))
  new_tb8 <- merge_tables(data$th5, data$th6, data$th6, data$th5)$
    agg_all_by(agg_var(), c("X", "Y"))$
    sort(c("X", "Y"))
  expect_equal(as.data.frame(new_th8), as.data.frame(new_tb8))

  data$client$close()
})

test_that("agg_std behaves as expected", {
  data <- setup()

  new_tb1 <- data$df1 %>%
    group_by(string_col) %>%
    summarise(int_col = sd(int_col))
  new_th1 <- data$th1$
    agg_by(agg_std("int_col"), "string_col")$
    sort("string_col")
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))

  new_tb2 <- data$df2 %>%
    group_by(col1, col2) %>%
    summarise(col3 = sd(col3))
  new_th2 <- data$th2$
    agg_by(agg_std("col3"), c("col1", "col2"))$
    sort(c("col1", "col2"))
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))

  new_tb3 <- data$df3 %>%
    group_by(X1, X2, X3, X4) %>%
    summarise(X5 = sd(X5), X6 = sd(X6), X7 = sd(X7), X8 = sd(X8))
  new_th3 <- data$th3$
    agg_by(agg_std(c("X5", "X6", "X7", "X8")), c("X1", "X2", "X3", "X4"))$
    sort(c("X1", "X2", "X3", "X4"))
  expect_equal(as.data.frame(new_th3), as.data.frame(new_tb3))

  new_tb4 <- data$df4 %>%
    group_by(bool_col) %>%
    summarise(int_col = sd(int_col))
  new_th4 <- data$th4$
    agg_by(agg_std("int_col"), "bool_col")$
    sort("bool_col")
  expect_equal(as.data.frame(new_th4), as.data.frame(new_tb4))

  new_tb5 <- data$df5 %>%
    group_by(X) %>%
    summarise(Number1 = sd(Number1), Number2 = sd(Number2))
  new_th5 <- data$th5$
    agg_by(agg_std(c("Number1", "Number2")), "X")$
    sort("X")
  expect_equal(as.data.frame(new_th5), as.data.frame(new_tb5))

  new_tb6 <- data$df6 %>%
    group_by(X, Y) %>%
    summarise(Number1 = sd(Number1), Number2 = sd(Number2))
  new_th6 <- data$th6$
    agg_by(agg_std(c("Number1", "Number2")), c("X", "Y"))$
    sort(c("X", "Y"))
  expect_equal(as.data.frame(new_th6), as.data.frame(new_tb6))

  new_tb7 <- data$df5 %>%
    select(-Y) %>%
    group_by(X) %>%
    summarise(across(everything(), sd))
  new_th7 <- data$th5$
    drop_columns("Y")$
    agg_all_by(agg_std(), "X")
  expect_equal(as.data.frame(new_th7), as.data.frame(new_tb7))

  new_th8 <- rbind(data$df5, data$df6, data$df6, data$df5) %>%
    group_by(X, Y) %>%
    summarise(across(everything(), sd))
  new_tb8 <- merge_tables(data$th5, data$th6, data$th6, data$th5)$
    agg_all_by(agg_std(), c("X", "Y"))$
    sort(c("X", "Y"))
  expect_equal(as.data.frame(new_th8), as.data.frame(new_tb8))

  data$client$close()
})

test_that("agg_percentile behaves as expected", {
  # There is not a clean analog to agg_percentile in dplyr, so we create the
  # dataframes directly, and only make comparisons on deterministic data frames.

  data <- setup()

  new_df1 <- data.frame(int_col = 2)
  new_th1 <- data$th1$
    agg_by(agg_percentile(0.4, "int_col"))
  expect_equal(as.data.frame(new_th1), new_df1)

  new_df2 <- data.frame(
    X = c("A", "B", "C"),
    Number1 = c(50, 18, 11),
    Number2 = c(-50, 137, 214)
  )
  new_th2 <- data$th5$
    agg_by(agg_percentile(0.6, c("Number1", "Number2")), "X")$
    sort("X")
  expect_equal(as.data.frame(new_th2), new_df2)

  new_df3 <- data.frame(
    X = c("A", "A", "B", "B", "B", "C", "C"),
    Y = c("O", "P", "M", "N", "O", "N", "P"),
    Number1 = c(-5, -45, 86, 55, 99, -65, 0),
    Number2 = c(6, 34, -6, 76, 34, -5, -76)
  )
  new_th3 <- data$th6$
    agg_by(agg_percentile(0.3, c("Number1", "Number2")), c("X", "Y"))$
    sort(c("X", "Y"))
  expect_equal(as.data.frame(new_th3), new_df3)

  new_df4 <- data.frame(
    X = c("A", "B", "C"),
    Number1 = c(50, -44, -70),
    Number2 = c(-50, 76, 130)
  )
  new_th4 <- data$th5$
    drop_columns("Y")$
    agg_all_by(agg_percentile(0.4), "X")
  expect_equal(as.data.frame(new_th4), new_df4)

  new_df5 <- data.frame(
    X = c("A", "B", "A", "C", "B", "B", "C", "B", "A", "C"),
    Y = c("M", "N", "O", "N", "P", "O", "M", "M", "P", "P"),
    Number1 = c(50, -44, 1, 11, -66, 99, -70, 86, -45, 0),
    Number2 = c(-55, 76, 12, 4, 137, 45, 214, -6, 34, -76)
  )
  new_th5 <- merge_tables(data$th5, data$th6, data$th6, data$th5)$
    agg_all_by(agg_percentile(0.4), c("X", "Y"))
  expect_equal(as.data.frame(new_th4), new_df4)

  data$client$close()
})

test_that("agg_count behaves as expected", {
  data <- setup()

  new_tb1 <- data$df1 %>%
    count(string_col)
  new_th1 <- data$th1$
    agg_by(agg_count("n"), "string_col")$
    sort("string_col")
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))

  new_tb2 <- data$df2 %>%
    count(col1, col2)
  new_th2 <- data$th2$
    agg_by(agg_count("n"), c("col1", "col2"))$
    sort(c("col1", "col2"))
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))

  new_tb3 <- data$df3 %>%
    count(X1, X2, X3, X4)
  new_th3 <- data$th3$
    agg_by(agg_count("n"), c("X1", "X2", "X3", "X4"))$
    sort(c("X1", "X2", "X3", "X4"))
  expect_equal(as.data.frame(new_th3), as.data.frame(new_tb3))

  new_tb4 <- data$df4 %>%
    count(bool_col)
  new_th4 <- data$th4$
    agg_by(agg_count("n"), "bool_col")$
    sort("bool_col")
  expect_equal(as.data.frame(new_th4), as.data.frame(new_tb4))

  new_tb5 <- data$df5 %>%
    count(X)
  new_th5 <- data$th5$
    agg_by(agg_count("n"), "X")$
    sort("X")
  expect_equal(as.data.frame(new_th5), as.data.frame(new_tb5))

  new_tb6 <- data$df6 %>%
    count(X, Y)
  new_th6 <- data$th6$
    agg_by(agg_count("n"), c("X", "Y"))$
    sort(c("X", "Y"))
  expect_equal(as.data.frame(new_th6), as.data.frame(new_tb6))

  new_th7 <- data$th5$
    agg_all_by(agg_count("n"), "X")$
    sort("X")
  expect_equal(as.data.frame(new_th7), as.data.frame(new_tb5))

  new_th8 <- data$th6$
    agg_all_by(agg_count("n"), c("X", "Y"))$
    sort(c("X", "Y"))
  expect_equal(as.data.frame(new_th8), as.data.frame(new_tb6))

  data$client$close()
})

##### TESTING BAD INPUTS #####

test_that("agg_by behaves nicely when given bad input", {
  data <- setup()

  expect_error(data$th1$agg_by(agg_first()),
    "Aggregations with no columns cannot be used in 'agg_by'. Got 'agg_first' at index 1 with an empty 'cols' argument.")

  expect_error(data$th1$agg_by(c(agg_first("int_col"), agg_last())),
    "Aggregations with no columns cannot be used in 'agg_by'. Got 'agg_last' at index 2 with an empty 'cols' argument.")

  expect_error(data$th1$agg_by(c(agg_first("int_col"), agg_last("int_col"), agg_count("n"), agg_avg())),
    "Aggregations with no columns cannot be used in 'agg_by'. Got 'agg_avg' at index 4 with an empty 'cols' argument.")

  data$client$close()
})
