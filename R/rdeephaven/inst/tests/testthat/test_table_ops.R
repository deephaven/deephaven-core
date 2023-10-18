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

test_that("merge_tables behaves as expected", {
  data <- setup()
  
  new_df1 <- rbind(data$df5)
  new_th1a <- data$th5$merge()
  new_th1b <- merge_tables(data$th5)
  expect_equal(as.data.frame(new_th1a), new_df1)
  expect_equal(as.data.frame(new_th1b), new_df1)
  
  new_df2 <- rbind(data$df5, data$df6)
  new_th2a <- data$th5$merge(data$th6)
  new_th2b <- merge_tables(data$th5, data$th6)
  expect_equal(as.data.frame(new_th2a), new_df2)
  expect_equal(as.data.frame(new_th2b), new_df2)
  
  new_df3 <- rbind(data$df5, data$df6, data$df6, data$df5)
  new_th3a <- data$th5$merge(data$th6, data$th6, data$th5)
  new_th3b <- merge_tables(data$th5, data$th6, data$th6, data$th5)
  expect_equal(as.data.frame(new_th3a), new_df3)
  expect_equal(as.data.frame(new_th3b), new_df3)
  
  new_th4a <- data$th5$merge(c(data$th6))
  new_th4b <- merge_tables(data$th5, c(data$th6))
  new_th4c <- merge_tables(c(data$th5, data$th6))
  expect_equal(as.data.frame(new_th4a), new_df2)
  expect_equal(as.data.frame(new_th4b), new_df2)
  expect_equal(as.data.frame(new_th4c), new_df2)
  
  new_th5a <- data$th5$merge(c(data$th6, NULL, data$th6, data$th5))
  new_th5b <- merge_tables(data$th5, c(data$th6, NULL, data$th6, data$th5))
  new_th5c <- merge_tables(c(data$th5, data$th6, NULL, data$th6, data$th5))
  new_th5d <- merge_tables(data$th5, data$th6, NULL, data$th6, data$th5)
  new_th5e <- merge_tables(c(data$th5, data$th6), NULL, data$th6, data$th5)
  new_th5f <- merge_tables(NULL, NULL, c(data$th5, data$th6), NULL, data$th6, data$th5)
  expect_equal(as.data.frame(new_th5a), new_df3)
  expect_equal(as.data.frame(new_th5b), new_df3)
  expect_equal(as.data.frame(new_th5c), new_df3)
  expect_equal(as.data.frame(new_th5d), new_df3)
  expect_equal(as.data.frame(new_th5e), new_df3)
  expect_equal(as.data.frame(new_th5f), new_df3)
  
  data$client$close()
})

test_that("select behaves as expected", {
  data <- setup()

  new_tb1 <- data$df1 %>%
    select(string_col)
  new_th1 <- data$th1$
    select("string_col")
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))

  new_tb2 <- data$df2 %>%
    select(col2, col3)
  new_th2 <- data$th2$
    select(c("col2", "col3"))
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))

  new_tb3 <- data$df3 %>%
    select(X1, X2) %>%
    rename(first_col = X1)
  new_th3 <- data$th3$
    select(c("first_col = X1", "X2"))
  expect_equal(as.data.frame(new_th3), as.data.frame(new_tb3))

  new_tb4 <- data$df4 %>%
    select(int_col) %>%
    mutate(new_col = int_col + 1, .keep = "none")
  new_th4 <- data$th4$
    select("new_col = int_col + 1")
  expect_equal(as.data.frame(new_th4), as.data.frame(new_tb4))

  new_tb5 <- data$df5 %>%
    mutate(Number3 = Number1 * Number2) %>%
    select(X, Number3)
  new_th5 <- data$th5$
    select(c("X", "Number3 = Number1 * Number2"))
  expect_equal(as.data.frame(new_th5), as.data.frame(new_tb5))

  data$client$close()
})

test_that("view behaves as expected", {
  data <- setup()

  new_tb1 <- data$df1 %>%
    select(string_col)
  new_th1 <- data$th1$
    view("string_col")
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))

  new_tb2 <- data$df2 %>%
    select(col2, col3)
  new_th2 <- data$th2$
    view(c("col2", "col3"))
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))

  new_tb3 <- data$df3 %>%
    select(X1, X2) %>%
    rename(first_col = X1)
  new_th3 <- data$th3$
    view(c("first_col = X1", "X2"))
  expect_equal(as.data.frame(new_th3), as.data.frame(new_tb3))

  new_tb4 <- data$df4 %>%
    select(int_col) %>%
    mutate(new_col = int_col + 1, .keep = "none")
  new_th4 <- data$th4$
    view("new_col = int_col + 1")
  expect_equal(as.data.frame(new_th4), as.data.frame(new_tb4))

  new_tb5 <- data$df5 %>%
    mutate(Number3 = Number1 * Number2) %>%
    select(X, Number3)
  new_th5 <- data$th5$
    view(c("X", "Number3 = Number1 * Number2"))
  expect_equal(as.data.frame(new_th5), as.data.frame(new_tb5))

  data$client$close()
})

test_that("update behaves as expected", {
  data <- setup()

  new_tb1 <- data$df1 %>%
    mutate(dbl_col_again = dbl_col)
  new_th1 <- data$th1$
    update("dbl_col_again = dbl_col")
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))

  new_tb2 <- data$df2 %>%
    mutate(col4 = col3 * 2)
  new_th2 <- data$th2$
    update("col4 = col3 * 2")
  expect_equal(as.data.frame(new_tb2), as.data.frame(new_th2))

  new_tb3 <- data$df3 %>%
    mutate(X1001 = X1000, X1002 = X1001)
  new_th3 <- data$th3$
    update(c("X1001 = X1000", "X1002 = X1001"))
  expect_equal(as.data.frame(new_tb3), as.data.frame(new_th3))

  new_tb4 <- data$df4 %>%
    mutate(new_col = sqrt(3 * int_col))
  new_th4 <- data$th4$
    update("new_col = sqrt(3 * int_col)")
  expect_equal(as.data.frame(new_tb4), as.data.frame(new_th4))

  new_tb5 <- data$df5 %>%
    mutate(Number3 = Number1 + Number2)
  new_th5 <- data$th5$
    update("Number3 = Number1 + Number2")
  expect_equal(as.data.frame(new_tb5), as.data.frame(new_th5))

  data$client$close()
})

test_that("update_view behaves as expected", {
  data <- setup()

  new_tb1 <- data$df1 %>%
    mutate(dbl_col_again = dbl_col)
  new_th1 <- data$th1$
    update_view("dbl_col_again = dbl_col")
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))

  new_tb2 <- data$df2 %>%
    mutate(col4 = col3 * 2)
  new_th2 <- data$th2$
    update_view("col4 = col3 * 2")
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))

  new_tb3 <- data$df3 %>%
    mutate(X1001 = X1000, X1002 = X1001)
  new_th3 <- data$th3$
    update_view(c("X1001 = X1000", "X1002 = X1001"))
  expect_equal(as.data.frame(new_th3), as.data.frame(new_tb3))

  new_tb4 <- data$df4 %>%
    mutate(new_col = sqrt(3 * int_col))
  new_th4 <- data$th4$
    update_view("new_col = sqrt(3 * int_col)")
  expect_equal(as.data.frame(new_th4), as.data.frame(new_tb4))

  new_tb5 <- data$df5 %>%
    mutate(Number3 = Number1 + Number2)
  new_th5 <- data$th5$
    update_view("Number3 = Number1 + Number2")
  expect_equal(as.data.frame(new_th5), as.data.frame(new_tb5))

  data$client$close()
})

test_that("drop_columns behaves as expected", {
  data <- setup()

  new_tb1 <- data$df1 %>%
    select(-string_col)
  new_th1 <- data$th1$
    drop_columns("string_col")
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))

  new_tb2 <- data$df2 %>%
    select(-c(col1, col2))
  new_th2 <- data$th2$
    drop_columns(c("col1", "col2"))
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))

  new_tb3 <- data$df3 %>%
    select(-paste0("X", seq(2, 1000)))
  new_th3 <- data$th3$
    drop_columns(paste0("X", seq(2, 1000)))
  expect_equal(as.data.frame(new_th3), as.data.frame(new_tb3))

  data$client$close()
})

test_that("where behaves as expected", {
  data <- setup()

  new_tb1 <- data$df1 %>%
    filter(int_col < 3)
  new_th1 <- data$th1$
    where("int_col < 3")
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))

  new_tb2 <- data$df2 %>%
    filter(col2 == "hello!")
  new_th2 <- data$th2$
    where("col2 == `hello!`")
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))

  new_tb3 <- data$df3 %>%
    filter(X1 - X4 + X8 + X32 - 2 * X5 >= 0)
  new_th3 <- data$th3$
    where("X1 - X4 + X8 + X32 - 2*X5 >= 0")
  expect_equal(as.data.frame(new_th3), as.data.frame(new_tb3))

  data$client$close()
})

test_that("group_by and ungroup behave as expected", {
  data <- setup()

  # There is not a clean analog to group_by() in dplyr, so we evaluate
  # correctness by evaluating that these functions behave as inverses.
  # Easiest when grouping columns are first, otherwise we must also reorder.

  new_th1 <- data$th1$
    group_by("string_col")$
    ungroup()$
    sort("string_col")
  expect_equal(as.data.frame(new_th1), as.data.frame(data$th1$sort("string_col")))

  new_th3 <- data$th3$
    group_by(c("X1", "X2", "X3", "X4", "X5"))$
    ungroup()$
    sort(c("X1", "X2", "X3", "X4", "X5"))
  expect_equal(as.data.frame(new_th3), as.data.frame(data$th3$sort(c("X1", "X2", "X3", "X4", "X5"))))

  new_th5 <- data$th5$
    group_by("X")$
    ungroup()$
    sort("X")
  expect_equal(as.data.frame(new_th5), as.data.frame(data$th5$sort("X")))

  new_th6 <- data$th6$
    group_by(c("X", "Y"))$
    ungroup()$
    sort(c("X", "Y"))
  expect_equal(as.data.frame(new_th6), as.data.frame(data$th6$sort(c("X", "Y"))))

  data$client$close()
})

test_that("first_by behaves as expected", {
  data <- setup()

  new_tb1 <- data$df5 %>%
    select(-Y) %>%
    group_by(X) %>%
    summarise(across(everything(), first))
  new_th1 <- data$th5$
    drop_columns("Y")$
    first_by("X")
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))

  new_tb2 <- data$df5 %>%
    group_by(X, Y) %>%
    summarise(across(everything(), first)) %>%
    arrange(X, Y)
  new_th2 <- data$th5$
    first_by(c("X", "Y"))$
    sort(c("X", "Y"))
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))

  data$client$close()
})

test_that("last_by behaves as expected", {
  data <- setup()

  new_tb1 <- data$df5 %>%
    select(-Y) %>%
    group_by(X) %>%
    summarise(across(everything(), last))
  new_th1 <- data$th5$
    drop_columns("Y")$
    last_by("X")
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))

  new_tb2 <- data$df5 %>%
    group_by(X, Y) %>%
    summarise(across(everything(), last)) %>%
    arrange(X, Y)
  new_th2 <- data$th5$
    last_by(c("X", "Y"))$
    sort(c("X", "Y"))
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))

  data$client$close()
})

test_that("head_by behaves as expected", {
  data <- setup()

  new_tb1 <- data$df5 %>%
    group_by(X) %>%
    slice_head(n = 2)
  new_th1 <- data$th5$
    head_by(2, "X")
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))

  new_tb2 <- data$df5 %>%
    group_by(X, Y) %>%
    slice_head(n = 2)
  new_th2 <- data$th5$
    head_by(2, c("X", "Y"))$
    sort(c("X", "Y"))
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))

  data$client$close()
})

test_that("tail_by behaves as expected", {
  data <- setup()

  new_tb1 <- data$df5 %>%
    group_by(X) %>%
    slice_tail(n = 2)
  new_th1 <- data$th5$
    tail_by(2, "X")
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))

  new_tb2 <- data$df5 %>%
    group_by(X, Y) %>%
    slice_tail(n = 2)
  new_th2 <- data$th5$
    tail_by(2, c("X", "Y"))$
    sort(c("X", "Y"))
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))

  data$client$close()
})

test_that("min_by behaves as expected", {
  data <- setup()

  new_tb1 <- data$df1 %>%
    group_by(int_col) %>%
    summarise(across(everything(), min))
  new_th1 <- data$th1$
    min_by("int_col")
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))

  new_tb2 <- data$df2 %>%
    group_by(col2) %>%
    summarise(across(everything(), min))
  new_th2 <- data$th2$
    min_by("col2")
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))

  new_tb3 <- data$df3 %>%
    mutate(bool_col1 = X1 >= 0, bool_col2 = X2 >= 0) %>%
    group_by(bool_col1, bool_col2) %>%
    summarise(across(everything(), min)) %>%
    arrange(bool_col1, bool_col2) # need to sort because resulting row orders are not the same
  new_th3 <- data$th3$
    update(c("bool_col1 = X1 >= 0", "bool_col2 = X2 >= 0"))$
    min_by(c("bool_col1", "bool_col2"))$
    sort(c("bool_col1", "bool_col2")) # need to sort because resulting row orders are not the same
  expect_equal(as.data.frame(new_th3), as.data.frame(new_tb3))

  new_tb4 <- data$df4 %>%
    group_by(bool_col) %>%
    summarise(across(everything(), min)) %>%
    arrange(bool_col)
  new_th4 <- data$th4$
    min_by("bool_col")$
    sort("bool_col")
  expect_equal(as.data.frame(new_th4), as.data.frame(new_tb4))

  data$client$close()
})

test_that("max_by behaves as expected", {
  data <- setup()

  new_tb1 <- data$df1 %>%
    group_by(int_col) %>%
    summarise(across(everything(), max))
  new_th1 <- data$th1$
    max_by("int_col")
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))

  new_tb2 <- data$df2 %>%
    group_by(col2) %>%
    summarise(across(everything(), max))
  new_th2 <- data$th2$
    max_by("col2")
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))

  new_tb3 <- data$df3 %>%
    mutate(bool_col1 = X1 >= 0, bool_col2 = X2 >= 0) %>%
    group_by(bool_col1, bool_col2) %>%
    summarise(across(everything(), max)) %>%
    arrange(bool_col1, bool_col2) # need to sort because resulting row orders are not the same
  new_th3 <- data$th3$
    update(c("bool_col1 = X1 >= 0", "bool_col2 = X2 >= 0"))$
    max_by(c("bool_col1", "bool_col2"))$
    sort(c("bool_col1", "bool_col2")) # need to sort because resulting row orders are not the same
  expect_equal(as.data.frame(new_th3), as.data.frame(new_tb3))

  new_tb4 <- data$df4 %>%
    group_by(bool_col) %>%
    summarise(across(everything(), max)) %>%
    arrange(bool_col)
  new_th4 <- data$th4$
    max_by("bool_col")$
    sort("bool_col")
  expect_equal(as.data.frame(new_th4), as.data.frame(new_tb4))

  data$client$close()
})

test_that("sum_by behaves as expected", {
  data <- setup()

  new_tb1 <- data$df5 %>%
    select(-Y) %>%
    group_by(X) %>%
    summarise(across(everything(), sum))
  new_th1 <- data$th5$
    drop_columns("Y")$
    sum_by("X")
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))

  new_tb2 <- data$df5 %>%
    group_by(X, Y) %>%
    summarise(across(everything(), sum)) %>%
    arrange(X, Y)
  new_th2 <- data$th5$
    sum_by(c("X", "Y"))$
    sort(c("X", "Y"))
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))

  data$client$close()
})

test_that("abs_sum_by behaves as expected", {
  data <- setup()

  new_tb1 <- data$df5 %>%
    select(-Y) %>%
    mutate(Number1 = abs(Number1), Number2 = abs(Number2)) %>%
    group_by(X) %>%
    summarise(across(everything(), sum))
  new_th1 <- data$th5$
    drop_columns("Y")$
    abs_sum_by("X")
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))

  new_tb2 <- data$df5 %>%
    mutate(Number1 = abs(Number1), Number2 = abs(Number2)) %>%
    group_by(X, Y) %>%
    summarise(across(everything(), sum)) %>%
    arrange(X, Y)
  new_th2 <- data$th5$
    abs_sum_by(c("X", "Y"))$
    sort(c("X", "Y"))
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))

  data$client$close()
})

test_that("avg_by behaves as expected", {
  data <- setup()

  new_tb1 <- data$df5 %>%
    select(-Y) %>%
    group_by(X) %>%
    summarise(across(everything(), mean))
  new_th1 <- data$th5$
    drop_columns("Y")$
    avg_by("X")
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))

  new_tb2 <- data$df5 %>%
    group_by(X, Y) %>%
    summarise(across(everything(), mean)) %>%
    arrange(X, Y)
  new_th2 <- data$th5$
    avg_by(c("X", "Y"))$
    sort(c("X", "Y"))
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))

  data$client$close()
})

test_that("w_avg_by behaves as expected", {
  data <- setup()

  new_tb1 <- data$df5 %>%
    select(-Y) %>%
    mutate(weights = Number1 * Number2) %>%
    group_by(X) %>%
    summarise(
      Number1 = weighted.mean(Number1, weights),
      Number2 = weighted.mean(Number2, weights)
    )
  new_th1 <- data$th5$
    drop_columns("Y")$
    update("weights = Number1 * Number2")$
    w_avg_by("weights", "X")
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))

  new_tb2 <- data$df5 %>%
    mutate(weights = Number1 * Number2) %>%
    group_by(X, Y) %>%
    summarise(
      Number1 = weighted.mean(Number1, weights),
      Number2 = weighted.mean(Number2, weights)
    ) %>%
    arrange(X, Y)
  new_th2 <- data$th5$
    update("weights = Number1 * Number2")$
    w_avg_by("weights", c("X", "Y"))$
    sort(c("X", "Y"))
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))

  data$client$close()
})

test_that("median_by behaves as expected", {
  data <- setup()

  new_tb1 <- data$df5 %>%
    select(-Y) %>%
    group_by(X) %>%
    summarise(across(everything(), median))
  new_th1 <- data$th5$
    drop_columns("Y")$
    median_by("X")
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))

  new_tb2 <- data$df5 %>%
    group_by(X, Y) %>%
    summarise(across(everything(), median)) %>%
    arrange(X, Y)
  new_th2 <- data$th5$
    median_by(c("X", "Y"))$
    sort(c("X", "Y"))
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))

  data$client$close()
})

test_that("var_by behaves as expected", {
  data <- setup()

  new_tb1 <- data$df5 %>%
    select(-Y) %>%
    group_by(X) %>%
    summarise(across(everything(), var))
  new_th1 <- data$th5$
    drop_columns("Y")$
    var_by("X")
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))

  new_tb2 <- data$df5 %>%
    group_by(X, Y) %>%
    summarise(across(everything(), var)) %>%
    arrange(X, Y)
  new_th2 <- data$th5$
    var_by(c("X", "Y"))$
    sort(c("X", "Y"))
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))

  data$client$close()
})

test_that("std_by behaves as expected", {
  data <- setup()

  new_tb1 <- data$df5 %>%
    select(-Y) %>%
    group_by(X) %>%
    summarise(across(everything(), sd))
  new_th1 <- data$th5$
    drop_columns("Y")$
    std_by("X")
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))

  new_tb2 <- data$df5 %>%
    group_by(X, Y) %>%
    summarise(across(everything(), sd)) %>%
    arrange(X, Y)
  new_th2 <- data$th5$
    std_by(c("X", "Y"))$
    sort(c("X", "Y"))
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))

  data$client$close()
})

test_that("percentile_by behaves as expected", {
  data <- setup()

  # There is not a clean analog to `percentile_by` in dplyr,
  # so we construct these data frames directly.

  new_df1 <- data.frame(
    X = c("A", "B", "C"),
    Number1 = c(50, -44, -70),
    Number2 = c(-50, 76, 130)
  )
  new_th1 <- data$th5$
    drop_columns("Y")$
    percentile_by(0.4, "X")
  expect_equal(as.data.frame(new_th1), new_df1)

  new_df2 <- data.frame(
    X = c("A", "B", "A", "C", "B", "B", "C"),
    Y = c("M", "N", "O", "N", "P", "O", "M"),
    Number1 = c(50, -44, 49, 11, -66, 29, -70),
    Number2 = c(-55, 76, 20, 130, 137, 73, 214)
  )
  new_th2 <- data$th5$
    percentile_by(0.4, c("X", "Y"))
  expect_equal(as.data.frame(new_th2), new_df2)


  data$client$close()
})

test_that("count_by behaves as expected", {
  data <- setup()

  new_tb1 <- data$df5 %>%
    count(X)
  new_th1 <- data$th5$
    count_by("n", "X")
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))

  new_tb2 <- data$df5 %>%
    count(X, Y)
  new_th2 <- data$th5$
    count_by("n", c("X", "Y"))$
    sort(c("X", "Y"))
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))

  data$client$close()
})

test_that("sort behaves as expected", {
  data <- setup()

  new_tb1 <- data$df1 %>%
    arrange(dbl_col)
  new_th1 <- data$th1$
    sort("dbl_col")
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))

  new_tb2 <- data$df2 %>%
    arrange(desc(col3))
  new_th2 <- data$th2$
    sort("col3", descending = TRUE)
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))

  new_tb3 <- data$df3 %>%
    arrange(X1, X2, X3, X4, X5)
  new_th3 <- data$th3$
    sort(c("X1", "X2", "X3", "X4", "X5"))
  expect_equal(as.data.frame(new_th3), as.data.frame(new_tb3))

  new_tb4 <- data$df4 %>%
    arrange(desc(bool_col), desc(int_col))
  new_th4 <- data$th4$
    sort(c("bool_col", "int_col"), descending = TRUE)
  expect_equal(as.data.frame(new_th4), as.data.frame(new_tb4))

  new_tb5 <- data$df5 %>%
    arrange(X, desc(Y), Number1)
  new_th5 <- data$th5$
    sort(c("X", "Y", "Number1"), descending = c(FALSE, TRUE, FALSE))
  expect_equal(as.data.frame(new_th5), as.data.frame(new_tb5))

  data$client$close()
})

test_that("join behaves as expected", {
  data <- setup()

  new_th1 <- data$th5$
    join(data$th6,
      on = character(),
      joins = c("X_y = X", "Y_y = Y", "Number1_y = Number1", "Number2_y = Number2")
    )
  new_tb1 <- data$df5 %>%
    cross_join(data$df6) %>%
    rename(
      X = X.x, Y = Y.x, Number1 = Number1.x, Number2 = Number2.x,
      X_y = X.y, Y_y = Y.y, Number1_y = Number1.y, Number2_y = Number2.y
    )
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))

  data$client$close()
})

test_that("natural_join behaves as expected", {
  data <- setup()

  new_th2 <- data$th6$
    drop_columns("Y")$
    avg_by("X")
  new_th1 <- data$th5$
    natural_join(new_th2,
      on = "X",
      joins = c("Number3 = Number1", "Number4 = Number2")
    )

  new_tb2 <- data$df6 %>%
    select(-Y) %>%
    group_by(X) %>%
    summarise(across(everything(), mean))
  new_tb1 <- data$df5 %>%
    left_join(new_tb2, by = "X") %>%
    rename(
      Number1 = Number1.x, Number2 = Number2.x,
      Number3 = Number1.y, Number4 = Number2.y
    )
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))

  data$client$close()
})

test_that("exact_join behaves as expected", {
  data <- setup()

  new_th2 <- data$th6$
    drop_columns("Y")$
    avg_by("X")
  new_th1 <- data$th5$
    exact_join(new_th2,
      on = "X",
      joins = c("Number3 = Number1", "Number4 = Number2")
    )

  new_tb2 <- data$df6 %>%
    select(-Y) %>%
    group_by(X) %>%
    summarise(across(everything(), mean))
  new_tb1 <- data$df5 %>%
    left_join(new_tb2, by = "X") %>%
    rename(Number1 = Number1.x, Number2 = Number2.x, Number3 = Number1.y, Number4 = Number2.y)
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))

  data$client$close()
})
