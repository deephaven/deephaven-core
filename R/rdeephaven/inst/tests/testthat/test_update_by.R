library(testthat)
library(dplyr)
library(rdeephaven)
library(lubridate)
library(zoo)

# We suppress warnings because warnings are thrown when min() and max() are
# applied to empty sets, which happens in the pure-R versions of rolling_*_time()
options(warn = -1)

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

  df3 <- data.frame(
    time_col = seq.POSIXt(as.POSIXct(Sys.Date()), as.POSIXct(Sys.Date() + 30), by = "1 sec")[1:250000],
    bool_col = sample(c(TRUE, FALSE), 250000, TRUE),
    int_col = sample(0:10000, 250000, TRUE)
  )

  deterministic_df3 <- data.frame(
    time_col = seq.POSIXt(as.POSIXct(Sys.Date()), as.POSIXct(Sys.Date() + 30), by = "1 sec")[1:500],
    bool_col = rep(c(TRUE, TRUE, FALSE, FALSE, TRUE, FALSE, TRUE, FALSE, FALSE, TRUE), 50),
    int_col = 1:500
  )

  df4 <- data.frame(
    X = c("A", "B", "A", "C", "B", "A", "B", "B", "C"),
    Y = c("M", "N", "O", "N", "P", "M", "O", "P", "M"),
    Number1 = c(100, -44, 49, 11, -66, 50, 29, 18, -70),
    Number2 = c(-55, 76, 20, 130, 230, -50, 73, 137, 214)
  )

  df5 <- data.frame(
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
  deterministic_th3 <- client$import_table(deterministic_df3)
  th4 <- client$import_table(df4)
  th5 <- client$import_table(df5)

  # create variants with missing data to test NULL stuff
  null_df1 <- as.data.frame(lapply(df1, function(x) {
    replace(x, sample(length(x), .5 * length(x)), NA)
  }))
  null_df2 <- as.data.frame(lapply(df2, function(x) {
    replace(x, sample(length(x), .5 * length(x)), NA)
  }))
  null_df3 <- as.data.frame(lapply(df3, function(x) {
    replace(x, sample(length(x), .5 * length(x)), NA)
  }))
  null_df4 <- as.data.frame(lapply(df4, function(x) {
    replace(x, sample(length(x), .5 * length(x)), NA)
  }))
  null_df5 <- as.data.frame(lapply(df5, function(x) {
    replace(x, sample(length(x), .5 * length(x)), NA)
  }))

  null_th1 <- client$import_table(null_df1)
  null_th2 <- client$import_table(null_df2)
  null_th3 <- client$import_table(null_df3)
  null_th4 <- client$import_table(null_df4)
  null_th5 <- client$import_table(null_df5)

  return(list(
    "client" = client,
    "df1" = df1, "df2" = df2, "df3" = df3, "deterministic_df3" = deterministic_df3, "df4" = df4, "df5" = df5,
    "th1" = th1, "th2" = th2, "th3" = th3, "deterministic_th3" = deterministic_th3, "th4" = th4, "th5" = th5,
    "null_df1" = null_df1, "null_df2" = null_df2, "null_df3" = null_df3, "null_df4" = null_df4, "null_df5" = null_df5,
    "null_th1" = null_th1, "null_th2" = null_th2, "null_th3" = null_th3, "null_th4" = null_th4, "null_th5" = null_th5
  ))
}

# this is for verifying rolling time functions in pure R, assumes timestamps are in 1s intervals
custom_rolling_time_op <- function(col, group_col, ...) {
  true_col <- rollapply(replace(col, group_col == FALSE, NA), ...)
  false_col <- rollapply(replace(col, group_col == TRUE, NA), ...)
  return(ifelse(group_col, true_col, false_col))
}

test_that("uby_cum_sum behaves as expected", {
  data <- setup()

  new_tb1 <- data$df1 %>%
    mutate(sum_int_col = cumsum(int_col))
  new_th1 <- data$th1$
    update_by(uby_cum_sum("sum_int_col = int_col"))
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))

  new_tb2 <- data$df2 %>%
    mutate(sum_col1 = cumsum(col1), sum_col3 = cumsum(col3))
  new_th2 <- data$th2$
    update_by(uby_cum_sum(c("sum_col1 = col1", "sum_col3 = col3")))
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))

  new_tb3 <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(sum_int_col = cumsum(int_col))
  new_th3 <- data$th3$
    update_by(uby_cum_sum("sum_int_col = int_col"), by = "bool_col")
  expect_equal(as.data.frame(new_th3), as.data.frame(new_tb3))

  new_tb4 <- data$df4 %>%
    group_by(X) %>%
    mutate(sum_Number1 = cumsum(Number1), sum_Number2 = cumsum(Number2))
  new_th4 <- data$th4$
    update_by(uby_cum_sum(c("sum_Number1 = Number1", "sum_Number2 = Number2")), by = "X")
  expect_equal(as.data.frame(new_th4), as.data.frame(new_tb4))

  new_tb5 <- data$df5 %>%
    group_by(Y) %>%
    mutate(sum_Number1 = cumsum(Number1), sum_Number2 = cumsum(Number2))
  new_th5 <- data$th5$
    update_by(uby_cum_sum(c("sum_Number1 = Number1", "sum_Number2 = Number2")), by = "Y")
  expect_equal(as.data.frame(new_th5), as.data.frame(new_tb5))

  new_tb6 <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
    group_by(X, Y) %>%
    mutate(sum_Number1 = cumsum(Number1), sum_Number2 = cumsum(Number2))
  new_th6 <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(uby_cum_sum(c("sum_Number1 = Number1", "sum_Number2 = Number2")), by = c("X", "Y"))
  expect_equal(as.data.frame(new_th6), as.data.frame(new_tb6))

  data$client$close()
})

test_that("uby_cum_prod behaves as expected", {
  data <- setup()

  new_tb1 <- data$df1 %>%
    mutate(prod_int_col = cumprod(int_col))
  new_th1 <- data$th1$
    update_by(uby_cum_prod("prod_int_col = int_col"))
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))

  new_tb2 <- data$df2 %>%
    mutate(prod_col1 = cumprod(col1), prod_col3 = cumprod(col3))
  new_th2 <- data$th2$
    update_by(uby_cum_prod(c("prod_col1 = col1", "prod_col3 = col3")))
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))

  # Using df3 yields gigantic products, which leads to overflow on the server
  # due to the column being an int. Clients cannot cast to java BigInt type,
  # so once a table has an int type, we cannot change it from here. Thus, using
  # cum_prod on int columns from R should be done with an abundance of caution,
  # and probably not at all. Make it a double before pushing to the server.

  new_tb4 <- data$df4 %>%
    group_by(X) %>%
    mutate(prod_Number1 = cumprod(Number1), prod_Number2 = cumprod(Number2))
  new_th4 <- data$th4$
    update_by(uby_cum_prod(c("prod_Number1 = Number1", "prod_Number2 = Number2")), by = "X")
  expect_equal(as.data.frame(new_th4), as.data.frame(new_tb4))

  new_tb5 <- data$df5 %>%
    group_by(Y) %>%
    mutate(prod_Number1 = cumprod(Number1), prod_Number2 = cumprod(Number2))
  new_th5 <- data$th5$
    update_by(uby_cum_prod(c("prod_Number1 = Number1", "prod_Number2 = Number2")), by = "Y")
  expect_equal(as.data.frame(new_th5), as.data.frame(new_tb5))

  new_tb6 <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
    group_by(X, Y) %>%
    mutate(prod_Number1 = cumprod(Number1), prod_Number2 = cumprod(Number2))
  new_th6 <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(uby_cum_prod(c("prod_Number1 = Number1", "prod_Number2 = Number2")), by = c("X", "Y"))
  expect_equal(as.data.frame(new_th6), as.data.frame(new_tb6))

  data$client$close()
})

test_that("uby_cum_min behaves as expected", {
  data <- setup()

  new_tb1 <- data$df1 %>%
    mutate(min_int_col = cummin(int_col))
  new_th1 <- data$th1$
    update_by(uby_cum_min("min_int_col = int_col"))
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))

  new_tb2 <- data$df2 %>%
    mutate(min_col1 = cummin(col1), min_col3 = cummin(col3))
  new_th2 <- data$th2$
    update_by(uby_cum_min(c("min_col1 = col1", "min_col3 = col3")))
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))

  new_tb3 <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(min_int_col = cummin(int_col))
  new_th3 <- data$th3$
    update_by(uby_cum_min("min_int_col = int_col"), by = "bool_col")
  expect_equal(as.data.frame(new_th3), as.data.frame(new_tb3))

  new_tb4 <- data$df4 %>%
    group_by(X) %>%
    mutate(min_Number1 = cummin(Number1), min_Number2 = cummin(Number2))
  new_th4 <- data$th4$
    update_by(uby_cum_min(c("min_Number1 = Number1", "min_Number2 = Number2")), by = "X")
  expect_equal(as.data.frame(new_th4), as.data.frame(new_tb4))

  new_tb5 <- data$df5 %>%
    group_by(Y) %>%
    mutate(min_Number1 = cummin(Number1), min_Number2 = cummin(Number2))
  new_th5 <- data$th5$
    update_by(uby_cum_min(c("min_Number1 = Number1", "min_Number2 = Number2")), by = "Y")
  expect_equal(as.data.frame(new_th5), as.data.frame(new_tb5))

  new_tb6 <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
    group_by(X, Y) %>%
    mutate(min_Number1 = cummin(Number1), min_Number2 = cummin(Number2))
  new_th6 <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(uby_cum_min(c("min_Number1 = Number1", "min_Number2 = Number2")), by = c("X", "Y"))
  expect_equal(as.data.frame(new_th6), as.data.frame(new_tb6))

  data$client$close()
})

test_that("uby_cum_max behaves as expected", {
  data <- setup()

  new_tb1 <- data$df1 %>%
    mutate(max_int_col = cummax(int_col))
  new_th1 <- data$th1$
    update_by(uby_cum_max("max_int_col = int_col"))
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))

  new_tb2 <- data$df2 %>%
    mutate(max_col1 = cummax(col1), max_col3 = cummax(col3))
  new_th2 <- data$th2$
    update_by(uby_cum_max(c("max_col1 = col1", "max_col3 = col3")))
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))

  new_tb3 <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(max_int_col = cummax(int_col))
  new_th3 <- data$th3$
    update_by(uby_cum_max("max_int_col = int_col"), by = "bool_col")
  expect_equal(as.data.frame(new_th3), as.data.frame(new_tb3))

  new_tb4 <- data$df4 %>%
    group_by(X) %>%
    mutate(max_Number1 = cummax(Number1), max_Number2 = cummax(Number2))
  new_th4 <- data$th4$
    update_by(uby_cum_max(c("max_Number1 = Number1", "max_Number2 = Number2")), by = "X")
  expect_equal(as.data.frame(new_th4), as.data.frame(new_tb4))

  new_tb5 <- data$df5 %>%
    group_by(Y) %>%
    mutate(max_Number1 = cummax(Number1), max_Number2 = cummax(Number2))
  new_th5 <- data$th5$
    update_by(uby_cum_max(c("max_Number1 = Number1", "max_Number2 = Number2")), by = "Y")
  expect_equal(as.data.frame(new_th5), as.data.frame(new_tb5))

  new_tb6 <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
    group_by(X, Y) %>%
    mutate(max_Number1 = cummax(Number1), max_Number2 = cummax(Number2))
  new_th6 <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(uby_cum_max(c("max_Number1 = Number1", "max_Number2 = Number2")), by = c("X", "Y"))
  expect_equal(as.data.frame(new_th6), as.data.frame(new_tb6))

  data$client$close()
})

test_that("uby_forward_fill behaves as expected", {
  data <- setup()

  new_th1 <- data$null_th1$
    update_by(uby_forward_fill())
  expect_equal(as.data.frame(new_th1), na.locf(data$null_df1, na.rm = FALSE))

  new_th2 <- data$null_th2$
    update_by(uby_forward_fill())
  expect_equal(as.data.frame(new_th2), na.locf(data$null_df2, na.rm = FALSE))

  new_th3 <- data$null_th3$
    update_by(uby_forward_fill())
  expect_equal(as.data.frame(new_th3), na.locf(data$null_df3, na.rm = FALSE))

  new_th4 <- data$null_th4$
    update_by(uby_forward_fill())
  expect_equal(as.data.frame(new_th4), na.locf(data$null_df4, na.rm = FALSE))

  new_th5 <- data$null_th5$
    update_by(uby_forward_fill())
  expect_equal(as.data.frame(new_th5), na.locf(data$null_df5, na.rm = FALSE))

  data$client$close()
})

test_that("uby_delta behaves as expected", {
  data <- setup()

  new_tb1 <- data$df1 %>%
    mutate(delta_int_col = c(NaN, diff(int_col)))
  new_th1 <- data$th1$
    update_by(uby_delta("delta_int_col = int_col"))
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))

  new_tb2 <- data$df2 %>%
    mutate(delta_col1 = c(NaN, diff(col1)), delta_col3 = c(NaN, diff(col3)))
  new_th2 <- data$th2$
    update_by(uby_delta(c("delta_col1 = col1", "delta_col3 = col3")))
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))

  new_tb3 <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(delta_int_col = c(NaN, diff(int_col)))
  new_th3 <- data$th3$
    update_by(uby_delta("delta_int_col = int_col"), by = "bool_col")
  expect_equal(as.data.frame(new_th3), as.data.frame(new_tb3))

  new_tb4 <- data$df4 %>%
    group_by(X) %>%
    mutate(delta_Number1 = c(NaN, diff(Number1)), delta_Number2 = c(NaN, diff(Number2)))
  new_th4 <- data$th4$
    update_by(uby_delta(c("delta_Number1 = Number1", "delta_Number2 = Number2")), by = "X")
  expect_equal(as.data.frame(new_th4), as.data.frame(new_tb4))

  new_tb5 <- data$df5 %>%
    group_by(Y) %>%
    mutate(delta_Number1 = c(NaN, diff(Number1)), delta_Number2 = c(NaN, diff(Number2)))
  new_th5 <- data$th5$
    update_by(uby_delta(c("delta_Number1 = Number1", "delta_Number2 = Number2")), by = "Y")
  expect_equal(as.data.frame(new_th5), as.data.frame(new_tb5))

  new_tb6 <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
    group_by(X, Y) %>%
    mutate(delta_Number1 = c(NaN, diff(Number1)), delta_Number2 = c(NaN, diff(Number2)))
  new_th6 <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(uby_delta(c("delta_Number1 = Number1", "delta_Number2 = Number2")), by = c("X", "Y"))
  expect_equal(as.data.frame(new_th6), as.data.frame(new_tb6))

  data$client$close()
})

test_that("uby_ema_tick behaves as expected", {
  data <- setup()

  custom_ema <- function(decay_ticks, x) {
    if (length(x) == 1) {
      return(x)
    }
    a <- exp(-1 / decay_ticks)
    ema <- c(x[1])
    for (i in seq(2, length(x))) {
      ema[i] <- a * ema[i - 1] + (1 - a) * x[i]
    }
    return(ema)
  }

  new_tb1 <- data$df1 %>%
    mutate(dbl_col = custom_ema(2, dbl_col))
  new_th1 <- data$th1$
    update_by(uby_ema_tick(2, "dbl_col"))
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))

  new_tb2 <- data$df2 %>%
    mutate(col1 = custom_ema(5, col1), col3 = custom_ema(5, col3))
  new_th2 <- data$th2$
    update_by(uby_ema_tick(5, c("col1", "col3")))
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))

  new_tb3 <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(ema_int_col = custom_ema(9, int_col))
  new_th3 <- data$th3$
    update_by(uby_ema_tick(9, "ema_int_col = int_col"), by = "bool_col")
  expect_equal(as.data.frame(new_th3), as.data.frame(new_tb3))

  new_tb4 <- data$df4 %>%
    group_by(X) %>%
    mutate(ema_Number1 = custom_ema(3, Number1), ema_Number2 = custom_ema(3, Number2))
  new_th4 <- data$th4$
    update_by(uby_ema_tick(3, c("ema_Number1 = Number1", "ema_Number2 = Number2")), by = "X")
  expect_equal(as.data.frame(new_th4), as.data.frame(new_tb4))

  new_tb5 <- data$df5 %>%
    group_by(Y) %>%
    mutate(ema_Number1 = custom_ema(3, Number1), ema_Number2 = custom_ema(3, Number2))
  new_th5 <- data$th5$
    update_by(uby_ema_tick(3, c("ema_Number1 = Number1", "ema_Number2 = Number2")), by = "Y")
  expect_equal(as.data.frame(new_th5), as.data.frame(new_tb5))

  new_tb6 <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
    group_by(X, Y) %>%
    mutate(ema_Number1 = custom_ema(3, Number1), ema_Number2 = custom_ema(3, Number2))
  new_th6 <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(uby_ema_tick(3, c("ema_Number1 = Number1", "ema_Number2 = Number2")), by = c("X", "Y"))
  expect_equal(as.data.frame(new_th6), as.data.frame(new_tb6))

  data$client$close()
})

test_that("uby_ema_time behaves as expected", {
  data <- setup()

  custom_ema_time <- function(ts, decay_time, x) {
    if (length(x) == 1) {
      return(x)
    }
    time_diffs <- as.numeric(ts[2:length(ts)] - ts[1:length(ts) - 1])
    a <- exp(-time_diffs / as.numeric(duration(decay_time)))
    ema <- c(x[1])
    for (i in seq(2, length(x))) {
      ema[i] <- a[i - 1] * ema[i - 1] + (1 - a[i - 1]) * x[i]
    }
    return(ema)
  }

  new_tb1 <- data$df3 %>%
    mutate(ema_int_col = custom_ema_time(time_col, "PT3s", int_col))
  new_th1 <- data$th3$
    update_by(uby_ema_time("time_col", "PT3s", "ema_int_col = int_col"))
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))

  new_tb2 <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(ema_int_col = custom_ema_time(time_col, "PT3s", int_col))
  new_th2 <- data$th3$
    update_by(uby_ema_time("time_col", "PT3s", "ema_int_col = int_col"), by = "bool_col")
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))

  data$client$close()
})

test_that("uby_ems_tick behaves as expected", {
  data <- setup()

  custom_ems <- function(decay_ticks, x) {
    if (length(x) == 1) {
      return(x)
    }
    a <- exp(-1 / decay_ticks)
    ems <- c(x[1])
    for (i in seq(2, length(x))) {
      ems[i] <- a * ems[i - 1] + x[i]
    }
    return(ems)
  }

  new_tb1 <- data$df1 %>%
    mutate(dbl_col = custom_ems(2, dbl_col))
  new_th1 <- data$th1$
    update_by(uby_ems_tick(2, "dbl_col"))
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))

  new_tb2 <- data$df2 %>%
    mutate(col1 = custom_ems(5, col1), col3 = custom_ems(5, col3))
  new_th2 <- data$th2$
    update_by(uby_ems_tick(5, c("col1", "col3")))
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))

  new_tb3 <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(ems_int_col = custom_ems(9, int_col))
  new_th3 <- data$th3$
    update_by(uby_ems_tick(9, "ems_int_col = int_col"), by = "bool_col")
  expect_equal(as.data.frame(new_th3), as.data.frame(new_tb3))

  new_tb4 <- data$df4 %>%
    group_by(X) %>%
    mutate(ems_Number1 = custom_ems(3, Number1), ems_Number2 = custom_ems(3, Number2))
  new_th4 <- data$th4$
    update_by(uby_ems_tick(3, c("ems_Number1 = Number1", "ems_Number2 = Number2")), by = "X")
  expect_equal(as.data.frame(new_th4), as.data.frame(new_tb4))

  new_tb5 <- data$df5 %>%
    group_by(Y) %>%
    mutate(ems_Number1 = custom_ems(3, Number1), ems_Number2 = custom_ems(3, Number2))
  new_th5 <- data$th5$
    update_by(uby_ems_tick(3, c("ems_Number1 = Number1", "ems_Number2 = Number2")), by = "Y")
  expect_equal(as.data.frame(new_th5), as.data.frame(new_tb5))

  new_tb6 <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
    group_by(X, Y) %>%
    mutate(ems_Number1 = custom_ems(3, Number1), ems_Number2 = custom_ems(3, Number2))
  new_th6 <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(uby_ems_tick(3, c("ems_Number1 = Number1", "ems_Number2 = Number2")), by = c("X", "Y"))
  expect_equal(as.data.frame(new_th6), as.data.frame(new_tb6))

  data$client$close()
})

test_that("uby_ems_time behaves as expected", {
  data <- setup()

  custom_ems_time <- function(ts, decay_time, x) {
    if (length(x) == 1) {
      return(x)
    }
    time_diffs <- as.numeric(ts[2:length(ts)] - ts[1:length(ts) - 1])
    a <- exp(-time_diffs / as.numeric(duration(decay_time)))
    ems <- c(x[1])
    for (i in seq(2, length(x))) {
      ems[i] <- a[i - 1] * ems[i - 1] + x[i]
    }
    return(ems)
  }

  new_tb1 <- data$df3 %>%
    mutate(ems_int_col = custom_ems_time(time_col, "PT3s", int_col))
  new_th1 <- data$th3$
    update_by(uby_ems_time("time_col", "PT3s", "ems_int_col = int_col"))
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))

  new_tb2 <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(ems_int_col = custom_ems_time(time_col, "PT3s", int_col))
  new_th2 <- data$th3$
    update_by(uby_ems_time("time_col", "PT3s", "ems_int_col = int_col"), by = "bool_col")
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))

  data$client$close()
})

test_that("uby_emmin_tick behaves as expected", {
  data <- setup()

  custom_emmin <- function(decay_ticks, x) {
    if (length(x) == 1) {
      return(x)
    }
    a <- exp(-1 / decay_ticks)
    emmin <- c(x[1])
    for (i in seq(2, length(x))) {
      emmin[i] <- min(a * emmin[i - 1], x[i])
    }
    return(emmin)
  }

  new_tb1 <- data$df1 %>%
    mutate(dbl_col = custom_emmin(2, dbl_col))
  new_th1 <- data$th1$
    update_by(uby_emmin_tick(2, "dbl_col"))
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))

  new_tb2 <- data$df2 %>%
    mutate(col1 = custom_emmin(5, col1), col3 = custom_emmin(5, col3))
  new_th2 <- data$th2$
    update_by(uby_emmin_tick(5, c("col1", "col3")))
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))

  new_tb3 <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(emmin_int_col = custom_emmin(9, int_col))
  new_th3 <- data$th3$
    update_by(uby_emmin_tick(9, "emmin_int_col = int_col"), by = "bool_col")
  expect_equal(as.data.frame(new_th3), as.data.frame(new_tb3))

  new_tb4 <- data$df4 %>%
    group_by(X) %>%
    mutate(emmin_Number1 = custom_emmin(3, Number1), emmin_Number2 = custom_emmin(3, Number2))
  new_th4 <- data$th4$
    update_by(uby_emmin_tick(3, c("emmin_Number1 = Number1", "emmin_Number2 = Number2")), by = "X")
  expect_equal(as.data.frame(new_th4), as.data.frame(new_tb4))

  new_tb5 <- data$df5 %>%
    group_by(Y) %>%
    mutate(emmin_Number1 = custom_emmin(3, Number1), emmin_Number2 = custom_emmin(3, Number2))
  new_th5 <- data$th5$
    update_by(uby_emmin_tick(3, c("emmin_Number1 = Number1", "emmin_Number2 = Number2")), by = "Y")
  expect_equal(as.data.frame(new_th5), as.data.frame(new_tb5))

  new_tb6 <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
    group_by(X, Y) %>%
    mutate(emmin_Number1 = custom_emmin(3, Number1), emmin_Number2 = custom_emmin(3, Number2))
  new_th6 <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(uby_emmin_tick(3, c("emmin_Number1 = Number1", "emmin_Number2 = Number2")), by = c("X", "Y"))
  expect_equal(as.data.frame(new_th6), as.data.frame(new_tb6))

  data$client$close()
})

test_that("uby_emmin_time behaves as expected", {
  data <- setup()

  custom_emmin_time <- function(ts, decay_time, x) {
    if (length(x) == 1) {
      return(x)
    }
    time_diffs <- as.numeric(ts[2:length(ts)] - ts[1:length(ts) - 1])
    a <- exp(-time_diffs / as.numeric(duration(decay_time)))
    emmin <- c(x[1])
    for (i in seq(2, length(x))) {
      emmin[i] <- min(a[i - 1] * emmin[i - 1], x[i])
    }
    return(emmin)
  }

  new_tb1 <- data$df3 %>%
    mutate(emmin_int_col = custom_emmin_time(time_col, "PT3s", int_col))
  new_th1 <- data$th3$
    update_by(uby_emmin_time("time_col", "PT3s", "emmin_int_col = int_col"))
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))

  new_tb2 <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(emmin_int_col = custom_emmin_time(time_col, "PT3s", int_col))
  new_th2 <- data$th3$
    update_by(uby_emmin_time("time_col", "PT3s", "emmin_int_col = int_col"), by = "bool_col")
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))

  data$client$close()
})

test_that("uby_emmax_tick behaves as expected", {
  data <- setup()

  custom_emmax <- function(decay_ticks, x) {
    if (length(x) == 1) {
      return(x)
    }
    a <- exp(-1 / decay_ticks)
    emmax <- c(x[1])
    for (i in seq(2, length(x))) {
      emmax[i] <- max(a * emmax[i - 1], x[i])
    }
    return(emmax)
  }

  new_tb1 <- data$df1 %>%
    mutate(dbl_col = custom_emmax(2, dbl_col))
  new_th1 <- data$th1$
    update_by(uby_emmax_tick(2, "dbl_col"))
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))

  new_tb2 <- data$df2 %>%
    mutate(col1 = custom_emmax(5, col1), col3 = custom_emmax(5, col3))
  new_th2 <- data$th2$
    update_by(uby_emmax_tick(5, c("col1", "col3")))
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))

  new_tb3 <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(emmax_int_col = custom_emmax(9, int_col))
  new_th3 <- data$th3$
    update_by(uby_emmax_tick(9, "emmax_int_col = int_col"), by = "bool_col")
  expect_equal(as.data.frame(new_th3), as.data.frame(new_tb3))

  new_tb4 <- data$df4 %>%
    group_by(X) %>%
    mutate(emmax_Number1 = custom_emmax(3, Number1), emmax_Number2 = custom_emmax(3, Number2))
  new_th4 <- data$th4$
    update_by(uby_emmax_tick(3, c("emmax_Number1 = Number1", "emmax_Number2 = Number2")), by = "X")
  expect_equal(as.data.frame(new_th4), as.data.frame(new_tb4))

  new_tb5 <- data$df5 %>%
    group_by(Y) %>%
    mutate(emmax_Number1 = custom_emmax(3, Number1), emmax_Number2 = custom_emmax(3, Number2))
  new_th5 <- data$th5$
    update_by(uby_emmax_tick(3, c("emmax_Number1 = Number1", "emmax_Number2 = Number2")), by = "Y")
  expect_equal(as.data.frame(new_th5), as.data.frame(new_tb5))

  new_tb6 <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
    group_by(X, Y) %>%
    mutate(emmax_Number1 = custom_emmax(3, Number1), emmax_Number2 = custom_emmax(3, Number2))
  new_th6 <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(uby_emmax_tick(3, c("emmax_Number1 = Number1", "emmax_Number2 = Number2")), by = c("X", "Y"))
  expect_equal(as.data.frame(new_th6), as.data.frame(new_tb6))

  data$client$close()
})

test_that("uby_emmax_time behaves as expected", {
  data <- setup()

  custom_emmax_time <- function(ts, decay_time, x) {
    if (length(x) == 1) {
      return(x)
    }
    time_diffs <- as.numeric(ts[2:length(ts)] - ts[1:length(ts) - 1])
    a <- exp(-time_diffs / as.numeric(duration(decay_time)))
    emmax <- c(x[1])
    for (i in seq(2, length(x))) {
      emmax[i] <- max(a[i - 1] * emmax[i - 1], x[i])
    }
    return(emmax)
  }

  new_tb1 <- data$df3 %>%
    mutate(emmax_int_col = custom_emmax_time(time_col, "PT3s", int_col))
  new_th1 <- data$th3$
    update_by(uby_emmax_time("time_col", "PT3s", "emmax_int_col = int_col"))
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))

  new_tb2 <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(emmax_int_col = custom_emmax_time(time_col, "PT3s", int_col))
  new_th2 <- data$th3$
    update_by(uby_emmax_time("time_col", "PT3s", "emmax_int_col = int_col"), by = "bool_col")
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))

  data$client$close()
})

test_that("uby_emstd_tick behaves as expected", {
  data <- setup()

  custom_emstd <- function(decay_ticks, x) {
    if (length(x) == 1) {
      return(NA)
    }
    a <- exp(-1 / decay_ticks)
    current_ema <- x[1]
    emvar <- c(0)
    for (i in seq(2, length(x))) {
      emvar[i] <- a * (emvar[i - 1] + (1 - a) * ((x[i] - current_ema)^2))
      current_ema <- a * current_ema + (1 - a) * x[i]
    }
    emvar[1] <- NA
    return(sqrt(emvar))
  }

  new_tb1 <- data$df1 %>%
    mutate(dbl_col = custom_emstd(2, dbl_col))
  new_th1 <- data$th1$
    update_by(uby_emstd_tick(2, "dbl_col"))
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))

  new_tb2 <- data$df2 %>%
    mutate(col1 = custom_emstd(5, col1), col3 = custom_emstd(5, col3))
  new_th2 <- data$th2$
    update_by(uby_emstd_tick(5, c("col1", "col3")))
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))

  new_tb3 <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(emstd_int_col = custom_emstd(9, int_col))
  new_th3 <- data$th3$
    update_by(uby_emstd_tick(9, "emstd_int_col = int_col"), by = "bool_col")
  expect_equal(as.data.frame(new_th3), as.data.frame(new_tb3))

  new_tb4 <- data$df4 %>%
    group_by(X) %>%
    mutate(emstd_Number1 = custom_emstd(3, Number1), emstd_Number2 = custom_emstd(3, Number2))
  new_th4 <- data$th4$
    update_by(uby_emstd_tick(3, c("emstd_Number1 = Number1", "emstd_Number2 = Number2")), by = "X")
  expect_equal(as.data.frame(new_th4), as.data.frame(new_tb4))

  new_tb5 <- data$df5 %>%
    group_by(Y) %>%
    mutate(emstd_Number1 = custom_emstd(3, Number1), emstd_Number2 = custom_emstd(3, Number2))
  new_th5 <- data$th5$
    update_by(uby_emstd_tick(3, c("emstd_Number1 = Number1", "emstd_Number2 = Number2")), by = "Y")
  expect_equal(as.data.frame(new_th5), as.data.frame(new_tb5))

  new_tb6 <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
    group_by(X, Y) %>%
    mutate(emstd_Number1 = custom_emstd(3, Number1), emstd_Number2 = custom_emstd(3, Number2))
  new_th6 <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(uby_emstd_tick(3, c("emstd_Number1 = Number1", "emstd_Number2 = Number2")), by = c("X", "Y"))
  expect_equal(as.data.frame(new_th6), as.data.frame(new_tb6))

  data$client$close()
})

test_that("uby_emstd_time behaves as expected", {
  data <- setup()

  custom_emstd_time <- function(ts, decay_time, x) {
    if (length(x) == 1) {
      return(NA)
    }
    time_diffs <- as.numeric(ts[2:length(ts)] - ts[1:length(ts) - 1])
    a <- exp(-time_diffs / as.numeric(duration(decay_time)))
    current_ema <- x[1]
    emvar <- c(0)
    for (i in seq(2, length(x))) {
      emvar[i] <- a[i - 1] * (emvar[i - 1] + (1 - a[i - 1]) * ((x[i] - current_ema)^2))
      current_ema <- a[i - 1] * current_ema + (1 - a[i - 1]) * x[i]
    }
    emvar[1] <- NA
    return(sqrt(emvar))
  }

  new_tb1 <- data$df3 %>%
    mutate(emstd_int_col = custom_emstd_time(time_col, "PT3s", int_col))
  new_th1 <- data$th3$
    update_by(uby_emstd_time("time_col", "PT3s", "emstd_int_col = int_col"))
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))

  new_tb2 <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(emstd_int_col = custom_emstd_time(time_col, "PT3s", int_col))
  new_th2 <- data$th3$
    update_by(uby_emstd_time("time_col", "PT3s", "emstd_int_col = int_col"), by = "bool_col")
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))

  data$client$close()
})

test_that("uby_rolling_sum_tick behaves as expected", {
  data <- setup()

  new_tb1a <- data$df1 %>%
    mutate(dbl_col = rollapply(dbl_col, 3, sum, partial = TRUE, align = "right"))
  new_th1a <- data$th1$
    update_by(uby_rolling_sum_tick("dbl_col", rev_ticks = 3))
  expect_equal(as.data.frame(new_th1a), as.data.frame(new_tb1a))

  new_tb1b <- data$df1 %>%
    mutate(dbl_col = rollapply(dbl_col, 3, sum, partial = TRUE, align = "left"))
  new_th1b <- data$th1$
    update_by(uby_rolling_sum_tick("dbl_col", rev_ticks = 1, fwd_ticks = 2))
  expect_equal(as.data.frame(new_th1b), as.data.frame(new_tb1b))

  new_tb1c <- data$df1 %>%
    mutate(dbl_col = rollapply(dbl_col, 3, sum, partial = TRUE, align = "center"))
  new_th1c <- data$th1$
    update_by(uby_rolling_sum_tick("dbl_col", rev_ticks = 2, fwd_ticks = 1))
  expect_equal(as.data.frame(new_th1c), as.data.frame(new_tb1c))

  new_tb2a <- data$df2 %>%
    mutate(
      col1 = rollapply(col1, 5, sum, partial = TRUE, align = "right"),
      col3 = rollapply(col3, 5, sum, partial = TRUE, align = "right")
    )
  new_th2a <- data$th2$
    update_by(uby_rolling_sum_tick(c("col1", "col3"), rev_ticks = 5))
  expect_equal(as.data.frame(new_th2a), as.data.frame(new_tb2a))

  new_tb2b <- data$df2 %>%
    mutate(
      col1 = rollapply(col1, 5, sum, partial = TRUE, align = "left"),
      col3 = rollapply(col3, 5, sum, partial = TRUE, align = "left")
    )
  new_th2b <- data$th2$
    update_by(uby_rolling_sum_tick(c("col1", "col3"), rev_ticks = 1, fwd_ticks = 4))
  expect_equal(as.data.frame(new_th2b), as.data.frame(new_tb2b))

  new_tb2c <- data$df2 %>%
    mutate(
      col1 = rollapply(col1, 5, sum, partial = TRUE, align = "center"),
      col3 = rollapply(col3, 5, sum, partial = TRUE, align = "center")
    )
  new_th2c <- data$th2$
    update_by(uby_rolling_sum_tick(c("col1", "col3"), rev_ticks = 3, fwd_ticks = 2))
  expect_equal(as.data.frame(new_th2c), as.data.frame(new_tb2c))

  new_tb3a <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(int_col = rollapply(int_col, 9, sum, partial = TRUE, align = "right"))
  new_th3a <- data$th3$
    update_by(uby_rolling_sum_tick("int_col", rev_ticks = 9), by = "bool_col")
  expect_equal(as.data.frame(new_th3a), as.data.frame(new_tb3a))

  new_tb3b <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(int_col = rollapply(int_col, 9, sum, partial = TRUE, align = "left"))
  new_th3b <- data$th3$
    update_by(uby_rolling_sum_tick("int_col", rev_ticks = 1, fwd_ticks = 8), by = "bool_col")
  expect_equal(as.data.frame(new_th3b), as.data.frame(new_tb3b))

  new_tb3c <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(int_col = rollapply(int_col, 9, sum, partial = TRUE, align = "center"))
  new_th3c <- data$th3$
    update_by(uby_rolling_sum_tick("int_col", rev_ticks = 5, fwd_ticks = 4), by = "bool_col")
  expect_equal(as.data.frame(new_th3c), as.data.frame(new_tb3c))

  new_tb4a <- data$df4 %>%
    group_by(X) %>%
    mutate(
      Number1 = rollapply(Number1, 3, sum, partial = TRUE, align = "right"),
      Number2 = rollapply(Number2, 3, sum, partial = TRUE, align = "right")
    )
  new_th4a <- data$th4$
    update_by(uby_rolling_sum_tick(c("Number1", "Number2"), rev_ticks = 3), by = "X")
  expect_equal(as.data.frame(new_th4a), as.data.frame(new_tb4a))

  new_tb4b <- data$df4 %>%
    group_by(X) %>%
    mutate(
      Number1 = rollapply(Number1, 3, sum, partial = TRUE, align = "left"),
      Number2 = rollapply(Number2, 3, sum, partial = TRUE, align = "left")
    )
  new_th4b <- data$th4$
    update_by(uby_rolling_sum_tick(c("Number1", "Number2"), rev_ticks = 1, fwd_ticks = 2), by = "X")
  expect_equal(as.data.frame(new_th4b), as.data.frame(new_tb4b))

  new_tb4c <- data$df4 %>%
    group_by(X) %>%
    mutate(
      Number1 = rollapply(Number1, 3, sum, partial = TRUE, align = "center"),
      Number2 = rollapply(Number2, 3, sum, partial = TRUE, align = "center")
    )
  new_th4c <- data$th4$
    update_by(uby_rolling_sum_tick(c("Number1", "Number2"), rev_ticks = 2, fwd_ticks = 1), by = "X")
  expect_equal(as.data.frame(new_th4c), as.data.frame(new_tb4c))

  new_tb5a <- data$df5 %>%
    group_by(Y) %>%
    mutate(
      Number1 = rollapply(Number1, 3, sum, partial = TRUE, align = "right"),
      Number2 = rollapply(Number2, 3, sum, partial = TRUE, align = "right")
    )
  new_th5a <- data$th5$
    update_by(uby_rolling_sum_tick(c("Number1", "Number2"), rev_ticks = 3), by = "Y")
  expect_equal(as.data.frame(new_th5a), as.data.frame(new_tb5a))

  new_tb5b <- data$df5 %>%
    group_by(Y) %>%
    mutate(
      Number1 = rollapply(Number1, 3, sum, partial = TRUE, align = "left"),
      Number2 = rollapply(Number2, 3, sum, partial = TRUE, align = "left")
    )
  new_th5b <- data$th5$
    update_by(uby_rolling_sum_tick(c("Number1", "Number2"), rev_ticks = 1, fwd_ticks = 2), by = "Y")
  expect_equal(as.data.frame(new_th5b), as.data.frame(new_tb5b))

  new_tb5c <- data$df5 %>%
    group_by(Y) %>%
    mutate(
      Number1 = rollapply(Number1, 3, sum, partial = TRUE, align = "center"),
      Number2 = rollapply(Number2, 3, sum, partial = TRUE, align = "center")
    )
  new_th5c <- data$th5$
    update_by(uby_rolling_sum_tick(c("Number1", "Number2"), rev_ticks = 2, fwd_ticks = 1), by = "Y")
  expect_equal(as.data.frame(new_th5c), as.data.frame(new_tb5c))

  new_tb6a <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
    group_by(X, Y) %>%
    mutate(
      Number1 = rollapply(Number1, 3, sum, partial = TRUE, align = "right"),
      Number2 = rollapply(Number2, 3, sum, partial = TRUE, align = "right")
    )
  new_th6a <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(uby_rolling_sum_tick(c("Number1", "Number2"), rev_ticks = 3), by = c("X", "Y"))
  expect_equal(as.data.frame(new_th6a), as.data.frame(new_tb6a))

  new_tb6b <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
    group_by(X, Y) %>%
    mutate(
      Number1 = rollapply(Number1, 3, sum, partial = TRUE, align = "left"),
      Number2 = rollapply(Number2, 3, sum, partial = TRUE, align = "left")
    )
  new_th6b <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(uby_rolling_sum_tick(c("Number1", "Number2"), rev_ticks = 1, fwd_ticks = 2), by = c("X", "Y"))
  expect_equal(as.data.frame(new_th6b), as.data.frame(new_tb6b))

  new_tb6c <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
    group_by(X, Y) %>%
    mutate(
      Number1 = rollapply(Number1, 3, sum, partial = TRUE, align = "center"),
      Number2 = rollapply(Number2, 3, sum, partial = TRUE, align = "center")
    )
  new_th6c <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(uby_rolling_sum_tick(c("Number1", "Number2"), rev_ticks = 2, fwd_ticks = 1), by = c("X", "Y"))
  expect_equal(as.data.frame(new_th6c), as.data.frame(new_tb6c))

  data$client$close()
})

test_that("uby_rolling_sum_time behaves as expected", {
  data <- setup()

  new_tb1a <- head(data$df3, 500) %>%
    mutate(int_col = rollapply(int_col, 9, sum, partial = TRUE, align = "right"))
  new_th1a <- head(data$th3, 500)$
    update_by(uby_rolling_sum_time("time_col", "int_col", "PT8s"))
  expect_equal(as.data.frame(new_th1a), as.data.frame(new_tb1a))

  new_tb1b <- head(data$df3, 500) %>%
    mutate(int_col = rollapply(int_col, 9, sum, partial = TRUE, align = "left"))
  new_th1b <- head(data$th3, 500)$
    update_by(uby_rolling_sum_time("time_col", "int_col", "PT0s", "PT8s"))
  expect_equal(as.data.frame(new_th1b), as.data.frame(new_tb1b))

  new_tb1c <- head(data$df3, 500) %>%
    mutate(int_col = rollapply(int_col, 9, sum, partial = TRUE, align = "center"))
  new_th1c <- head(data$th3, 500)$
    update_by(uby_rolling_sum_time("time_col", "int_col", "PT4s", "PT4s"))
  expect_equal(as.data.frame(new_th1c), as.data.frame(new_tb1c))

  new_tb2a <- head(data$df3, 500) %>%
    mutate(int_col = custom_rolling_time_op(int_col, bool_col, width = 9, FUN = sum, partial = TRUE, align = "right", na.rm = TRUE))
  new_th2a <- head(data$th3, 500)$
    update_by(uby_rolling_sum_time("time_col", "int_col", "PT8s"), by = "bool_col")
  expect_equal(as.data.frame(new_th2a), as.data.frame(new_tb2a))

  new_tb2b <- head(data$df3, 500) %>%
    mutate(int_col = custom_rolling_time_op(int_col, bool_col, width = 9, FUN = sum, partial = TRUE, align = "left", na.rm = TRUE))
  new_th2b <- head(data$th3, 500)$
    update_by(uby_rolling_sum_time("time_col", "int_col", "PT0s", "PT8s"), by = "bool_col")
  expect_equal(as.data.frame(new_th2b), as.data.frame(new_tb2b))

  new_tb2c <- head(data$df3, 500) %>%
    mutate(int_col = custom_rolling_time_op(int_col, bool_col, width = 9, FUN = sum, partial = TRUE, align = "center", na.rm = TRUE))
  new_th2c <- head(data$th3, 500)$
    update_by(uby_rolling_sum_time("time_col", "int_col", "PT4s", "PT4s"), by = "bool_col")
  expect_equal(as.data.frame(new_th2b), as.data.frame(new_tb2b))

  data$client$close()
})

test_that("uby_rolling_group_tick behaves as expected", {
  data <- setup()

  right_group <- list(
    1.65,
    c(1.6500, 3.1234),
    c(1.6500, 3.1234, 100000.5000),
    c(3.1234, 100000.5000, 543.234567),
    c(100000.5000, 543.234567, 0.0000)
  )
  new_th1a <- data$th1$
    update_by(uby_rolling_group_tick("dbl_col_group = dbl_col", rev_ticks = 3))
  expect_equal(as.list(as.data.frame(new_th1a)$dbl_col_group), right_group)


  left_group <- list(
    c(1.6500, 3.1234, 100000.5000),
    c(3.1234, 100000.5000, 543.234567),
    c(100000.5000, 543.234567, 0.0000),
    c(543.234567, 0.0000),
    0
  )
  new_th1b <- data$th1$
    update_by(uby_rolling_group_tick("dbl_col_group = dbl_col", rev_ticks = 1, fwd_ticks = 2))
  expect_equal(as.list(as.data.frame(new_th1b)$dbl_col_group), left_group)


  center_group <- list(
    c(1.6500, 3.1234),
    c(1.6500, 3.1234, 100000.5000),
    c(3.1234, 100000.5000, 543.234567),
    c(100000.5000, 543.234567, 0.0000),
    c(543.234567, 0.0000)
  )
  new_th1c <- data$th1$
    update_by(uby_rolling_group_tick("dbl_col_group = dbl_col", rev_ticks = 2, fwd_ticks = 1))
  expect_equal(as.list(as.data.frame(new_th1c)$dbl_col_group), center_group)

  data$client$close()
})

test_that("uby_rolling_group_time behaves as expected", {
  data <- setup()

  right_group <- c(lapply(1:9, function(x) 1:x), lapply(2:492, function(x) c(x:(x + 8))))
  new_th1a <- data$deterministic_th3$
    update_by(uby_rolling_group_time("time_col", "int_col_group = int_col", "PT8s"))
  expect_equal(as.list(as.data.frame(new_th1a)$int_col_group), right_group)

  left_group <- c(lapply(1:491, function(x) c(x:(x + 8))), lapply(492:500, function(x) x:500))
  new_th1b <- data$deterministic_th3$
    update_by(uby_rolling_group_time("time_col", "int_col_group = int_col", "PT0s", "PT8s"))
  expect_equal(as.list(as.data.frame(new_th1b)$int_col_group), left_group)

  center_group <- c(lapply(5:9, function(x) 1:x), lapply(2:491, function(x) c(x:(x + 8))), lapply(492:496, function(x) x:500))
  new_th1c <- data$deterministic_th3$
    update_by(uby_rolling_group_time("time_col", "int_col_group = int_col", "PT4s", "PT4s"))
  expect_equal(as.list(as.data.frame(new_th1c)$int_col_group), center_group)

  data$client$close()
})

test_that("uby_rolling_avg_tick behaves as expected", {
  data <- setup()

  new_tb1a <- data$df1 %>%
    mutate(dbl_col = rollapply(dbl_col, 3, mean, partial = TRUE, align = "right"))
  new_th1a <- data$th1$
    update_by(uby_rolling_avg_tick("dbl_col", rev_ticks = 3))
  expect_equal(as.data.frame(new_th1a), as.data.frame(new_tb1a))

  new_tb1b <- data$df1 %>%
    mutate(dbl_col = rollapply(dbl_col, 3, mean, partial = TRUE, align = "left"))
  new_th1b <- data$th1$
    update_by(uby_rolling_avg_tick("dbl_col", rev_ticks = 1, fwd_ticks = 2))
  expect_equal(as.data.frame(new_th1b), as.data.frame(new_tb1b))

  new_tb1c <- data$df1 %>%
    mutate(dbl_col = rollapply(dbl_col, 3, mean, partial = TRUE, align = "center"))
  new_th1c <- data$th1$
    update_by(uby_rolling_avg_tick("dbl_col", rev_ticks = 2, fwd_ticks = 1))
  expect_equal(as.data.frame(new_th1c), as.data.frame(new_tb1c))

  new_tb2a <- data$df2 %>%
    mutate(
      col1 = rollapply(col1, 5, mean, partial = TRUE, align = "right"),
      col3 = rollapply(col3, 5, mean, partial = TRUE, align = "right")
    )
  new_th2a <- data$th2$
    update_by(uby_rolling_avg_tick(c("col1", "col3"), rev_ticks = 5))
  expect_equal(as.data.frame(new_th2a), as.data.frame(new_tb2a))

  new_tb2b <- data$df2 %>%
    mutate(
      col1 = rollapply(col1, 5, mean, partial = TRUE, align = "left"),
      col3 = rollapply(col3, 5, mean, partial = TRUE, align = "left")
    )
  new_th2b <- data$th2$
    update_by(uby_rolling_avg_tick(c("col1", "col3"), rev_ticks = 1, fwd_ticks = 4))
  expect_equal(as.data.frame(new_th2b), as.data.frame(new_tb2b))

  new_tb2c <- data$df2 %>%
    mutate(
      col1 = rollapply(col1, 5, mean, partial = TRUE, align = "center"),
      col3 = rollapply(col3, 5, mean, partial = TRUE, align = "center")
    )
  new_th2c <- data$th2$
    update_by(uby_rolling_avg_tick(c("col1", "col3"), rev_ticks = 3, fwd_ticks = 2))
  expect_equal(as.data.frame(new_th2c), as.data.frame(new_tb2c))

  new_tb3a <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(int_col = rollapply(int_col, 9, mean, partial = TRUE, align = "right"))
  new_th3a <- data$th3$
    update_by(uby_rolling_avg_tick("int_col", rev_ticks = 9), by = "bool_col")
  expect_equal(as.data.frame(new_th3a), as.data.frame(new_tb3a))

  new_tb3b <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(int_col = rollapply(int_col, 9, mean, partial = TRUE, align = "left"))
  new_th3b <- data$th3$
    update_by(uby_rolling_avg_tick("int_col", rev_ticks = 1, fwd_ticks = 8), by = "bool_col")
  expect_equal(as.data.frame(new_th3b), as.data.frame(new_tb3b))

  new_tb3c <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(int_col = rollapply(int_col, 9, mean, partial = TRUE, align = "center"))
  new_th3c <- data$th3$
    update_by(uby_rolling_avg_tick("int_col", rev_ticks = 5, fwd_ticks = 4), by = "bool_col")
  expect_equal(as.data.frame(new_th3c), as.data.frame(new_tb3c))

  new_tb4a <- data$df4 %>%
    group_by(X) %>%
    mutate(
      Number1 = rollapply(Number1, 3, mean, partial = TRUE, align = "right"),
      Number2 = rollapply(Number2, 3, mean, partial = TRUE, align = "right")
    )
  new_th4a <- data$th4$
    update_by(uby_rolling_avg_tick(c("Number1", "Number2"), rev_ticks = 3), by = "X")
  expect_equal(as.data.frame(new_th4a), as.data.frame(new_tb4a))

  new_tb4b <- data$df4 %>%
    group_by(X) %>%
    mutate(
      Number1 = rollapply(Number1, 3, mean, partial = TRUE, align = "left"),
      Number2 = rollapply(Number2, 3, mean, partial = TRUE, align = "left")
    )
  new_th4b <- data$th4$
    update_by(uby_rolling_avg_tick(c("Number1", "Number2"), rev_ticks = 1, fwd_ticks = 2), by = "X")
  expect_equal(as.data.frame(new_th4b), as.data.frame(new_tb4b))

  new_tb4c <- data$df4 %>%
    group_by(X) %>%
    mutate(
      Number1 = rollapply(Number1, 3, mean, partial = TRUE, align = "center"),
      Number2 = rollapply(Number2, 3, mean, partial = TRUE, align = "center")
    )
  new_th4c <- data$th4$
    update_by(uby_rolling_avg_tick(c("Number1", "Number2"), rev_ticks = 2, fwd_ticks = 1), by = "X")
  expect_equal(as.data.frame(new_th4c), as.data.frame(new_tb4c))

  new_tb5a <- data$df5 %>%
    group_by(Y) %>%
    mutate(
      Number1 = rollapply(Number1, 3, mean, partial = TRUE, align = "right"),
      Number2 = rollapply(Number2, 3, mean, partial = TRUE, align = "right")
    )
  new_th5a <- data$th5$
    update_by(uby_rolling_avg_tick(c("Number1", "Number2"), rev_ticks = 3), by = "Y")
  expect_equal(as.data.frame(new_th5a), as.data.frame(new_tb5a))

  new_tb5b <- data$df5 %>%
    group_by(Y) %>%
    mutate(
      Number1 = rollapply(Number1, 3, mean, partial = TRUE, align = "left"),
      Number2 = rollapply(Number2, 3, mean, partial = TRUE, align = "left")
    )
  new_th5b <- data$th5$
    update_by(uby_rolling_avg_tick(c("Number1", "Number2"), rev_ticks = 1, fwd_ticks = 2), by = "Y")
  expect_equal(as.data.frame(new_th5b), as.data.frame(new_tb5b))

  new_tb5c <- data$df5 %>%
    group_by(Y) %>%
    mutate(
      Number1 = rollapply(Number1, 3, mean, partial = TRUE, align = "center"),
      Number2 = rollapply(Number2, 3, mean, partial = TRUE, align = "center")
    )
  new_th5c <- data$th5$
    update_by(uby_rolling_avg_tick(c("Number1", "Number2"), rev_ticks = 2, fwd_ticks = 1), by = "Y")
  expect_equal(as.data.frame(new_th5c), as.data.frame(new_tb5c))

  new_tb6a <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
    group_by(X, Y) %>%
    mutate(
      Number1 = rollapply(Number1, 3, mean, partial = TRUE, align = "right"),
      Number2 = rollapply(Number2, 3, mean, partial = TRUE, align = "right")
    )
  new_th6a <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(uby_rolling_avg_tick(c("Number1", "Number2"), rev_ticks = 3), by = c("X", "Y"))
  expect_equal(as.data.frame(new_th6a), as.data.frame(new_tb6a))

  new_tb6b <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
    group_by(X, Y) %>%
    mutate(
      Number1 = rollapply(Number1, 3, mean, partial = TRUE, align = "left"),
      Number2 = rollapply(Number2, 3, mean, partial = TRUE, align = "left")
    )
  new_th6b <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(uby_rolling_avg_tick(c("Number1", "Number2"), rev_ticks = 1, fwd_ticks = 2), by = c("X", "Y"))
  expect_equal(as.data.frame(new_th6b), as.data.frame(new_tb6b))

  new_tb6c <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
    group_by(X, Y) %>%
    mutate(
      Number1 = rollapply(Number1, 3, mean, partial = TRUE, align = "center"),
      Number2 = rollapply(Number2, 3, mean, partial = TRUE, align = "center")
    )
  new_th6c <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(uby_rolling_avg_tick(c("Number1", "Number2"), rev_ticks = 2, fwd_ticks = 1), by = c("X", "Y"))
  expect_equal(as.data.frame(new_th6c), as.data.frame(new_tb6c))

  data$client$close()
})

test_that("uby_rolling_avg_time behaves as expected", {
  data <- setup()

  new_tb1a <- head(data$df3, 500) %>%
    mutate(int_col = rollapply(int_col, 9, mean, partial = TRUE, align = "right"))
  new_th1a <- head(data$th3, 500)$
    update_by(uby_rolling_avg_time("time_col", "int_col", "PT8s"))
  expect_equal(as.data.frame(new_th1a), as.data.frame(new_tb1a))

  new_tb1b <- head(data$df3, 500) %>%
    mutate(int_col = rollapply(int_col, 9, mean, partial = TRUE, align = "left"))
  new_th1b <- head(data$th3, 500)$
    update_by(uby_rolling_avg_time("time_col", "int_col", "PT0s", "PT8s"))
  expect_equal(as.data.frame(new_th1b), as.data.frame(new_tb1b))

  new_tb1c <- head(data$df3, 500) %>%
    mutate(int_col = rollapply(int_col, 9, mean, partial = TRUE, align = "center"))
  new_th1c <- head(data$th3, 500)$
    update_by(uby_rolling_avg_time("time_col", "int_col", "PT4s", "PT4s"))
  expect_equal(as.data.frame(new_th1c), as.data.frame(new_tb1c))

  new_tb2a <- head(data$df3, 500) %>%
    mutate(int_col = custom_rolling_time_op(int_col, bool_col, width = 9, FUN = mean, partial = TRUE, align = "right", na.rm = TRUE))
  new_th2a <- head(data$th3, 500)$
    update_by(uby_rolling_avg_time("time_col", "int_col", "PT8s"), by = "bool_col")
  expect_equal(as.data.frame(new_th2a), as.data.frame(new_tb2a))

  new_tb2b <- head(data$df3, 500) %>%
    mutate(int_col = custom_rolling_time_op(int_col, bool_col, width = 9, FUN = mean, partial = TRUE, align = "left", na.rm = TRUE))
  new_th2b <- head(data$th3, 500)$
    update_by(uby_rolling_avg_time("time_col", "int_col", "PT0s", "PT8s"), by = "bool_col")
  expect_equal(as.data.frame(new_th2b), as.data.frame(new_tb2b))

  new_tb2c <- head(data$df3, 500) %>%
    mutate(int_col = custom_rolling_time_op(int_col, bool_col, width = 9, FUN = mean, partial = TRUE, align = "center", na.rm = TRUE))
  new_th2c <- head(data$th3, 500)$
    update_by(uby_rolling_avg_time("time_col", "int_col", "PT4s", "PT4s"), by = "bool_col")
  expect_equal(as.data.frame(new_th2b), as.data.frame(new_tb2b))

  data$client$close()
})

test_that("uby_rolling_min_tick behaves as expected", {
  data <- setup()

  new_tb1a <- data$df1 %>%
    mutate(dbl_col = rollapply(dbl_col, 3, min, partial = TRUE, align = "right"))
  new_th1a <- data$th1$
    update_by(uby_rolling_min_tick("dbl_col", rev_ticks = 3))
  expect_equal(as.data.frame(new_th1a), as.data.frame(new_tb1a))

  new_tb1b <- data$df1 %>%
    mutate(dbl_col = rollapply(dbl_col, 3, min, partial = TRUE, align = "left"))
  new_th1b <- data$th1$
    update_by(uby_rolling_min_tick("dbl_col", rev_ticks = 1, fwd_ticks = 2))
  expect_equal(as.data.frame(new_th1b), as.data.frame(new_tb1b))

  new_tb1c <- data$df1 %>%
    mutate(dbl_col = rollapply(dbl_col, 3, min, partial = TRUE, align = "center"))
  new_th1c <- data$th1$
    update_by(uby_rolling_min_tick("dbl_col", rev_ticks = 2, fwd_ticks = 1))
  expect_equal(as.data.frame(new_th1c), as.data.frame(new_tb1c))

  new_tb2a <- data$df2 %>%
    mutate(
      col1 = rollapply(col1, 5, min, partial = TRUE, align = "right"),
      col3 = rollapply(col3, 5, min, partial = TRUE, align = "right")
    )
  new_th2a <- data$th2$
    update_by(uby_rolling_min_tick(c("col1", "col3"), rev_ticks = 5))
  expect_equal(as.data.frame(new_th2a), as.data.frame(new_tb2a))

  new_tb2b <- data$df2 %>%
    mutate(
      col1 = rollapply(col1, 5, min, partial = TRUE, align = "left"),
      col3 = rollapply(col3, 5, min, partial = TRUE, align = "left")
    )
  new_th2b <- data$th2$
    update_by(uby_rolling_min_tick(c("col1", "col3"), rev_ticks = 1, fwd_ticks = 4))
  expect_equal(as.data.frame(new_th2b), as.data.frame(new_tb2b))

  new_tb2c <- data$df2 %>%
    mutate(
      col1 = rollapply(col1, 5, min, partial = TRUE, align = "center"),
      col3 = rollapply(col3, 5, min, partial = TRUE, align = "center")
    )
  new_th2c <- data$th2$
    update_by(uby_rolling_min_tick(c("col1", "col3"), rev_ticks = 3, fwd_ticks = 2))
  expect_equal(as.data.frame(new_th2c), as.data.frame(new_tb2c))

  new_tb3a <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(int_col = rollapply(int_col, 9, min, partial = TRUE, align = "right"))
  new_th3a <- data$th3$
    update_by(uby_rolling_min_tick("int_col", rev_ticks = 9), by = "bool_col")
  expect_equal(as.data.frame(new_th3a), as.data.frame(new_tb3a))

  new_tb3b <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(int_col = rollapply(int_col, 9, min, partial = TRUE, align = "left"))
  new_th3b <- data$th3$
    update_by(uby_rolling_min_tick("int_col", rev_ticks = 1, fwd_ticks = 8), by = "bool_col")
  expect_equal(as.data.frame(new_th3b), as.data.frame(new_tb3b))

  new_tb3c <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(int_col = rollapply(int_col, 9, min, partial = TRUE, align = "center"))
  new_th3c <- data$th3$
    update_by(uby_rolling_min_tick("int_col", rev_ticks = 5, fwd_ticks = 4), by = "bool_col")
  expect_equal(as.data.frame(new_th3c), as.data.frame(new_tb3c))

  new_tb4a <- data$df4 %>%
    group_by(X) %>%
    mutate(
      Number1 = rollapply(Number1, 3, min, partial = TRUE, align = "right"),
      Number2 = rollapply(Number2, 3, min, partial = TRUE, align = "right")
    )
  new_th4a <- data$th4$
    update_by(uby_rolling_min_tick(c("Number1", "Number2"), rev_ticks = 3), by = "X")
  expect_equal(as.data.frame(new_th4a), as.data.frame(new_tb4a))

  new_tb4b <- data$df4 %>%
    group_by(X) %>%
    mutate(
      Number1 = rollapply(Number1, 3, min, partial = TRUE, align = "left"),
      Number2 = rollapply(Number2, 3, min, partial = TRUE, align = "left")
    )
  new_th4b <- data$th4$
    update_by(uby_rolling_min_tick(c("Number1", "Number2"), rev_ticks = 1, fwd_ticks = 2), by = "X")
  expect_equal(as.data.frame(new_th4b), as.data.frame(new_tb4b))

  new_tb4c <- data$df4 %>%
    group_by(X) %>%
    mutate(
      Number1 = rollapply(Number1, 3, min, partial = TRUE, align = "center"),
      Number2 = rollapply(Number2, 3, min, partial = TRUE, align = "center")
    )
  new_th4c <- data$th4$
    update_by(uby_rolling_min_tick(c("Number1", "Number2"), rev_ticks = 2, fwd_ticks = 1), by = "X")
  expect_equal(as.data.frame(new_th4c), as.data.frame(new_tb4c))

  new_tb5a <- data$df5 %>%
    group_by(Y) %>%
    mutate(
      Number1 = rollapply(Number1, 3, min, partial = TRUE, align = "right"),
      Number2 = rollapply(Number2, 3, min, partial = TRUE, align = "right")
    )
  new_th5a <- data$th5$
    update_by(uby_rolling_min_tick(c("Number1", "Number2"), rev_ticks = 3), by = "Y")
  expect_equal(as.data.frame(new_th5a), as.data.frame(new_tb5a))

  new_tb5b <- data$df5 %>%
    group_by(Y) %>%
    mutate(
      Number1 = rollapply(Number1, 3, min, partial = TRUE, align = "left"),
      Number2 = rollapply(Number2, 3, min, partial = TRUE, align = "left")
    )
  new_th5b <- data$th5$
    update_by(uby_rolling_min_tick(c("Number1", "Number2"), rev_ticks = 1, fwd_ticks = 2), by = "Y")
  expect_equal(as.data.frame(new_th5b), as.data.frame(new_tb5b))

  new_tb5c <- data$df5 %>%
    group_by(Y) %>%
    mutate(
      Number1 = rollapply(Number1, 3, min, partial = TRUE, align = "center"),
      Number2 = rollapply(Number2, 3, min, partial = TRUE, align = "center")
    )
  new_th5c <- data$th5$
    update_by(uby_rolling_min_tick(c("Number1", "Number2"), rev_ticks = 2, fwd_ticks = 1), by = "Y")
  expect_equal(as.data.frame(new_th5c), as.data.frame(new_tb5c))

  new_tb6a <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
    group_by(X, Y) %>%
    mutate(
      Number1 = rollapply(Number1, 3, min, partial = TRUE, align = "right"),
      Number2 = rollapply(Number2, 3, min, partial = TRUE, align = "right")
    )
  new_th6a <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(uby_rolling_min_tick(c("Number1", "Number2"), rev_ticks = 3), by = c("X", "Y"))
  expect_equal(as.data.frame(new_th6a), as.data.frame(new_tb6a))

  new_tb6b <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
    group_by(X, Y) %>%
    mutate(
      Number1 = rollapply(Number1, 3, min, partial = TRUE, align = "left"),
      Number2 = rollapply(Number2, 3, min, partial = TRUE, align = "left")
    )
  new_th6b <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(uby_rolling_min_tick(c("Number1", "Number2"), rev_ticks = 1, fwd_ticks = 2), by = c("X", "Y"))
  expect_equal(as.data.frame(new_th6b), as.data.frame(new_tb6b))

  new_tb6c <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
    group_by(X, Y) %>%
    mutate(
      Number1 = rollapply(Number1, 3, min, partial = TRUE, align = "center"),
      Number2 = rollapply(Number2, 3, min, partial = TRUE, align = "center")
    )
  new_th6c <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(uby_rolling_min_tick(c("Number1", "Number2"), rev_ticks = 2, fwd_ticks = 1), by = c("X", "Y"))
  expect_equal(as.data.frame(new_th6c), as.data.frame(new_tb6c))

  data$client$close()
})

test_that("uby_rolling_min_time behaves as expected", {
  data <- setup()

  new_tb1a <- head(data$df3, 500) %>%
    mutate(int_col = rollapply(int_col, 9, min, partial = TRUE, align = "right"))
  new_th1a <- head(data$th3, 500)$
    update_by(uby_rolling_min_time("time_col", "int_col", "PT8s"))
  expect_equal(as.data.frame(new_th1a), as.data.frame(new_tb1a))

  new_tb1b <- head(data$df3, 500) %>%
    mutate(int_col = rollapply(int_col, 9, min, partial = TRUE, align = "left"))
  new_th1b <- head(data$th3, 500)$
    update_by(uby_rolling_min_time("time_col", "int_col", "PT0s", "PT8s"))
  expect_equal(as.data.frame(new_th1b), as.data.frame(new_tb1b))

  new_tb1c <- head(data$df3, 500) %>%
    mutate(int_col = rollapply(int_col, 9, min, partial = TRUE, align = "center"))
  new_th1c <- head(data$th3, 500)$
    update_by(uby_rolling_min_time("time_col", "int_col", "PT4s", "PT4s"))
  expect_equal(as.data.frame(new_th1c), as.data.frame(new_tb1c))

  new_tb2a <- head(data$df3, 500) %>%
    mutate(int_col = custom_rolling_time_op(int_col, bool_col, width = 9, FUN = min, partial = TRUE, align = "right", na.rm = TRUE))
  new_th2a <- head(data$th3, 500)$
    update_by(uby_rolling_min_time("time_col", "int_col", "PT8s"), by = "bool_col")
  expect_equal(as.data.frame(new_th2a), as.data.frame(new_tb2a))

  new_tb2b <- head(data$df3, 500) %>%
    mutate(int_col = custom_rolling_time_op(int_col, bool_col, width = 9, FUN = min, partial = TRUE, align = "left", na.rm = TRUE))
  new_th2b <- head(data$th3, 500)$
    update_by(uby_rolling_min_time("time_col", "int_col", "PT0s", "PT8s"), by = "bool_col")
  expect_equal(as.data.frame(new_th2b), as.data.frame(new_tb2b))

  new_tb2c <- head(data$df3, 500) %>%
    mutate(int_col = custom_rolling_time_op(int_col, bool_col, width = 9, FUN = min, partial = TRUE, align = "center", na.rm = TRUE))
  new_th2c <- head(data$th3, 500)$
    update_by(uby_rolling_min_time("time_col", "int_col", "PT4s", "PT4s"), by = "bool_col")
  expect_equal(as.data.frame(new_th2b), as.data.frame(new_tb2b))

  data$client$close()
})

test_that("uby_rolling_max_tick behaves as expected", {
  data <- setup()

  new_tb1a <- data$df1 %>%
    mutate(dbl_col = rollapply(dbl_col, 3, max, partial = TRUE, align = "right"))
  new_th1a <- data$th1$
    update_by(uby_rolling_max_tick("dbl_col", rev_ticks = 3))
  expect_equal(as.data.frame(new_th1a), as.data.frame(new_tb1a))

  new_tb1b <- data$df1 %>%
    mutate(dbl_col = rollapply(dbl_col, 3, max, partial = TRUE, align = "left"))
  new_th1b <- data$th1$
    update_by(uby_rolling_max_tick("dbl_col", rev_ticks = 1, fwd_ticks = 2))
  expect_equal(as.data.frame(new_th1b), as.data.frame(new_tb1b))

  new_tb1c <- data$df1 %>%
    mutate(dbl_col = rollapply(dbl_col, 3, max, partial = TRUE, align = "center"))
  new_th1c <- data$th1$
    update_by(uby_rolling_max_tick("dbl_col", rev_ticks = 2, fwd_ticks = 1))
  expect_equal(as.data.frame(new_th1c), as.data.frame(new_tb1c))

  new_tb2a <- data$df2 %>%
    mutate(
      col1 = rollapply(col1, 5, max, partial = TRUE, align = "right"),
      col3 = rollapply(col3, 5, max, partial = TRUE, align = "right")
    )
  new_th2a <- data$th2$
    update_by(uby_rolling_max_tick(c("col1", "col3"), rev_ticks = 5))
  expect_equal(as.data.frame(new_th2a), as.data.frame(new_tb2a))

  new_tb2b <- data$df2 %>%
    mutate(
      col1 = rollapply(col1, 5, max, partial = TRUE, align = "left"),
      col3 = rollapply(col3, 5, max, partial = TRUE, align = "left")
    )
  new_th2b <- data$th2$
    update_by(uby_rolling_max_tick(c("col1", "col3"), rev_ticks = 1, fwd_ticks = 4))
  expect_equal(as.data.frame(new_th2b), as.data.frame(new_tb2b))

  new_tb2c <- data$df2 %>%
    mutate(
      col1 = rollapply(col1, 5, max, partial = TRUE, align = "center"),
      col3 = rollapply(col3, 5, max, partial = TRUE, align = "center")
    )
  new_th2c <- data$th2$
    update_by(uby_rolling_max_tick(c("col1", "col3"), rev_ticks = 3, fwd_ticks = 2))
  expect_equal(as.data.frame(new_th2c), as.data.frame(new_tb2c))

  new_tb3a <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(int_col = rollapply(int_col, 9, max, partial = TRUE, align = "right"))
  new_th3a <- data$th3$
    update_by(uby_rolling_max_tick("int_col", rev_ticks = 9), by = "bool_col")
  expect_equal(as.data.frame(new_th3a), as.data.frame(new_tb3a))

  new_tb3b <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(int_col = rollapply(int_col, 9, max, partial = TRUE, align = "left"))
  new_th3b <- data$th3$
    update_by(uby_rolling_max_tick("int_col", rev_ticks = 1, fwd_ticks = 8), by = "bool_col")
  expect_equal(as.data.frame(new_th3b), as.data.frame(new_tb3b))

  new_tb3c <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(int_col = rollapply(int_col, 9, max, partial = TRUE, align = "center"))
  new_th3c <- data$th3$
    update_by(uby_rolling_max_tick("int_col", rev_ticks = 5, fwd_ticks = 4), by = "bool_col")
  expect_equal(as.data.frame(new_th3c), as.data.frame(new_tb3c))

  new_tb4a <- data$df4 %>%
    group_by(X) %>%
    mutate(
      Number1 = rollapply(Number1, 3, max, partial = TRUE, align = "right"),
      Number2 = rollapply(Number2, 3, max, partial = TRUE, align = "right")
    )
  new_th4a <- data$th4$
    update_by(uby_rolling_max_tick(c("Number1", "Number2"), rev_ticks = 3), by = "X")
  expect_equal(as.data.frame(new_th4a), as.data.frame(new_tb4a))

  new_tb4b <- data$df4 %>%
    group_by(X) %>%
    mutate(
      Number1 = rollapply(Number1, 3, max, partial = TRUE, align = "left"),
      Number2 = rollapply(Number2, 3, max, partial = TRUE, align = "left")
    )
  new_th4b <- data$th4$
    update_by(uby_rolling_max_tick(c("Number1", "Number2"), rev_ticks = 1, fwd_ticks = 2), by = "X")
  expect_equal(as.data.frame(new_th4b), as.data.frame(new_tb4b))

  new_tb4c <- data$df4 %>%
    group_by(X) %>%
    mutate(
      Number1 = rollapply(Number1, 3, max, partial = TRUE, align = "center"),
      Number2 = rollapply(Number2, 3, max, partial = TRUE, align = "center")
    )
  new_th4c <- data$th4$
    update_by(uby_rolling_max_tick(c("Number1", "Number2"), rev_ticks = 2, fwd_ticks = 1), by = "X")
  expect_equal(as.data.frame(new_th4c), as.data.frame(new_tb4c))

  new_tb5a <- data$df5 %>%
    group_by(Y) %>%
    mutate(
      Number1 = rollapply(Number1, 3, max, partial = TRUE, align = "right"),
      Number2 = rollapply(Number2, 3, max, partial = TRUE, align = "right")
    )
  new_th5a <- data$th5$
    update_by(uby_rolling_max_tick(c("Number1", "Number2"), rev_ticks = 3), by = "Y")
  expect_equal(as.data.frame(new_th5a), as.data.frame(new_tb5a))

  new_tb5b <- data$df5 %>%
    group_by(Y) %>%
    mutate(
      Number1 = rollapply(Number1, 3, max, partial = TRUE, align = "left"),
      Number2 = rollapply(Number2, 3, max, partial = TRUE, align = "left")
    )
  new_th5b <- data$th5$
    update_by(uby_rolling_max_tick(c("Number1", "Number2"), rev_ticks = 1, fwd_ticks = 2), by = "Y")
  expect_equal(as.data.frame(new_th5b), as.data.frame(new_tb5b))

  new_tb5c <- data$df5 %>%
    group_by(Y) %>%
    mutate(
      Number1 = rollapply(Number1, 3, max, partial = TRUE, align = "center"),
      Number2 = rollapply(Number2, 3, max, partial = TRUE, align = "center")
    )
  new_th5c <- data$th5$
    update_by(uby_rolling_max_tick(c("Number1", "Number2"), rev_ticks = 2, fwd_ticks = 1), by = "Y")
  expect_equal(as.data.frame(new_th5c), as.data.frame(new_tb5c))

  new_tb6a <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
    group_by(X, Y) %>%
    mutate(
      Number1 = rollapply(Number1, 3, max, partial = TRUE, align = "right"),
      Number2 = rollapply(Number2, 3, max, partial = TRUE, align = "right")
    )
  new_th6a <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(uby_rolling_max_tick(c("Number1", "Number2"), rev_ticks = 3), by = c("X", "Y"))
  expect_equal(as.data.frame(new_th6a), as.data.frame(new_tb6a))

  new_tb6b <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
    group_by(X, Y) %>%
    mutate(
      Number1 = rollapply(Number1, 3, max, partial = TRUE, align = "left"),
      Number2 = rollapply(Number2, 3, max, partial = TRUE, align = "left")
    )
  new_th6b <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(uby_rolling_max_tick(c("Number1", "Number2"), rev_ticks = 1, fwd_ticks = 2), by = c("X", "Y"))
  expect_equal(as.data.frame(new_th6b), as.data.frame(new_tb6b))

  new_tb6c <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
    group_by(X, Y) %>%
    mutate(
      Number1 = rollapply(Number1, 3, max, partial = TRUE, align = "center"),
      Number2 = rollapply(Number2, 3, max, partial = TRUE, align = "center")
    )
  new_th6c <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(uby_rolling_max_tick(c("Number1", "Number2"), rev_ticks = 2, fwd_ticks = 1), by = c("X", "Y"))
  expect_equal(as.data.frame(new_th6c), as.data.frame(new_tb6c))

  data$client$close()
})

test_that("uby_rolling_max_time behaves as expected", {
  data <- setup()

  new_tb1a <- head(data$df3, 500) %>%
    mutate(int_col = rollapply(int_col, 9, max, partial = TRUE, align = "right"))
  new_th1a <- head(data$th3, 500)$
    update_by(uby_rolling_max_time("time_col", "int_col", "PT8s"))
  expect_equal(as.data.frame(new_th1a), as.data.frame(new_tb1a))

  new_tb1b <- head(data$df3, 500) %>%
    mutate(int_col = rollapply(int_col, 9, max, partial = TRUE, align = "left"))
  new_th1b <- head(data$th3, 500)$
    update_by(uby_rolling_max_time("time_col", "int_col", "PT0s", "PT8s"))
  expect_equal(as.data.frame(new_th1b), as.data.frame(new_tb1b))

  new_tb1c <- head(data$df3, 500) %>%
    mutate(int_col = rollapply(int_col, 9, max, partial = TRUE, align = "center"))
  new_th1c <- head(data$th3, 500)$
    update_by(uby_rolling_max_time("time_col", "int_col", "PT4s", "PT4s"))
  expect_equal(as.data.frame(new_th1c), as.data.frame(new_tb1c))

  new_tb2a <- head(data$df3, 500) %>%
    mutate(int_col = custom_rolling_time_op(int_col, bool_col, width = 9, FUN = max, partial = TRUE, align = "right", na.rm = TRUE))
  new_th2a <- head(data$th3, 500)$
    update_by(uby_rolling_max_time("time_col", "int_col", "PT8s"), by = "bool_col")
  expect_equal(as.data.frame(new_th2a), as.data.frame(new_tb2a))

  new_tb2b <- head(data$df3, 500) %>%
    mutate(int_col = custom_rolling_time_op(int_col, bool_col, width = 9, FUN = max, partial = TRUE, align = "left", na.rm = TRUE))
  new_th2b <- head(data$th3, 500)$
    update_by(uby_rolling_max_time("time_col", "int_col", "PT0s", "PT8s"), by = "bool_col")
  expect_equal(as.data.frame(new_th2b), as.data.frame(new_tb2b))

  new_tb2c <- head(data$df3, 500) %>%
    mutate(int_col = custom_rolling_time_op(int_col, bool_col, width = 9, FUN = max, partial = TRUE, align = "center", na.rm = TRUE))
  new_th2c <- head(data$th3, 500)$
    update_by(uby_rolling_max_time("time_col", "int_col", "PT4s", "PT4s"), by = "bool_col")
  expect_equal(as.data.frame(new_th2b), as.data.frame(new_tb2b))

  data$client$close()
})

test_that("uby_rolling_prod_tick behaves as expected", {
  data <- setup()

  new_tb1a <- data$df1 %>%
    mutate(dbl_col = rollapply(dbl_col, 3, prod, partial = TRUE, align = "right"))
  new_th1a <- data$th1$
    update_by(uby_rolling_prod_tick("dbl_col", rev_ticks = 3))
  expect_equal(as.data.frame(new_th1a), as.data.frame(new_tb1a))

  new_tb1b <- data$df1 %>%
    mutate(dbl_col = rollapply(dbl_col, 3, prod, partial = TRUE, align = "left"))
  new_th1b <- data$th1$
    update_by(uby_rolling_prod_tick("dbl_col", rev_ticks = 1, fwd_ticks = 2))
  expect_equal(as.data.frame(new_th1b), as.data.frame(new_tb1b))

  new_tb1c <- data$df1 %>%
    mutate(dbl_col = rollapply(dbl_col, 3, prod, partial = TRUE, align = "center"))
  new_th1c <- data$th1$
    update_by(uby_rolling_prod_tick("dbl_col", rev_ticks = 2, fwd_ticks = 1))
  expect_equal(as.data.frame(new_th1c), as.data.frame(new_tb1c))

  new_tb2a <- data$df2 %>%
    mutate(
      col1 = rollapply(col1, 5, prod, partial = TRUE, align = "right"),
      col3 = rollapply(col3, 5, prod, partial = TRUE, align = "right")
    )
  new_th2a <- data$th2$
    update_by(uby_rolling_prod_tick(c("col1", "col3"), rev_ticks = 5))
  expect_equal(as.data.frame(new_th2a), as.data.frame(new_tb2a))

  new_tb2b <- data$df2 %>%
    mutate(
      col1 = rollapply(col1, 5, prod, partial = TRUE, align = "left"),
      col3 = rollapply(col3, 5, prod, partial = TRUE, align = "left")
    )
  new_th2b <- data$th2$
    update_by(uby_rolling_prod_tick(c("col1", "col3"), rev_ticks = 1, fwd_ticks = 4))
  expect_equal(as.data.frame(new_th2b), as.data.frame(new_tb2b))

  new_tb2c <- data$df2 %>%
    mutate(
      col1 = rollapply(col1, 5, prod, partial = TRUE, align = "center"),
      col3 = rollapply(col3, 5, prod, partial = TRUE, align = "center")
    )
  new_th2c <- data$th2$
    update_by(uby_rolling_prod_tick(c("col1", "col3"), rev_ticks = 3, fwd_ticks = 2))
  expect_equal(as.data.frame(new_th2c), as.data.frame(new_tb2c))

  new_tb3a <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(int_col = rollapply(int_col, 9, prod, partial = TRUE, align = "right"))
  new_th3a <- data$th3$
    update_by(uby_rolling_prod_tick("int_col", rev_ticks = 9), by = "bool_col")
  expect_equal(as.data.frame(new_th3a), as.data.frame(new_tb3a))

  new_tb3b <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(int_col = rollapply(int_col, 9, prod, partial = TRUE, align = "left"))
  new_th3b <- data$th3$
    update_by(uby_rolling_prod_tick("int_col", rev_ticks = 1, fwd_ticks = 8), by = "bool_col")
  expect_equal(as.data.frame(new_th3b), as.data.frame(new_tb3b))

  new_tb3c <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(int_col = rollapply(int_col, 9, prod, partial = TRUE, align = "center"))
  new_th3c <- data$th3$
    update_by(uby_rolling_prod_tick("int_col", rev_ticks = 5, fwd_ticks = 4), by = "bool_col")
  expect_equal(as.data.frame(new_th3c), as.data.frame(new_tb3c))

  new_tb4a <- data$df4 %>%
    group_by(X) %>%
    mutate(
      Number1 = rollapply(Number1, 3, prod, partial = TRUE, align = "right"),
      Number2 = rollapply(Number2, 3, prod, partial = TRUE, align = "right")
    )
  new_th4a <- data$th4$
    update_by(uby_rolling_prod_tick(c("Number1", "Number2"), rev_ticks = 3), by = "X")
  expect_equal(as.data.frame(new_th4a), as.data.frame(new_tb4a))

  new_tb4b <- data$df4 %>%
    group_by(X) %>%
    mutate(
      Number1 = rollapply(Number1, 3, prod, partial = TRUE, align = "left"),
      Number2 = rollapply(Number2, 3, prod, partial = TRUE, align = "left")
    )
  new_th4b <- data$th4$
    update_by(uby_rolling_prod_tick(c("Number1", "Number2"), rev_ticks = 1, fwd_ticks = 2), by = "X")
  expect_equal(as.data.frame(new_th4b), as.data.frame(new_tb4b))

  new_tb4c <- data$df4 %>%
    group_by(X) %>%
    mutate(
      Number1 = rollapply(Number1, 3, prod, partial = TRUE, align = "center"),
      Number2 = rollapply(Number2, 3, prod, partial = TRUE, align = "center")
    )
  new_th4c <- data$th4$
    update_by(uby_rolling_prod_tick(c("Number1", "Number2"), rev_ticks = 2, fwd_ticks = 1), by = "X")
  expect_equal(as.data.frame(new_th4c), as.data.frame(new_tb4c))

  new_tb5a <- data$df5 %>%
    group_by(Y) %>%
    mutate(
      Number1 = rollapply(Number1, 3, prod, partial = TRUE, align = "right"),
      Number2 = rollapply(Number2, 3, prod, partial = TRUE, align = "right")
    )
  new_th5a <- data$th5$
    update_by(uby_rolling_prod_tick(c("Number1", "Number2"), rev_ticks = 3), by = "Y")
  expect_equal(as.data.frame(new_th5a), as.data.frame(new_tb5a))

  new_tb5b <- data$df5 %>%
    group_by(Y) %>%
    mutate(
      Number1 = rollapply(Number1, 3, prod, partial = TRUE, align = "left"),
      Number2 = rollapply(Number2, 3, prod, partial = TRUE, align = "left")
    )
  new_th5b <- data$th5$
    update_by(uby_rolling_prod_tick(c("Number1", "Number2"), rev_ticks = 1, fwd_ticks = 2), by = "Y")
  expect_equal(as.data.frame(new_th5b), as.data.frame(new_tb5b))

  new_tb5c <- data$df5 %>%
    group_by(Y) %>%
    mutate(
      Number1 = rollapply(Number1, 3, prod, partial = TRUE, align = "center"),
      Number2 = rollapply(Number2, 3, prod, partial = TRUE, align = "center")
    )
  new_th5c <- data$th5$
    update_by(uby_rolling_prod_tick(c("Number1", "Number2"), rev_ticks = 2, fwd_ticks = 1), by = "Y")
  expect_equal(as.data.frame(new_th5c), as.data.frame(new_tb5c))

  new_tb6a <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
    group_by(X, Y) %>%
    mutate(
      Number1 = rollapply(Number1, 3, prod, partial = TRUE, align = "right"),
      Number2 = rollapply(Number2, 3, prod, partial = TRUE, align = "right")
    )
  new_th6a <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(uby_rolling_prod_tick(c("Number1", "Number2"), rev_ticks = 3), by = c("X", "Y"))
  expect_equal(as.data.frame(new_th6a), as.data.frame(new_tb6a))

  new_tb6b <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
    group_by(X, Y) %>%
    mutate(
      Number1 = rollapply(Number1, 3, prod, partial = TRUE, align = "left"),
      Number2 = rollapply(Number2, 3, prod, partial = TRUE, align = "left")
    )
  new_th6b <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(uby_rolling_prod_tick(c("Number1", "Number2"), rev_ticks = 1, fwd_ticks = 2), by = c("X", "Y"))
  expect_equal(as.data.frame(new_th6b), as.data.frame(new_tb6b))

  new_tb6c <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
    group_by(X, Y) %>%
    mutate(
      Number1 = rollapply(Number1, 3, prod, partial = TRUE, align = "center"),
      Number2 = rollapply(Number2, 3, prod, partial = TRUE, align = "center")
    )
  new_th6c <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(uby_rolling_prod_tick(c("Number1", "Number2"), rev_ticks = 2, fwd_ticks = 1), by = c("X", "Y"))
  expect_equal(as.data.frame(new_th6c), as.data.frame(new_tb6c))

  data$client$close()
})

test_that("uby_rolling_prod_time behaves as expected", {
  data <- setup()

  new_tb1a <- head(data$df3, 500) %>%
    mutate(int_col = rollapply(int_col, 9, prod, partial = TRUE, align = "right"))
  new_th1a <- head(data$th3, 500)$
    update_by(uby_rolling_prod_time("time_col", "int_col", "PT8s"))
  expect_equal(as.data.frame(new_th1a), as.data.frame(new_tb1a))

  new_tb1b <- head(data$df3, 500) %>%
    mutate(int_col = rollapply(int_col, 9, prod, partial = TRUE, align = "left"))
  new_th1b <- head(data$th3, 500)$
    update_by(uby_rolling_prod_time("time_col", "int_col", "PT0s", "PT8s"))
  expect_equal(as.data.frame(new_th1b), as.data.frame(new_tb1b))

  new_tb1c <- head(data$df3, 500) %>%
    mutate(int_col = rollapply(int_col, 9, prod, partial = TRUE, align = "center"))
  new_th1c <- head(data$th3, 500)$
    update_by(uby_rolling_prod_time("time_col", "int_col", "PT4s", "PT4s"))
  expect_equal(as.data.frame(new_th1c), as.data.frame(new_tb1c))

  new_tb2a <- head(data$df3, 500) %>%
    mutate(int_col = custom_rolling_time_op(int_col, bool_col, width = 9, FUN = prod, partial = TRUE, align = "right", na.rm = TRUE))
  new_th2a <- head(data$th3, 500)$
    update_by(uby_rolling_prod_time("time_col", "int_col", "PT8s"), by = "bool_col")
  expect_equal(as.data.frame(new_th2a), as.data.frame(new_tb2a))

  new_tb2b <- head(data$df3, 500) %>%
    mutate(int_col = custom_rolling_time_op(int_col, bool_col, width = 9, FUN = prod, partial = TRUE, align = "left", na.rm = TRUE))
  new_th2b <- head(data$th3, 500)$
    update_by(uby_rolling_prod_time("time_col", "int_col", "PT0s", "PT8s"), by = "bool_col")
  expect_equal(as.data.frame(new_th2b), as.data.frame(new_tb2b))

  new_tb2c <- head(data$df3, 500) %>%
    mutate(int_col = custom_rolling_time_op(int_col, bool_col, width = 9, FUN = prod, partial = TRUE, align = "center", na.rm = TRUE))
  new_th2c <- head(data$th3, 500)$
    update_by(uby_rolling_prod_time("time_col", "int_col", "PT4s", "PT4s"), by = "bool_col")
  expect_equal(as.data.frame(new_th2b), as.data.frame(new_tb2b))

  data$client$close()
})

test_that("uby_rolling_count_tick behaves as expected", {
  data <- setup()

  new_tb1a <- data$df1 %>%
    mutate(dbl_col = rollapply(dbl_col, 3, length, partial = TRUE, align = "right"))
  new_th1a <- data$th1$
    update_by(uby_rolling_count_tick("dbl_col", rev_ticks = 3))
  expect_equal(as.data.frame(new_th1a), as.data.frame(new_tb1a))

  new_tb1b <- data$df1 %>%
    mutate(dbl_col = rollapply(dbl_col, 3, length, partial = TRUE, align = "left"))
  new_th1b <- data$th1$
    update_by(uby_rolling_count_tick("dbl_col", rev_ticks = 1, fwd_ticks = 2))
  expect_equal(as.data.frame(new_th1b), as.data.frame(new_tb1b))

  new_tb1c <- data$df1 %>%
    mutate(dbl_col = rollapply(dbl_col, 3, length, partial = TRUE, align = "center"))
  new_th1c <- data$th1$
    update_by(uby_rolling_count_tick("dbl_col", rev_ticks = 2, fwd_ticks = 1))
  expect_equal(as.data.frame(new_th1c), as.data.frame(new_tb1c))

  new_tb2a <- data$df2 %>%
    mutate(
      col1 = rollapply(col1, 5, length, partial = TRUE, align = "right"),
      col3 = rollapply(col3, 5, length, partial = TRUE, align = "right")
    )
  new_th2a <- data$th2$
    update_by(uby_rolling_count_tick(c("col1", "col3"), rev_ticks = 5))
  expect_equal(as.data.frame(new_th2a), as.data.frame(new_tb2a))

  new_tb2b <- data$df2 %>%
    mutate(
      col1 = rollapply(col1, 5, length, partial = TRUE, align = "left"),
      col3 = rollapply(col3, 5, length, partial = TRUE, align = "left")
    )
  new_th2b <- data$th2$
    update_by(uby_rolling_count_tick(c("col1", "col3"), rev_ticks = 1, fwd_ticks = 4))
  expect_equal(as.data.frame(new_th2b), as.data.frame(new_tb2b))

  new_tb2c <- data$df2 %>%
    mutate(
      col1 = rollapply(col1, 5, length, partial = TRUE, align = "center"),
      col3 = rollapply(col3, 5, length, partial = TRUE, align = "center")
    )
  new_th2c <- data$th2$
    update_by(uby_rolling_count_tick(c("col1", "col3"), rev_ticks = 3, fwd_ticks = 2))
  expect_equal(as.data.frame(new_th2c), as.data.frame(new_tb2c))

  new_tb3a <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(int_col = rollapply(int_col, 9, length, partial = TRUE, align = "right"))
  new_th3a <- data$th3$
    update_by(uby_rolling_count_tick("int_col", rev_ticks = 9), by = "bool_col")
  expect_equal(as.data.frame(new_th3a), as.data.frame(new_tb3a))

  new_tb3b <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(int_col = rollapply(int_col, 9, length, partial = TRUE, align = "left"))
  new_th3b <- data$th3$
    update_by(uby_rolling_count_tick("int_col", rev_ticks = 1, fwd_ticks = 8), by = "bool_col")
  expect_equal(as.data.frame(new_th3b), as.data.frame(new_tb3b))

  new_tb3c <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(int_col = rollapply(int_col, 9, length, partial = TRUE, align = "center"))
  new_th3c <- data$th3$
    update_by(uby_rolling_count_tick("int_col", rev_ticks = 5, fwd_ticks = 4), by = "bool_col")
  expect_equal(as.data.frame(new_th3c), as.data.frame(new_tb3c))

  new_tb4a <- data$df4 %>%
    group_by(X) %>%
    mutate(
      Number1 = rollapply(Number1, 3, length, partial = TRUE, align = "right"),
      Number2 = rollapply(Number2, 3, length, partial = TRUE, align = "right")
    )
  new_th4a <- data$th4$
    update_by(uby_rolling_count_tick(c("Number1", "Number2"), rev_ticks = 3), by = "X")
  expect_equal(as.data.frame(new_th4a), as.data.frame(new_tb4a))

  new_tb4b <- data$df4 %>%
    group_by(X) %>%
    mutate(
      Number1 = rollapply(Number1, 3, length, partial = TRUE, align = "left"),
      Number2 = rollapply(Number2, 3, length, partial = TRUE, align = "left")
    )
  new_th4b <- data$th4$
    update_by(uby_rolling_count_tick(c("Number1", "Number2"), rev_ticks = 1, fwd_ticks = 2), by = "X")
  expect_equal(as.data.frame(new_th4b), as.data.frame(new_tb4b))

  new_tb4c <- data$df4 %>%
    group_by(X) %>%
    mutate(
      Number1 = rollapply(Number1, 3, length, partial = TRUE, align = "center"),
      Number2 = rollapply(Number2, 3, length, partial = TRUE, align = "center")
    )
  new_th4c <- data$th4$
    update_by(uby_rolling_count_tick(c("Number1", "Number2"), rev_ticks = 2, fwd_ticks = 1), by = "X")
  expect_equal(as.data.frame(new_th4c), as.data.frame(new_tb4c))

  new_tb5a <- data$df5 %>%
    group_by(Y) %>%
    mutate(
      Number1 = rollapply(Number1, 3, length, partial = TRUE, align = "right"),
      Number2 = rollapply(Number2, 3, length, partial = TRUE, align = "right")
    )
  new_th5a <- data$th5$
    update_by(uby_rolling_count_tick(c("Number1", "Number2"), rev_ticks = 3), by = "Y")
  expect_equal(as.data.frame(new_th5a), as.data.frame(new_tb5a))

  new_tb5b <- data$df5 %>%
    group_by(Y) %>%
    mutate(
      Number1 = rollapply(Number1, 3, length, partial = TRUE, align = "left"),
      Number2 = rollapply(Number2, 3, length, partial = TRUE, align = "left")
    )
  new_th5b <- data$th5$
    update_by(uby_rolling_count_tick(c("Number1", "Number2"), rev_ticks = 1, fwd_ticks = 2), by = "Y")
  expect_equal(as.data.frame(new_th5b), as.data.frame(new_tb5b))

  new_tb5c <- data$df5 %>%
    group_by(Y) %>%
    mutate(
      Number1 = rollapply(Number1, 3, length, partial = TRUE, align = "center"),
      Number2 = rollapply(Number2, 3, length, partial = TRUE, align = "center")
    )
  new_th5c <- data$th5$
    update_by(uby_rolling_count_tick(c("Number1", "Number2"), rev_ticks = 2, fwd_ticks = 1), by = "Y")
  expect_equal(as.data.frame(new_th5c), as.data.frame(new_tb5c))

  new_tb6a <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
    group_by(X, Y) %>%
    mutate(
      Number1 = rollapply(Number1, 3, length, partial = TRUE, align = "right"),
      Number2 = rollapply(Number2, 3, length, partial = TRUE, align = "right")
    )
  new_th6a <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(uby_rolling_count_tick(c("Number1", "Number2"), rev_ticks = 3), by = c("X", "Y"))
  expect_equal(as.data.frame(new_th6a), as.data.frame(new_tb6a))

  new_tb6b <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
    group_by(X, Y) %>%
    mutate(
      Number1 = rollapply(Number1, 3, length, partial = TRUE, align = "left"),
      Number2 = rollapply(Number2, 3, length, partial = TRUE, align = "left")
    )
  new_th6b <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(uby_rolling_count_tick(c("Number1", "Number2"), rev_ticks = 1, fwd_ticks = 2), by = c("X", "Y"))
  expect_equal(as.data.frame(new_th6b), as.data.frame(new_tb6b))

  new_tb6c <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
    group_by(X, Y) %>%
    mutate(
      Number1 = rollapply(Number1, 3, length, partial = TRUE, align = "center"),
      Number2 = rollapply(Number2, 3, length, partial = TRUE, align = "center")
    )
  new_th6c <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(uby_rolling_count_tick(c("Number1", "Number2"), rev_ticks = 2, fwd_ticks = 1), by = c("X", "Y"))
  expect_equal(as.data.frame(new_th6c), as.data.frame(new_tb6c))

  data$client$close()
})

test_that("uby_rolling_count_time behaves as expected", {
  data <- setup()

  custom_count <- function(x) {
    return(sum(!is.na(x)))
  }

  new_tb1a <- head(data$df3, 500) %>%
    mutate(int_col = rollapply(int_col, 9, length, partial = TRUE, align = "right"))
  new_th1a <- head(data$th3, 500)$
    update_by(uby_rolling_count_time("time_col", "int_col", "PT8s"))
  expect_equal(as.data.frame(new_th1a), as.data.frame(new_tb1a))

  new_tb1b <- head(data$df3, 500) %>%
    mutate(int_col = rollapply(int_col, 9, length, partial = TRUE, align = "left"))
  new_th1b <- head(data$th3, 500)$
    update_by(uby_rolling_count_time("time_col", "int_col", "PT0s", "PT8s"))
  expect_equal(as.data.frame(new_th1b), as.data.frame(new_tb1b))

  new_tb1c <- head(data$df3, 500) %>%
    mutate(int_col = rollapply(int_col, 9, length, partial = TRUE, align = "center"))
  new_th1c <- head(data$th3, 500)$
    update_by(uby_rolling_count_time("time_col", "int_col", "PT4s", "PT4s"))
  expect_equal(as.data.frame(new_th1c), as.data.frame(new_tb1c))

  new_tb2a <- head(data$df3, 500) %>%
    mutate(int_col = custom_rolling_time_op(int_col, bool_col, width = 9, FUN = custom_count, partial = TRUE, align = "right"))
  new_th2a <- head(data$th3, 500)$
    update_by(uby_rolling_count_time("time_col", "int_col", "PT8s"), by = "bool_col")
  expect_equal(as.data.frame(new_th2a), as.data.frame(new_tb2a))

  new_tb2b <- head(data$df3, 500) %>%
    mutate(int_col = custom_rolling_time_op(int_col, bool_col, width = 9, FUN = custom_count, partial = TRUE, align = "left"))
  new_th2b <- head(data$th3, 500)$
    update_by(uby_rolling_count_time("time_col", "int_col", "PT0s", "PT8s"), by = "bool_col")
  expect_equal(as.data.frame(new_th2b), as.data.frame(new_tb2b))

  new_tb2c <- head(data$df3, 500) %>%
    mutate(int_col = custom_rolling_time_op(int_col, bool_col, width = 9, FUN = custom_count, partial = TRUE, align = "center"))
  new_th2c <- head(data$th3, 500)$
    update_by(uby_rolling_count_time("time_col", "int_col", "PT4s", "PT4s"), by = "bool_col")
  expect_equal(as.data.frame(new_th2b), as.data.frame(new_tb2b))

  data$client$close()
})

test_that("uby_rolling_std_tick behaves as expected", {
  data <- setup()

  new_tb1a <- data$df1 %>%
    mutate(dbl_col = rollapply(dbl_col, 3, sd, partial = TRUE, align = "right"))
  new_th1a <- data$th1$
    update_by(uby_rolling_std_tick("dbl_col", rev_ticks = 3))
  expect_equal(as.data.frame(new_th1a), as.data.frame(new_tb1a))

  new_tb1b <- data$df1 %>%
    mutate(dbl_col = rollapply(dbl_col, 3, sd, partial = TRUE, align = "left"))
  new_th1b <- data$th1$
    update_by(uby_rolling_std_tick("dbl_col", rev_ticks = 1, fwd_ticks = 2))
  expect_equal(as.data.frame(new_th1b), as.data.frame(new_tb1b))

  new_tb1c <- data$df1 %>%
    mutate(dbl_col = rollapply(dbl_col, 3, sd, partial = TRUE, align = "center"))
  new_th1c <- data$th1$
    update_by(uby_rolling_std_tick("dbl_col", rev_ticks = 2, fwd_ticks = 1))
  expect_equal(as.data.frame(new_th1c), as.data.frame(new_tb1c))

  new_tb2a <- data$df2 %>%
    mutate(
      col1 = rollapply(col1, 5, sd, partial = TRUE, align = "right"),
      col3 = rollapply(col3, 5, sd, partial = TRUE, align = "right")
    )
  new_th2a <- data$th2$
    update_by(uby_rolling_std_tick(c("col1", "col3"), rev_ticks = 5))
  expect_equal(as.data.frame(new_th2a), as.data.frame(new_tb2a))

  new_tb2b <- data$df2 %>%
    mutate(
      col1 = rollapply(col1, 5, sd, partial = TRUE, align = "left"),
      col3 = rollapply(col3, 5, sd, partial = TRUE, align = "left")
    )
  new_th2b <- data$th2$
    update_by(uby_rolling_std_tick(c("col1", "col3"), rev_ticks = 1, fwd_ticks = 4))
  expect_equal(as.data.frame(new_th2b), as.data.frame(new_tb2b))

  new_tb2c <- data$df2 %>%
    mutate(
      col1 = rollapply(col1, 5, sd, partial = TRUE, align = "center"),
      col3 = rollapply(col3, 5, sd, partial = TRUE, align = "center")
    )
  new_th2c <- data$th2$
    update_by(uby_rolling_std_tick(c("col1", "col3"), rev_ticks = 3, fwd_ticks = 2))
  expect_equal(as.data.frame(new_th2c), as.data.frame(new_tb2c))

  new_tb3a <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(int_col = rollapply(int_col, 9, sd, partial = TRUE, align = "right"))
  new_th3a <- data$th3$
    update_by(uby_rolling_std_tick("int_col", rev_ticks = 9), by = "bool_col")
  expect_equal(as.data.frame(new_th3a), as.data.frame(new_tb3a))

  new_tb3b <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(int_col = rollapply(int_col, 9, sd, partial = TRUE, align = "left"))
  new_th3b <- data$th3$
    update_by(uby_rolling_std_tick("int_col", rev_ticks = 1, fwd_ticks = 8), by = "bool_col")
  expect_equal(as.data.frame(new_th3b), as.data.frame(new_tb3b))

  new_tb3c <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(int_col = rollapply(int_col, 9, sd, partial = TRUE, align = "center"))
  new_th3c <- data$th3$
    update_by(uby_rolling_std_tick("int_col", rev_ticks = 5, fwd_ticks = 4), by = "bool_col")
  expect_equal(as.data.frame(new_th3c), as.data.frame(new_tb3c))

  new_tb4a <- data$df4 %>%
    group_by(X) %>%
    mutate(
      Number1 = rollapply(Number1, 3, sd, partial = TRUE, align = "right"),
      Number2 = rollapply(Number2, 3, sd, partial = TRUE, align = "right")
    )
  new_th4a <- data$th4$
    update_by(uby_rolling_std_tick(c("Number1", "Number2"), rev_ticks = 3), by = "X")
  expect_equal(as.data.frame(new_th4a), as.data.frame(new_tb4a))

  new_tb4b <- data$df4 %>%
    group_by(X) %>%
    mutate(
      Number1 = rollapply(Number1, 3, sd, partial = TRUE, align = "left"),
      Number2 = rollapply(Number2, 3, sd, partial = TRUE, align = "left")
    )
  new_th4b <- data$th4$
    update_by(uby_rolling_std_tick(c("Number1", "Number2"), rev_ticks = 1, fwd_ticks = 2), by = "X")
  expect_equal(as.data.frame(new_th4b), as.data.frame(new_tb4b))

  new_tb4c <- data$df4 %>%
    group_by(X) %>%
    mutate(
      Number1 = rollapply(Number1, 3, sd, partial = TRUE, align = "center"),
      Number2 = rollapply(Number2, 3, sd, partial = TRUE, align = "center")
    )
  new_th4c <- data$th4$
    update_by(uby_rolling_std_tick(c("Number1", "Number2"), rev_ticks = 2, fwd_ticks = 1), by = "X")
  expect_equal(as.data.frame(new_th4c), as.data.frame(new_tb4c))

  new_tb5a <- data$df5 %>%
    group_by(Y) %>%
    mutate(
      Number1 = rollapply(Number1, 3, sd, partial = TRUE, align = "right"),
      Number2 = rollapply(Number2, 3, sd, partial = TRUE, align = "right")
    )
  new_th5a <- data$th5$
    update_by(uby_rolling_std_tick(c("Number1", "Number2"), rev_ticks = 3), by = "Y")
  expect_equal(as.data.frame(new_th5a), as.data.frame(new_tb5a))

  new_tb5b <- data$df5 %>%
    group_by(Y) %>%
    mutate(
      Number1 = rollapply(Number1, 3, sd, partial = TRUE, align = "left"),
      Number2 = rollapply(Number2, 3, sd, partial = TRUE, align = "left")
    )
  new_th5b <- data$th5$
    update_by(uby_rolling_std_tick(c("Number1", "Number2"), rev_ticks = 1, fwd_ticks = 2), by = "Y")
  expect_equal(as.data.frame(new_th5b), as.data.frame(new_tb5b))

  new_tb5c <- data$df5 %>%
    group_by(Y) %>%
    mutate(
      Number1 = rollapply(Number1, 3, sd, partial = TRUE, align = "center"),
      Number2 = rollapply(Number2, 3, sd, partial = TRUE, align = "center")
    )
  new_th5c <- data$th5$
    update_by(uby_rolling_std_tick(c("Number1", "Number2"), rev_ticks = 2, fwd_ticks = 1), by = "Y")
  expect_equal(as.data.frame(new_th5c), as.data.frame(new_tb5c))

  new_tb6a <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
    group_by(X, Y) %>%
    mutate(
      Number1 = rollapply(Number1, 3, sd, partial = TRUE, align = "right"),
      Number2 = rollapply(Number2, 3, sd, partial = TRUE, align = "right")
    )
  new_th6a <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(uby_rolling_std_tick(c("Number1", "Number2"), rev_ticks = 3), by = c("X", "Y"))
  expect_equal(as.data.frame(new_th6a), as.data.frame(new_tb6a))

  new_tb6b <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
    group_by(X, Y) %>%
    mutate(
      Number1 = rollapply(Number1, 3, sd, partial = TRUE, align = "left"),
      Number2 = rollapply(Number2, 3, sd, partial = TRUE, align = "left")
    )
  new_th6b <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(uby_rolling_std_tick(c("Number1", "Number2"), rev_ticks = 1, fwd_ticks = 2), by = c("X", "Y"))
  expect_equal(as.data.frame(new_th6b), as.data.frame(new_tb6b))

  new_tb6c <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
    group_by(X, Y) %>%
    mutate(
      Number1 = rollapply(Number1, 3, sd, partial = TRUE, align = "center"),
      Number2 = rollapply(Number2, 3, sd, partial = TRUE, align = "center")
    )
  new_th6c <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(uby_rolling_std_tick(c("Number1", "Number2"), rev_ticks = 2, fwd_ticks = 1), by = c("X", "Y"))
  expect_equal(as.data.frame(new_th6c), as.data.frame(new_tb6c))

  data$client$close()
})

test_that("uby_rolling_std_time behaves as expected", {
  data <- setup()

  new_tb1a <- head(data$df3, 500) %>%
    mutate(int_col = rollapply(int_col, 9, sd, partial = TRUE, align = "right"))
  new_th1a <- head(data$th3, 500)$
    update_by(uby_rolling_std_time("time_col", "int_col", "PT8s"))
  expect_equal(as.data.frame(new_th1a), as.data.frame(new_tb1a))

  new_tb1b <- head(data$df3, 500) %>%
    mutate(int_col = rollapply(int_col, 9, sd, partial = TRUE, align = "left"))
  new_th1b <- head(data$th3, 500)$
    update_by(uby_rolling_std_time("time_col", "int_col", "PT0s", "PT8s"))
  expect_equal(as.data.frame(new_th1b), as.data.frame(new_tb1b))

  new_tb1c <- head(data$df3, 500) %>%
    mutate(int_col = rollapply(int_col, 9, sd, partial = TRUE, align = "center"))
  new_th1c <- head(data$th3, 500)$
    update_by(uby_rolling_std_time("time_col", "int_col", "PT4s", "PT4s"))
  expect_equal(as.data.frame(new_th1c), as.data.frame(new_tb1c))

  new_tb2a <- head(data$df3, 500) %>%
    mutate(int_col = custom_rolling_time_op(int_col, bool_col, width = 9, FUN = sd, partial = TRUE, align = "right", na.rm = TRUE))
  new_th2a <- head(data$th3, 500)$
    update_by(uby_rolling_std_time("time_col", "int_col", "PT8s"), by = "bool_col")
  expect_equal(as.data.frame(new_th2a), as.data.frame(new_tb2a))

  new_tb2b <- head(data$df3, 500) %>%
    mutate(int_col = custom_rolling_time_op(int_col, bool_col, width = 9, FUN = sd, partial = TRUE, align = "left", na.rm = TRUE))
  new_th2b <- head(data$th3, 500)$
    update_by(uby_rolling_std_time("time_col", "int_col", "PT0s", "PT8s"), by = "bool_col")
  expect_equal(as.data.frame(new_th2b), as.data.frame(new_tb2b))

  new_tb2c <- head(data$df3, 500) %>%
    mutate(int_col = custom_rolling_time_op(int_col, bool_col, width = 9, FUN = sd, partial = TRUE, align = "center", na.rm = TRUE))
  new_th2c <- head(data$th3, 500)$
    update_by(uby_rolling_std_time("time_col", "int_col", "PT4s", "PT4s"), by = "bool_col")
  expect_equal(as.data.frame(new_th2b), as.data.frame(new_tb2b))

  data$client$close()
})

test_that("uby_rolling_wavg_tick behaves as expected", {
  data <- setup()

  # There is not a clean analog to our grouped weighted average in R, so we create
  # these tables directly

  new_df1a <- data.frame(
    string_col = c("I", "am", "a", "string", "column"),
    int_col = c(0, 1, 2, 3, 4),
    dbl_col = c(NA, 3.1234, 66668.0411, 33605.6379, 22403.4115)
  )
  new_th1a <- data$th1$
    update_by(uby_rolling_wavg_tick("int_col", "dbl_col", rev_ticks = 3))
  expect_true(all.equal(as.data.frame(new_th1a), new_df1a, tolerance = 1e-4))

  new_df1b <- data.frame(
    string_col = c("I", "am", "a", "string", "column"),
    int_col = c(0, 1, 2, 3, 4),
    dbl_col = c(66668.0411, 33605.6379, 22403.4115, 232.8148, 0)
  )
  new_th1b <- data$th1$
    update_by(uby_rolling_wavg_tick("int_col", "dbl_col", rev_ticks = 1, fwd_ticks = 2))
  expect_true(all.equal(as.data.frame(new_th1b), new_df1b, tolerance = 1e-4))

  new_df1c <- data.frame(
    string_col = c("I", "am", "a", "string", "column"),
    int_col = c(0, 1, 2, 3, 4),
    dbl_col = c(3.1234, 66668.0411, 33605.6379, 22403.4115, 232.8148)
  )
  new_th1c <- data$th1$
    update_by(uby_rolling_wavg_tick("int_col", "dbl_col", rev_ticks = 2, fwd_ticks = 1))
  expect_true(all.equal(as.data.frame(new_th1c), new_df1c, tolerance = 1e-4))

  new_df4a <- data.frame(
    X = c("A", "B", "A", "C", "B", "A", "B", "B", "C"),
    Y = c("M", "N", "O", "N", "P", "M", "O", "P", "M"),
    Number1 = c(100.00000, -44.00000, 83.22819, 11.00000, -57.20000, 74.87940, -88.06173, -290.57895, -85.10169),
    Number2 = c(-55.00000, 76.00000, -30.33557, 130.00000, 168.40000, -35.27638, 202.55556, 557.73684, 229.66102)
  )
  new_th4a <- data$th4$
    update_by(uby_rolling_wavg_tick("Number1", c("Number1", "Number2"), rev_ticks = 3), by = "X")
  expect_true(all.equal(as.data.frame(new_th4a), new_df4a, tolerance = 1e-4))

  new_df4b <- data.frame(
    X = c("A", "B", "A", "C", "B", "A", "B", "B", "C"),
    Y = c("M", "N", "O", "N", "P", "M", "O", "P", "M"),
    Number1 = c(74.87940, -88.06173, 49.50505, -85.10169, -290.57895, 50.00000, 24.78723, 18.00000, -70.00000),
    Number2 = c(-35.27638, 202.55556, -15.35354, 229.66102, 557.73684, -50.00000, 97.51064, 137.00000, 214.00000)
  )
  new_th4b <- data$th4$
    update_by(uby_rolling_wavg_tick("Number1", c("Number1", "Number2"), rev_ticks = 1, fwd_ticks = 2), by = "X")
  expect_true(all.equal(as.data.frame(new_th4b), new_df4b, tolerance = 1e-4))

  new_df4c <- data.frame(
    X = c("A", "B", "A", "C", "B", "A", "B", "B", "C"),
    Y = c("M", "N", "O", "N", "P", "M", "O", "P", "M"),
    Number1 = c(83.22819, -57.20000, 74.87940, -85.10169, -88.06173, 49.50505, -290.57895, 24.78723, -85.10169),
    Number2 = c(-30.33557, 168.40000, -35.27638, 229.66102, 202.55556, -15.35354, 557.73684, 97.51064, 229.66102)
  )
  new_th4c <- data$th4$
    update_by(uby_rolling_wavg_tick("Number1", c("Number1", "Number2"), rev_ticks = 2, fwd_ticks = 1), by = "X")
  expect_true(all.equal(as.data.frame(new_th4c), new_df4c, tolerance = 1e-4))

  new_df5a <- data.frame(
    X = c("B", "C", "B", "A", "A", "C", "B", "C", "B", "A"),
    Y = c("N", "N", "M", "P", "O", "P", "O", "N", "O", "O"),
    Number1 = c(55.00000, 55.85000, 86.00000, -45.00000, 1.00000, 36.42857, 272.57895, 63.90667, 207.72527, 221.89412),
    Number2 = c(76.00000, 72.40000, -6.00000, 34.00000, 12.00000, -165.04762, 38.05263, 77.56000, 36.53846, 37.84706)
  )
  new_th5a <- data$th5$
    update_by(uby_rolling_wavg_tick("Number2", c("Number1", "Number2"), rev_ticks = 3), by = "Y")
  expect_true(all.equal(as.data.frame(new_th5a), new_df5a, tolerance = 1e-4))

  new_df5b <- data.frame(
    X = c("B", "C", "B", "A", "A", "C", "B", "C", "B", "A"),
    Y = c("N", "N", "M", "P", "O", "P", "O", "N", "O", "O"),
    Number1 = c(63.90667, -613.00000, 86.00000, 36.42857, 207.72527, 0.00000, 221.89412, -65.00000, 83.40000, -5.00000),
    Number2 = c(77.56000, -41.00000, -6.00000, -165.04762, 36.53846, -76.00000, 37.84706, -5.00000, 29.80000, 6.00000)
  )
  new_th5b <- data$th5$
    update_by(uby_rolling_wavg_tick("Number2", c("Number1", "Number2"), rev_ticks = 1, fwd_ticks = 2), by = "Y")
  expect_true(all.equal(as.data.frame(new_th5b), new_df5b, tolerance = 1e-4))

  new_df5c <- data.frame(
    X = c("B", "C", "B", "A", "A", "C", "B", "C", "B", "A"),
    Y = c("N", "N", "M", "P", "O", "P", "O", "N", "O", "O"),
    Number1 = c(55.85000, 63.90667, 86.00000, 36.42857, 272.57895, 36.42857, 207.72527, -613.00000, 221.89412, 83.40000),
    Number2 = c(72.40000, 77.56000, -6.00000, -165.04762, 38.05263, -165.04762, 36.53846, -41.00000, 37.84706, 29.80000)
  )
  new_th5c <- data$th5$
    update_by(uby_rolling_wavg_tick("Number2", c("Number1", "Number2"), rev_ticks = 2, fwd_ticks = 1), by = "Y")
  expect_true(all.equal(as.data.frame(new_th5c), new_df5c, tolerance = 1e-4))

  new_df6a <- data.frame(
    X = c(
      "A", "B", "A", "C", "B", "A", "B", "B", "C", "B",
      "C", "B", "A", "A", "C", "B", "C", "B", "A", "A",
      "B", "A", "C", "B", "A", "B", "B", "C", "B", "C",
      "B", "A", "A", "C", "B", "C", "B", "A"
    ),
    Y = c(
      "M", "N", "O", "N", "P", "M", "O", "P", "M", "N",
      "N", "M", "P", "O", "P", "O", "N", "O", "O", "M",
      "N", "O", "N", "P", "M", "O", "P", "M", "N", "N",
      "M", "P", "O", "P", "O", "N", "O", "O"
    ),
    Number1 = c(
      100.00000, -44.00000, 49.00000, 11.00000, -66.00000,
      83.33333, 29.00000, -97.50000, -70.00000, 451.00000,
      63.91566, 86.00000, -45.00000, 48.04000, NA, 320.49733,
      529.44444, 274.13742, 53.93333, 90.00000,
      -209.00000, 53.93333, 529.44444, -79.26316,
      75.00000, 274.13742, -166.80000, -70.00000, 121.00000,
      529.44444, 86.00000, -45.00000, 53.93333, NA, 274.13742,
      529.44444, 274.13742, 53.93333
    ),
    Number2 = c(
      -55.00000, 76.00000, 20.00000, 130.00000, 230.00000,
      -53.33333, 73.00000, 264.87500, 214.00000, 76.00000,
      20.69880, -6.00000, 34.00000, 19.84000, NA, 47.17112,
      113.50000, 44.41438, 21.37778, -54.00000,
      76.00000, 21.37778, 113.50000, 244.68421,
      -52.50000, 44.41438, 341.60000, 214.00000, 76.00000,
      113.50000, -6.00000, 34.00000, 21.37778, NA, 44.41438,
      113.50000, 44.41438, 21.37778
    )
  )
  new_th6a <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(uby_rolling_wavg_tick("Number1", c("Number1", "Number2"), rev_ticks = 3), by = c("X", "Y"))
  expect_true(all.equal(as.data.frame(new_th6a), new_df6a, tolerance = 1e-4))

  new_df6b <- data.frame(
    X = c(
      "A", "B", "A", "C", "B", "A", "B", "B", "C", "B",
      "C", "B", "A", "A", "C", "B", "C", "B", "A", "A",
      "B", "A", "C", "B", "A", "B", "B", "C", "B", "C",
      "B", "A", "A", "C", "B", "C", "B", "A"
    ),
    Y = c(
      "M", "N", "O", "N", "P", "M", "O", "P", "M", "N",
      "N", "M", "P", "O", "P", "O", "N", "O", "O", "M",
      "N", "O", "N", "P", "M", "O", "P", "M", "N", "N",
      "M", "P", "O", "P", "O", "N", "O", "O"
    ),
    Number1 = c(
      90.00000, -209.00000, 53.93333, 529.44444, -79.26316,
      75.00000, 274.13742, -166.80000, -70.00000, 121.00000,
      529.44444, 86.00000, -45.00000, 53.93333, NA, 274.13742,
      529.44444, 274.13742, 53.93333, 83.33333,
      451.00000, 53.93333, 529.44444, -97.50000,
      50.00000, 274.13742, 18.00000, -70.00000, 55.00000,
      1344.14286, 86.00000, -45.00000, -6.50000, NA, 290.14865,
      -65.00000, 99.00000, -5.00000
    ),
    Number2 = c(
      -54.00000, 76.00000, 21.37778, 113.50000, 244.68421,
      -52.50000, 44.41438, 341.60000, 214.00000, 76.00000,
      113.50000, -6.00000, 34.00000, 21.37778, NA, 44.41438,
      113.50000, 44.41438, 21.37778, -53.33333,
      76.00000, 21.37778, 113.50000, 264.87500,
      -50.00000, 44.41438, 137.00000, 214.00000, 76.00000,
      87.57143, -6.00000, 34.00000, 4.50000, NA, 42.54730,
      -5.00000, 34.00000, 6.00000
    )
  )
  new_th6b <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(uby_rolling_wavg_tick("Number1", c("Number1", "Number2"), rev_ticks = 1, fwd_ticks = 2), by = c("X", "Y"))
  expect_true(all.equal(as.data.frame(new_th6b), new_df6b, tolerance = 1e-4))

  new_df6c <- data.frame(
    X = c(
      "A", "B", "A", "C", "B", "A", "B", "B", "C", "B",
      "C", "B", "A", "A", "C", "B", "C", "B", "A", "A",
      "B", "A", "C", "B", "A", "B", "B", "C", "B", "C",
      "B", "A", "A", "C", "B", "C", "B", "A"
    ),
    Y = c(
      "M", "N", "O", "N", "P", "M", "O", "P", "M", "N",
      "N", "M", "P", "O", "P", "O", "N", "O", "O", "M",
      "N", "O", "N", "P", "M", "O", "P", "M", "N", "N",
      "M", "P", "O", "P", "O", "N", "O", "O"
    ),
    Number1 = c(
      83.33333, 451.00000, 48.04000, 63.91566, -97.50000,
      90.00000, 320.49733, -79.26316, -70.00000, -209.00000,
      529.44444, 86.00000, -45.00000, 53.93333, NA, 274.13742,
      529.44444, 274.13742, 53.93333, 75.00000,
      121.00000, 53.93333, 529.44444, -166.80000,
      83.33333, 274.13742, -97.50000, -70.00000, 451.00000,
      529.44444, 86.00000, -45.00000, 53.93333, NA, 274.13742,
      1344.14286, 290.14865, -6.50000
    ),
    Number2 = c(
      -53.33333, 76.00000, 19.84000, 20.69880, 264.87500,
      -54.00000, 47.17112, 244.68421, 214.00000, 76.00000,
      113.50000, -6.00000, 34.00000, 21.37778, NA, 44.41438,
      113.50000, 44.41438, 21.37778, -52.50000,
      76.00000, 21.37778, 113.50000, 341.60000,
      -53.33333, 44.41438, 264.87500, 214.00000, 76.00000,
      113.50000, -6.00000, 34.00000, 21.37778, NA, 44.41438,
      87.57143, 42.54730, 4.50000
    )
  )
  new_th6c <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(uby_rolling_wavg_tick("Number1", c("Number1", "Number2"), rev_ticks = 2, fwd_ticks = 1), by = c("X", "Y"))
  expect_true(all.equal(as.data.frame(new_th6c), new_df6c, tolerance = 1e-4))

  data$client$close()
})

test_that("uby_rolling_wavg_time behaves as expected", {
  data <- setup()

  # Need to append a weight column to the df and th
  data$deterministic_df3 <- data$deterministic_df3 %>%
    mutate(weight_col = sqrt(int_col))
  data$deterministic_th3 <- data$deterministic_th3$
    update("weight_col = sqrt(int_col)")

  base_df <- data.frame(
    time_col = seq.POSIXt(as.POSIXct(Sys.Date()), as.POSIXct(Sys.Date() + 0.001), by = "1 sec")[1:50],
    bool_col = c(
      TRUE, TRUE, FALSE, FALSE, TRUE, FALSE, TRUE, FALSE, FALSE, TRUE,
      TRUE, TRUE, FALSE, FALSE, TRUE, FALSE, TRUE, FALSE, FALSE, TRUE,
      TRUE, TRUE, FALSE, FALSE, TRUE, FALSE, TRUE, FALSE, FALSE, TRUE,
      TRUE, TRUE, FALSE, FALSE, TRUE, FALSE, TRUE, FALSE, FALSE, TRUE,
      TRUE, TRUE, FALSE, FALSE, TRUE, FALSE, TRUE, FALSE, FALSE, TRUE
    )
  )

  new_df1a <- cbind(base_df, "int_col" = c(
    1.000000, 1.585786, 2.176557, 2.769907, 3.364806, 3.960724,
    4.557357, 5.154516, 5.752074, 6.599146, 7.501993, 8.433335,
    9.381805, 10.341536, 11.309121, 12.282429, 13.260044, 14.240990,
    15.224566, 16.210258, 17.197678, 18.186530, 19.176580, 20.167643,
    21.159573, 22.152247, 23.145567, 24.139451, 25.133831, 26.128647,
    27.123851, 28.119401, 29.115260, 30.111398, 31.107787, 32.104403,
    33.101225, 34.098236, 35.095418, 36.092758, 37.090243, 38.087860,
    39.085600, 40.083454, 41.081413, 42.079469, 43.077616, 44.075848,
    45.074159, 46.072543
  ))
  new_th1a <- head(data$deterministic_th3, 50)$
    update_by(uby_rolling_wavg_time("time_col", "weight_col", "int_col", "PT8s"))$
    drop_columns("weight_col")
  expect_equal(as.data.frame(new_th1a), new_df1a)

  new_df1b <- cbind(base_df, "int_col" = c(
    5.752074, 6.599146, 7.501993, 8.433335, 9.381805, 10.341536,
    11.309121, 12.282429, 13.260044, 14.240990, 15.224566,
    16.210258, 17.197678, 18.186530, 19.176580, 20.167643,
    21.159573, 22.152247, 23.145567, 24.139451, 25.133831,
    26.128647, 27.123851, 28.119401, 29.115260, 30.111398,
    31.107787, 32.104403, 33.101225, 34.098236, 35.095418,
    36.092758, 37.090243, 38.087860, 39.085600, 40.083454,
    41.081413, 42.079469, 43.077616, 44.075848, 45.074159,
    46.072543, 46.556499, 47.042580, 47.530715, 48.020839,
    48.512889, 49.006803, 49.502525, 50.000000
  ))
  new_th1b <- head(data$deterministic_th3, 50)$
    update_by(uby_rolling_wavg_time("time_col", "weight_col", "int_col", "PT0s", "PT8s"))$
    drop_columns("weight_col")
  expect_equal(as.data.frame(new_th1b), new_df1b)

  new_df1c <- cbind(base_df, "int_col" = c(
    3.364806, 3.960724, 4.557357, 5.154516, 5.752074, 6.599146,
    7.501993, 8.433335, 9.381805, 10.341536, 11.309121,
    12.282429, 13.260044, 14.240990, 15.224566, 16.210258,
    17.197678, 18.186530, 19.176580, 20.167643, 21.159573,
    22.152247, 23.145567, 24.139451, 25.133831, 26.128647,
    27.123851, 28.119401, 29.115260, 30.111398, 31.107787,
    32.104403, 33.101225, 34.098236, 35.095418, 36.092758,
    37.090243, 38.087860, 39.085600, 40.083454, 41.081413,
    42.079469, 43.077616, 44.075848, 45.074159, 46.072543,
    46.556499, 47.042580, 47.530715, 48.020839
  ))
  new_th1c <- head(data$deterministic_th3, 50)$
    update_by(uby_rolling_wavg_time("time_col", "weight_col", "int_col", "PT4s", "PT4s"))$
    drop_columns("weight_col")
  expect_equal(as.data.frame(new_th1c), new_df1c)

  new_df2a <- cbind(base_df, "int_col" = c(
    1.000000, 1.585786, 3.000000, 3.535898, 3.227496, 4.512320,
    4.595515, 5.607180, 6.454681, 6.782586, 8.609158, 9.401493,
    9.357245, 10.469018, 11.316537, 12.394463, 13.259445,
    15.370444, 16.163520, 16.268778, 18.407602, 19.182620,
    19.169030, 20.230497, 21.162927, 22.211264, 23.146719,
    25.322767, 26.100235, 26.164183, 28.351280, 29.118634,
    29.111044, 30.153311, 31.109992, 32.144589, 33.102407,
    35.302156, 36.072311, 36.118326, 38.324651, 39.087902,
    39.082723, 40.114920, 41.083063, 42.109955, 43.078675,
    45.290649, 46.056564, 46.092521
  ))
  new_th2a <- head(data$deterministic_th3, 50)$
    update_by(uby_rolling_wavg_time("time_col", "weight_col", "int_col", "PT8s"), by = "bool_col")$
    drop_columns("weight_col")
  expect_equal(as.data.frame(new_th2a), new_df2a)

  new_df2b <- cbind(base_df, "int_col" = c(
    4.595515, 6.782586, 6.454681, 7.036869, 9.401493, 10.469018,
    11.316537, 12.394463, 13.260793, 13.259445, 13.956975,
    16.268778, 16.163520, 16.861439, 19.182620, 20.230497,
    21.162927, 22.211264, 23.144128, 23.146719, 23.869532,
    26.164183, 26.100235, 26.819386, 29.118634, 30.153311,
    31.109992, 32.144589, 33.099748, 33.102407, 33.834106,
    36.118326, 36.072311, 36.800397, 39.087902, 40.114920,
    41.083063, 42.109955, 43.076293, 43.078675, 43.814892,
    46.092521, 46.056564, 46.789573, 47.377845, 47.683028,
    48.523201, 48.502577, 49.000000, 50.000000
  ))
  new_th2b <- head(data$deterministic_th3, 50)$
    update_by(uby_rolling_wavg_time("time_col", "weight_col", "int_col", "PT0s", "PT8s"), by = "bool_col")$
    drop_columns("weight_col")
  expect_equal(as.data.frame(new_th2b), new_df2b)

  new_df2c <- cbind(base_df, "int_col" = c(
    3.227496, 3.227496, 4.512320, 5.607180, 4.595515, 6.454681,
    8.609158, 7.036869, 9.357245, 10.183304, 11.316537, 12.143151,
    13.260793, 15.370444, 13.956975, 16.163520, 18.407602, 16.861439,
    19.169030, 20.089214, 21.162927, 22.078589, 23.144128, 25.322767,
    23.869532, 26.100235, 28.351280, 26.819386, 29.111044, 30.059047,
    31.109992, 32.054206, 33.099748, 35.302156, 33.834106, 36.072311,
    38.324651, 36.800397, 39.082723, 40.044139, 41.083063, 42.041378,
    43.076293, 45.290649, 43.814892, 46.056564, 47.377845, 46.789573,
    47.683028, 48.523201
  ))
  new_th2c <- head(data$deterministic_th3, 50)$
    update_by(uby_rolling_wavg_time("time_col", "weight_col", "int_col", "PT4s", "PT4s"), by = "bool_col")$
    drop_columns("weight_col")
  expect_equal(as.data.frame(new_th2c), new_df2c)

  data$client$close()
})
