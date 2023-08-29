library(testthat)
library(dplyr)
library(rdeephaven)
library(lubridate)
library(zoo)

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
  client <- Client$new(target = "localhost:10000")

  # move dataframes to server and get TableHandles for testing
  th1 <- client$import_table(df1)
  th2 <- client$import_table(df2)
  th3 <- client$import_table(df3)
  th4 <- client$import_table(df4)
  th5 <- client$import_table(df5)

  return(list(
    "client" = client,
    "df1" = df1, "df2" = df2, "df3" = df3, "df4" = df4, "df5" = df5,
    "th1" = th1, "th2" = th2, "th3" = th3, "th4" = th4, "th5" = th5
  ))
}

test_that("udb_cum_sum behaves as expected", {
  data <- setup()
  
  new_tb1 <- data$df1 %>%
    mutate(sum_int_col = cumsum(int_col))
  new_th1 <- data$th1$
    update_by(udb_cum_sum("sum_int_col = int_col"))
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))
  
  new_tb2 <- data$df2 %>%
    mutate(sum_col1 = cumsum(col1), sum_col3 = cumsum(col3))
  new_th2 <- data$th2$
    update_by(udb_cum_sum(c("sum_col1 = col1", "sum_col3 = col3")))
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))
  
  new_tb3 <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(sum_int_col = cumsum(int_col))
  new_th3 <- data$th3$
    update_by(udb_cum_sum("sum_int_col = int_col"), by = "bool_col")
  expect_equal(as.data.frame(new_th3), as.data.frame(new_tb3))
  
  new_tb4 <- data$df4 %>%
    group_by(X) %>%
    mutate(sum_Number1 = cumsum(Number1), sum_Number2 = cumsum(Number2))
  new_th4 <- data$th4$
    update_by(udb_cum_sum(c("sum_Number1 = Number1", "sum_Number2 = Number2")), by = "X")
  expect_equal(as.data.frame(new_th4), as.data.frame(new_tb4))
  
  new_tb5 <- data$df5 %>%
    group_by(Y) %>%
    mutate(sum_Number1 = cumsum(Number1), sum_Number2 = cumsum(Number2))
  new_th5 <- data$th5$
    update_by(udb_cum_sum(c("sum_Number1 = Number1", "sum_Number2 = Number2")), by = "Y")
  expect_equal(as.data.frame(new_th5), as.data.frame(new_tb5))
  
  new_tb6 <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
    group_by(X, Y) %>%
    mutate(sum_Number1 = cumsum(Number1), sum_Number2 = cumsum(Number2))
  new_th6 <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(udb_cum_sum(c("sum_Number1 = Number1", "sum_Number2 = Number2")), by = c("X", "Y"))
  expect_equal(as.data.frame(new_th6), as.data.frame(new_tb6))
  
  data$client$close()
})

test_that("udb_cum_prod behaves as expected", {
  data <- setup()
  
  new_tb1 <- data$df1 %>%
    mutate(prod_int_col = cumprod(int_col))
  new_th1 <- data$th1$
    update_by(udb_cum_prod("prod_int_col = int_col"))
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))
  
  new_tb2 <- data$df2 %>%
    mutate(prod_col1 = cumprod(col1), prod_col3 = cumprod(col3))
  new_th2 <- data$th2$
    update_by(udb_cum_prod(c("prod_col1 = col1", "prod_col3 = col3")))
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))
  
  # TODO: Get rid of R's scientific notation
  # new_tb3 <- head(data$df3, 20) %>%
  #   group_by(bool_col) %>%
  #   mutate(prod_int_col = cumprod(int_col))
  # new_th3 <- head(data$th3, 20)$
  #   update_view("big_int_col=java.math.BigInteger.valueOf(int_col)")$
  #   update_by(udb_cum_prod("prod_int_col = big_int_col"), by = "bool_col")
  # expect_equal(as.data.frame(new_th3), as.data.frame(new_tb3))
  
  new_tb4 <- data$df4 %>%
    group_by(X) %>%
    mutate(prod_Number1 = cumprod(Number1), prod_Number2 = cumprod(Number2))
  new_th4 <- data$th4$
    update_by(udb_cum_prod(c("prod_Number1 = Number1", "prod_Number2 = Number2")), by = "X")
  expect_equal(as.data.frame(new_th4), as.data.frame(new_tb4))
  
  new_tb5 <- data$df5 %>%
    group_by(Y) %>%
    mutate(prod_Number1 = cumprod(Number1), prod_Number2 = cumprod(Number2))
  new_th5 <- data$th5$
    update_by(udb_cum_prod(c("prod_Number1 = Number1", "prod_Number2 = Number2")), by = "Y")
  expect_equal(as.data.frame(new_th5), as.data.frame(new_tb5))
  
  new_tb6 <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
    group_by(X, Y) %>%
    mutate(prod_Number1 = cumprod(Number1), prod_Number2 = cumprod(Number2))
  new_th6 <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(udb_cum_prod(c("prod_Number1 = Number1", "prod_Number2 = Number2")), by = c("X", "Y"))
  expect_equal(as.data.frame(new_th6), as.data.frame(new_tb6))
  
  data$client$close()
})

test_that("udb_cum_min behaves as expected", {
  data <- setup()
  
  new_tb1 <- data$df1 %>%
    mutate(min_int_col = cummin(int_col))
  new_th1 <- data$th1$
    update_by(udb_cum_min("min_int_col = int_col"))
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))
  
  new_tb2 <- data$df2 %>%
    mutate(min_col1 = cummin(col1), min_col3 = cummin(col3))
  new_th2 <- data$th2$
    update_by(udb_cum_min(c("min_col1 = col1", "min_col3 = col3")))
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))
  
  new_tb3 <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(min_int_col = cummin(int_col))
  new_th3 <- data$th3$
    update_by(udb_cum_min("min_int_col = int_col"), by = "bool_col")
  expect_equal(as.data.frame(new_th3), as.data.frame(new_tb3))
  
  new_tb4 <- data$df4 %>%
    group_by(X) %>%
    mutate(min_Number1 = cummin(Number1), min_Number2 = cummin(Number2))
  new_th4 <- data$th4$
    update_by(udb_cum_min(c("min_Number1 = Number1", "min_Number2 = Number2")), by = "X")
  expect_equal(as.data.frame(new_th4), as.data.frame(new_tb4))
  
  new_tb5 <- data$df5 %>%
    group_by(Y) %>%
    mutate(min_Number1 = cummin(Number1), min_Number2 = cummin(Number2))
  new_th5 <- data$th5$
    update_by(udb_cum_min(c("min_Number1 = Number1", "min_Number2 = Number2")), by = "Y")
  expect_equal(as.data.frame(new_th5), as.data.frame(new_tb5))
  
  new_tb6 <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
    group_by(X, Y) %>%
    mutate(min_Number1 = cummin(Number1), min_Number2 = cummin(Number2))
  new_th6 <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(udb_cum_min(c("min_Number1 = Number1", "min_Number2 = Number2")), by = c("X", "Y"))
  expect_equal(as.data.frame(new_th6), as.data.frame(new_tb6))
  
  data$client$close()
})

test_that("udb_cum_max behaves as expected", {
  data <- setup()
  
  new_tb1 <- data$df1 %>%
    mutate(max_int_col = cummax(int_col))
  new_th1 <- data$th1$
    update_by(udb_cum_max("max_int_col = int_col"))
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))
  
  new_tb2 <- data$df2 %>%
    mutate(max_col1 = cummax(col1), max_col3 = cummax(col3))
  new_th2 <- data$th2$
    update_by(udb_cum_max(c("max_col1 = col1", "max_col3 = col3")))
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))
  
  new_tb3 <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(max_int_col = cummax(int_col))
  new_th3 <- data$th3$
    update_by(udb_cum_max("max_int_col = int_col"), by = "bool_col")
  expect_equal(as.data.frame(new_th3), as.data.frame(new_tb3))
  
  new_tb4 <- data$df4 %>%
    group_by(X) %>%
    mutate(max_Number1 = cummax(Number1), max_Number2 = cummax(Number2))
  new_th4 <- data$th4$
    update_by(udb_cum_max(c("max_Number1 = Number1", "max_Number2 = Number2")), by = "X")
  expect_equal(as.data.frame(new_th4), as.data.frame(new_tb4))
  
  new_tb5 <- data$df5 %>%
    group_by(Y) %>%
    mutate(max_Number1 = cummax(Number1), max_Number2 = cummax(Number2))
  new_th5 <- data$th5$
    update_by(udb_cum_max(c("max_Number1 = Number1", "max_Number2 = Number2")), by = "Y")
  expect_equal(as.data.frame(new_th5), as.data.frame(new_tb5))
  
  new_tb6 <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
    group_by(X, Y) %>%
    mutate(max_Number1 = cummax(Number1), max_Number2 = cummax(Number2))
  new_th6 <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(udb_cum_max(c("max_Number1 = Number1", "max_Number2 = Number2")), by = c("X", "Y"))
  expect_equal(as.data.frame(new_th6), as.data.frame(new_tb6))
  
  data$client$close()
})

# TODO: This
test_that("udb_forward_fill behaves as expected", {
  data <- setup()
  data$client$close()
})

# TODO: This
test_that("udb_delta behaves as expected", {
  data <- setup()
  data$client$close()
})

test_that("udb_ema_tick behaves as expected", {
  data <- setup()
  
  custom_ema <- function(decay_ticks, x) {
    if (length(x) == 1) {
      return(x)
    }
    a = exp(-1/decay_ticks)
    ema = c(x[1])
    for(i in seq(2,length(x))) {
      ema[i] = a*ema[i-1] + (1-a)*x[i]
    }
    return(ema)
  }
  
  new_tb1 <- data$df1 %>%
    mutate(dbl_col = custom_ema(2, dbl_col))
  new_th1 <- data$th1$
    update_by(udb_ema_tick(2, "dbl_col"))
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))
  
  new_tb2 <- data$df2 %>%
    mutate(col1 = custom_ema(5, col1), col3 = custom_ema(5, col3))
  new_th2 <- data$th2$
    update_by(udb_ema_tick(5, c("col1", "col3")))
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))
  
  new_tb3 <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(ema_int_col = custom_ema(9, int_col))
  new_th3 <- data$th3$
    update_by(udb_ema_tick(9, "ema_int_col = int_col"), by = "bool_col")
  expect_equal(as.data.frame(new_th3), as.data.frame(new_tb3))
  
  new_tb4 <- data$df4 %>%
    group_by(X) %>%
    mutate(ema_Number1 = custom_ema(3, Number1), ema_Number2 = custom_ema(3, Number2))
  new_th4 <- data$th4$
    update_by(udb_ema_tick(3, c("ema_Number1 = Number1", "ema_Number2 = Number2")), by = "X")
  expect_equal(as.data.frame(new_th4), as.data.frame(new_tb4))
  
  new_tb5 <- data$df5 %>%
    group_by(Y) %>%
    mutate(ema_Number1 = custom_ema(3, Number1), ema_Number2 = custom_ema(3, Number2))
  new_th5 <- data$th5$
    update_by(udb_ema_tick(3, c("ema_Number1 = Number1", "ema_Number2 = Number2")), by = "Y")
  expect_equal(as.data.frame(new_th5), as.data.frame(new_tb5))

  new_tb6 <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
    group_by(X, Y) %>%
    mutate(ema_Number1 = custom_ema(3, Number1), ema_Number2 = custom_ema(3, Number2))
  new_th6 <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(udb_ema_tick(3, c("ema_Number1 = Number1", "ema_Number2 = Number2")), by = c("X", "Y"))
  expect_equal(as.data.frame(new_th6), as.data.frame(new_tb6))
  
  data$client$close()
})

test_that("udb_ema_time behaves as expected", {
  data <- setup()
  
  custom_ema_time <- function(ts, decay_time, x) {
    if (length(x) == 1) {
      return(x)
    }
    time_diffs = as.numeric(ts[2:length(ts)] - ts[1:length(ts)-1])
    a = exp(-time_diffs/as.numeric(duration(decay_time)))
    ema = c(x[1])
    for(i in seq(2,length(x))) {
      ema[i] = a[i-1]*ema[i-1] + (1-a[i-1])*x[i]
    }
    return(ema)
  }
  
  new_tb1 <- data$df3 %>%
    mutate(ema_int_col = custom_ema_time(time_col, "PT3s", int_col))
  new_th1 <- data$th3$
    update_by(udb_ema_time("time_col", "PT3s", "ema_int_col = int_col"))
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))
  
  new_tb2 <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(ema_int_col = custom_ema_time(time_col, "PT3s", int_col))
  new_th2 <- data$th3$
    update_by(udb_ema_time("time_col", "PT3s", "ema_int_col = int_col"), by = "bool_col")
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))
      
  data$client$close()
})

test_that("udb_ems_tick behaves as expected", {
  data <- setup()
  
  custom_ems <- function(decay_ticks, x) {
    if (length(x) == 1) {
      return(x)
    }
    a = exp(-1/decay_ticks)
    ems = c(x[1])
    for(i in seq(2,length(x))) {
      ems[i] = a*ems[i-1] + x[i]
    }
    return(ems)
  }
  
  new_tb1 <- data$df1 %>%
    mutate(dbl_col = custom_ems(2, dbl_col))
  new_th1 <- data$th1$
    update_by(udb_ems_tick(2, "dbl_col"))
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))
  
  new_tb2 <- data$df2 %>%
    mutate(col1 = custom_ems(5, col1), col3 = custom_ems(5, col3))
  new_th2 <- data$th2$
    update_by(udb_ems_tick(5, c("col1", "col3")))
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))
  
  new_tb3 <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(ems_int_col = custom_ems(9, int_col))
  new_th3 <- data$th3$
    update_by(udb_ems_tick(9, "ems_int_col = int_col"), by = "bool_col")
  expect_equal(as.data.frame(new_th3), as.data.frame(new_tb3))
  
  new_tb4 <- data$df4 %>%
    group_by(X) %>%
    mutate(ems_Number1 = custom_ems(3, Number1), ems_Number2 = custom_ems(3, Number2))
  new_th4 <- data$th4$
    update_by(udb_ems_tick(3, c("ems_Number1 = Number1", "ems_Number2 = Number2")), by = "X")
  expect_equal(as.data.frame(new_th4), as.data.frame(new_tb4))
  
  new_tb5 <- data$df5 %>%
    group_by(Y) %>%
    mutate(ems_Number1 = custom_ems(3, Number1), ems_Number2 = custom_ems(3, Number2))
  new_th5 <- data$th5$
    update_by(udb_ems_tick(3, c("ems_Number1 = Number1", "ems_Number2 = Number2")), by = "Y")
  expect_equal(as.data.frame(new_th5), as.data.frame(new_tb5))
  
  new_tb6 <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
    group_by(X, Y) %>%
    mutate(ems_Number1 = custom_ems(3, Number1), ems_Number2 = custom_ems(3, Number2))
  new_th6 <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(udb_ems_tick(3, c("ems_Number1 = Number1", "ems_Number2 = Number2")), by = c("X", "Y"))
  expect_equal(as.data.frame(new_th6), as.data.frame(new_tb6))
  
  data$client$close()
})

test_that("udb_ems_time behaves as expected", {
  data <- setup()
  
  custom_ems_time <- function(ts, decay_time, x) {
    if (length(x) == 1) {
      return(x)
    }
    time_diffs = as.numeric(ts[2:length(ts)] - ts[1:length(ts)-1])
    a = exp(-time_diffs/as.numeric(duration(decay_time)))
    ems = c(x[1])
    for(i in seq(2,length(x))) {
      ems[i] = a[i-1]*ems[i-1] + x[i]
    }
    return(ems)
  }
  
  new_tb1 <- data$df3 %>%
    mutate(ems_int_col = custom_ems_time(time_col, "PT3s", int_col))
  new_th1 <- data$th3$
    update_by(udb_ems_time("time_col", "PT3s", "ems_int_col = int_col"))
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))
  
  new_tb2 <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(ems_int_col = custom_ems_time(time_col, "PT3s", int_col))
  new_th2 <- data$th3$
    update_by(udb_ems_time("time_col", "PT3s", "ems_int_col = int_col"), by = "bool_col")
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))
  
  data$client$close()
})

test_that("udb_emmin_tick behaves as expected", {
  data <- setup()
  
  custom_emmin <- function(decay_ticks, x) {
    if (length(x) == 1) {
      return(x)
    }
    a = exp(-1/decay_ticks)
    emmin = c(x[1])
    for(i in seq(2,length(x))) {
      emmin[i] = min(a*emmin[i-1], x[i])
    }
    return(emmin)
  }
  
  new_tb1 <- data$df1 %>%
    mutate(dbl_col = custom_emmin(2, dbl_col))
  new_th1 <- data$th1$
    update_by(udb_emmin_tick(2, "dbl_col"))
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))
  
  new_tb2 <- data$df2 %>%
    mutate(col1 = custom_emmin(5, col1), col3 = custom_emmin(5, col3))
  new_th2 <- data$th2$
    update_by(udb_emmin_tick(5, c("col1", "col3")))
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))
  
  new_tb3 <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(emmin_int_col = custom_emmin(9, int_col))
  new_th3 <- data$th3$
    update_by(udb_emmin_tick(9, "emmin_int_col = int_col"), by = "bool_col")
  expect_equal(as.data.frame(new_th3), as.data.frame(new_tb3))
  
  new_tb4 <- data$df4 %>%
    group_by(X) %>%
    mutate(emmin_Number1 = custom_emmin(3, Number1), emmin_Number2 = custom_emmin(3, Number2))
  new_th4 <- data$th4$
    update_by(udb_emmin_tick(3, c("emmin_Number1 = Number1", "emmin_Number2 = Number2")), by = "X")
  expect_equal(as.data.frame(new_th4), as.data.frame(new_tb4))
  
  new_tb5 <- data$df5 %>%
    group_by(Y) %>%
    mutate(emmin_Number1 = custom_emmin(3, Number1), emmin_Number2 = custom_emmin(3, Number2))
  new_th5 <- data$th5$
    update_by(udb_emmin_tick(3, c("emmin_Number1 = Number1", "emmin_Number2 = Number2")), by = "Y")
  expect_equal(as.data.frame(new_th5), as.data.frame(new_tb5))
  
  new_tb6 <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
    group_by(X, Y) %>%
    mutate(emmin_Number1 = custom_emmin(3, Number1), emmin_Number2 = custom_emmin(3, Number2))
  new_th6 <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(udb_emmin_tick(3, c("emmin_Number1 = Number1", "emmin_Number2 = Number2")), by = c("X", "Y"))
  expect_equal(as.data.frame(new_th6), as.data.frame(new_tb6))
  
  data$client$close()
})

test_that("udb_emmin_time behaves as expected", {
  data <- setup()
  
  custom_emmin_time <- function(ts, decay_time, x) {
    if (length(x) == 1) {
      return(x)
    }
    time_diffs = as.numeric(ts[2:length(ts)] - ts[1:length(ts)-1])
    a = exp(-time_diffs/as.numeric(duration(decay_time)))
    emmin = c(x[1])
    for(i in seq(2,length(x))) {
      emmin[i] = min(a[i-1]*emmin[i-1], x[i])
    }
    return(emmin)
  }
  
  new_tb1 <- data$df3 %>%
    mutate(emmin_int_col = custom_emmin_time(time_col, "PT3s", int_col))
  new_th1 <- data$th3$
    update_by(udb_emmin_time("time_col", "PT3s", "emmin_int_col = int_col"))
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))
  
  new_tb2 <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(emmin_int_col = custom_emmin_time(time_col, "PT3s", int_col))
  new_th2 <- data$th3$
    update_by(udb_emmin_time("time_col", "PT3s", "emmin_int_col = int_col"), by = "bool_col")
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))
  
  data$client$close()
})

test_that("udb_emmax_tick behaves as expected", {
  data <- setup()
  
  custom_emmax <- function(decay_ticks, x) {
    if (length(x) == 1) {
      return(x)
    }
    a = exp(-1/decay_ticks)
    emmax = c(x[1])
    for(i in seq(2,length(x))) {
      emmax[i] = max(a*emmax[i-1], x[i])
    }
    return(emmax)
  }
  
  new_tb1 <- data$df1 %>%
    mutate(dbl_col = custom_emmax(2, dbl_col))
  new_th1 <- data$th1$
    update_by(udb_emmax_tick(2, "dbl_col"))
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))
  
  new_tb2 <- data$df2 %>%
    mutate(col1 = custom_emmax(5, col1), col3 = custom_emmax(5, col3))
  new_th2 <- data$th2$
    update_by(udb_emmax_tick(5, c("col1", "col3")))
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))
  
  new_tb3 <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(emmax_int_col = custom_emmax(9, int_col))
  new_th3 <- data$th3$
    update_by(udb_emmax_tick(9, "emmax_int_col = int_col"), by = "bool_col")
  expect_equal(as.data.frame(new_th3), as.data.frame(new_tb3))
  
  new_tb4 <- data$df4 %>%
    group_by(X) %>%
    mutate(emmax_Number1 = custom_emmax(3, Number1), emmax_Number2 = custom_emmax(3, Number2))
  new_th4 <- data$th4$
    update_by(udb_emmax_tick(3, c("emmax_Number1 = Number1", "emmax_Number2 = Number2")), by = "X")
  expect_equal(as.data.frame(new_th4), as.data.frame(new_tb4))
  
  new_tb5 <- data$df5 %>%
    group_by(Y) %>%
    mutate(emmax_Number1 = custom_emmax(3, Number1), emmax_Number2 = custom_emmax(3, Number2))
  new_th5 <- data$th5$
    update_by(udb_emmax_tick(3, c("emmax_Number1 = Number1", "emmax_Number2 = Number2")), by = "Y")
  expect_equal(as.data.frame(new_th5), as.data.frame(new_tb5))
  
  new_tb6 <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
    group_by(X, Y) %>%
    mutate(emmax_Number1 = custom_emmax(3, Number1), emmax_Number2 = custom_emmax(3, Number2))
  new_th6 <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(udb_emmax_tick(3, c("emmax_Number1 = Number1", "emmax_Number2 = Number2")), by = c("X", "Y"))
  expect_equal(as.data.frame(new_th6), as.data.frame(new_tb6))
  
  data$client$close()
})

test_that("udb_emmax_time behaves as expected", {
  data <- setup()
  
  custom_emmax_time <- function(ts, decay_time, x) {
    if (length(x) == 1) {
      return(x)
    }
    time_diffs = as.numeric(ts[2:length(ts)] - ts[1:length(ts)-1])
    a = exp(-time_diffs/as.numeric(duration(decay_time)))
    emmax = c(x[1])
    for(i in seq(2,length(x))) {
      emmax[i] = max(a[i-1]*emmax[i-1], x[i])
    }
    return(emmax)
  }
  
  new_tb1 <- data$df3 %>%
    mutate(emmax_int_col = custom_emmax_time(time_col, "PT3s", int_col))
  new_th1 <- data$th3$
    update_by(udb_emmax_time("time_col", "PT3s", "emmax_int_col = int_col"))
  expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))
  
  new_tb2 <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(emmax_int_col = custom_emmax_time(time_col, "PT3s", int_col))
  new_th2 <- data$th3$
    update_by(udb_emmax_time("time_col", "PT3s", "emmax_int_col = int_col"), by = "bool_col")
  expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))
  
  data$client$close()
})

# TODO: Figure out formula for emstd
# test_that("udb_emstd_tick behaves as expected", {
#   data <- setup()
#   
#   custom_emstd <- function(decay_ticks, x) {
#   }
#   
#   new_tb1 <- data$df1 %>%
#     mutate(dbl_col = custom_emstd(2, dbl_col))
#   new_th1 <- data$th1$
#     update_by(udb_emstd_tick(2, "dbl_col"))
#   expect_equal(as.data.frame(new_th1), as.data.frame(new_tb1))
#   
#   new_tb2 <- data$df2 %>%
#     mutate(col1 = custom_emstd(5, col1), col3 = custom_emstd(5, col3))
#   new_th2 <- data$th2$
#     update_by(udb_emstd_tick(5, c("col1", "col3")))
#   expect_equal(as.data.frame(new_th2), as.data.frame(new_tb2))
#   
#   new_tb3 <- data$df3 %>%
#     group_by(bool_col) %>%
#     mutate(emstd_int_col = custom_emstd(9, int_col))
#   new_th3 <- data$th3$
#     update_by(udb_emstd_tick(9, "emstd_int_col = int_col"), by = "bool_col")
#   expect_equal(as.data.frame(new_th3), as.data.frame(new_tb3))
#   
#   new_tb4 <- data$df4 %>%
#     group_by(X) %>%
#     mutate(emstd_Number1 = custom_emstd(3, Number1), emstd_Number2 = custom_emstd(3, Number2))
#   new_th4 <- data$th4$
#     update_by(udb_emstd_tick(3, c("emstd_Number1 = Number1", "emstd_Number2 = Number2")), by = "X")
#   expect_equal(as.data.frame(new_th4), as.data.frame(new_tb4))
#   
#   new_tb5 <- data$df5 %>%
#     group_by(Y) %>%
#     mutate(emstd_Number1 = custom_emstd(3, Number1), emstd_Number2 = custom_emstd(3, Number2))
#   new_th5 <- data$th5$
#     update_by(udb_emstd_tick(3, c("emstd_Number1 = Number1", "emstd_Number2 = Number2")), by = "Y")
#   expect_equal(as.data.frame(new_th5), as.data.frame(new_tb5))
#   
#   new_tb6 <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
#     group_by(X, Y) %>%
#     mutate(emstd_Number1 = custom_emstd(3, Number1), emstd_Number2 = custom_emstd(3, Number2))
#   new_th6 <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
#     update_by(udb_emstd_tick(3, c("emstd_Number1 = Number1", "emstd_Number2 = Number2")), by = c("X", "Y"))
#   expect_equal(as.data.frame(new_th6), as.data.frame(new_tb6))
#   
#   data$client$close()
# })
# 
# test_that("udb_emstd_time behaves as expected", {
#   data <- setup()
#   data$client$close()
# })

test_that("udb_roll_sum_tick behaves as expected", {
  data <- setup()
  
  new_tb1a <- data$df1 %>%
    mutate(dbl_col = rollapply(dbl_col, 3, sum, partial = TRUE, align = "right"))
  new_th1a <- data$th1$
    update_by(udb_roll_sum_tick("dbl_col", rev_ticks = 3))
  expect_equal(as.data.frame(new_th1a), as.data.frame(new_tb1a))
  
  new_tb1b <- data$df1 %>%
    mutate(dbl_col = rollapply(dbl_col, 3, sum, partial = TRUE, align = "left"))
  new_th1b <- data$th1$
    update_by(udb_roll_sum_tick("dbl_col", rev_ticks = 1, fwd_ticks = 2))
  expect_equal(as.data.frame(new_th1b), as.data.frame(new_tb1b))
  
  new_tb1c <- data$df1 %>%
    mutate(dbl_col = rollapply(dbl_col, 3, sum, partial = TRUE, align = "center"))
  new_th1c <- data$th1$
    update_by(udb_roll_sum_tick("dbl_col", rev_ticks = 2, fwd_ticks = 1))
  expect_equal(as.data.frame(new_th1c), as.data.frame(new_tb1c))
  
  new_tb2a <- data$df2 %>%
    mutate(col1 = rollapply(col1, 5, sum, partial = TRUE, align = "right"),
           col3 = rollapply(col3, 5, sum, partial = TRUE, align = "right"))
  new_th2a <- data$th2$
    update_by(udb_roll_sum_tick(c("col1", "col3"), rev_ticks = 5))
  expect_equal(as.data.frame(new_th2a), as.data.frame(new_tb2a))
  
  new_tb2b <- data$df2 %>%
    mutate(col1 = rollapply(col1, 5, sum, partial = TRUE, align = "left"),
           col3 = rollapply(col3, 5, sum, partial = TRUE, align = "left"))
  new_th2b <- data$th2$
    update_by(udb_roll_sum_tick(c("col1", "col3"), rev_ticks = 1, fwd_ticks = 4))
  expect_equal(as.data.frame(new_th2b), as.data.frame(new_tb2b))
  
  new_tb2c <- data$df2 %>%
    mutate(col1 = rollapply(col1, 5, sum, partial = TRUE, align = "center"),
           col3 = rollapply(col3, 5, sum, partial = TRUE, align = "center"))
  new_th2c <- data$th2$
    update_by(udb_roll_sum_tick(c("col1", "col3"), rev_ticks = 3, fwd_ticks = 2))
  expect_equal(as.data.frame(new_th2c), as.data.frame(new_tb2c))
  
  new_tb3a <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(int_col = rollapply(int_col, 9, sum, partial = TRUE, align = "right"))
  new_th3a <- data$th3$
    update_by(udb_roll_sum_tick("int_col", rev_ticks = 9), by = "bool_col")
  expect_equal(as.data.frame(new_th3a), as.data.frame(new_tb3a))
  
  new_tb3b <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(int_col = rollapply(int_col, 9, sum, partial = TRUE, align = "left"))
  new_th3b <- data$th3$
    update_by(udb_roll_sum_tick("int_col", rev_ticks = 1, fwd_ticks = 8), by = "bool_col")
  expect_equal(as.data.frame(new_th3b), as.data.frame(new_tb3b))
  
  new_tb3c <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(int_col = rollapply(int_col, 9, sum, partial = TRUE, align = "center"))
  new_th3c <- data$th3$
    update_by(udb_roll_sum_tick("int_col", rev_ticks = 5, fwd_ticks = 4), by = "bool_col")
  expect_equal(as.data.frame(new_th3c), as.data.frame(new_tb3c))
  
  new_tb4a <- data$df4 %>%
    group_by(X) %>%
    mutate(Number1 = rollapply(Number1, 3, sum, partial = TRUE, align = "right"),
           Number2 = rollapply(Number2, 3, sum, partial = TRUE, align = "right"))
  new_th4a <- data$th4$
    update_by(udb_roll_sum_tick(c("Number1", "Number2"), rev_ticks = 3), by = "X")
  expect_equal(as.data.frame(new_th4a), as.data.frame(new_tb4a))
  
  new_tb4b <- data$df4 %>%
    group_by(X) %>%
    mutate(Number1 = rollapply(Number1, 3, sum, partial = TRUE, align = "left"),
           Number2 = rollapply(Number2, 3, sum, partial = TRUE, align = "left"))
  new_th4b <- data$th4$
    update_by(udb_roll_sum_tick(c("Number1", "Number2"), rev_ticks = 1, fwd_ticks = 2), by = "X")
  expect_equal(as.data.frame(new_th4b), as.data.frame(new_tb4b))
  
  new_tb4c <- data$df4 %>%
    group_by(X) %>%
    mutate(Number1 = rollapply(Number1, 3, sum, partial = TRUE, align = "center"),
           Number2 = rollapply(Number2, 3, sum, partial = TRUE, align = "center"))
  new_th4c <- data$th4$
    update_by(udb_roll_sum_tick(c("Number1", "Number2"), rev_ticks = 2, fwd_ticks = 1), by = "X")
  expect_equal(as.data.frame(new_th4c), as.data.frame(new_tb4c))
  
  new_tb5a <- data$df5 %>%
    group_by(Y) %>%
    mutate(Number1 = rollapply(Number1, 3, sum, partial = TRUE, align = "right"),
           Number2 = rollapply(Number2, 3, sum, partial = TRUE, align = "right"))
  new_th5a <- data$th5$
    update_by(udb_roll_sum_tick(c("Number1", "Number2"), rev_ticks = 3), by = "Y")
  expect_equal(as.data.frame(new_th5a), as.data.frame(new_tb5a))
  
  new_tb5b <- data$df5 %>%
    group_by(Y) %>%
    mutate(Number1 = rollapply(Number1, 3, sum, partial = TRUE, align = "left"),
           Number2 = rollapply(Number2, 3, sum, partial = TRUE, align = "left"))
  new_th5b <- data$th5$
    update_by(udb_roll_sum_tick(c("Number1", "Number2"), rev_ticks = 1, fwd_ticks = 2), by = "Y")
  expect_equal(as.data.frame(new_th5b), as.data.frame(new_tb5b))
  
  new_tb5c <- data$df5 %>%
    group_by(Y) %>%
    mutate(Number1 = rollapply(Number1, 3, sum, partial = TRUE, align = "center"),
           Number2 = rollapply(Number2, 3, sum, partial = TRUE, align = "center"))
  new_th5c <- data$th5$
    update_by(udb_roll_sum_tick(c("Number1", "Number2"), rev_ticks = 2, fwd_ticks = 1), by = "Y")
  expect_equal(as.data.frame(new_th5c), as.data.frame(new_tb5c))
  
  new_tb6a <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
    group_by(X, Y) %>%
    mutate(Number1 = rollapply(Number1, 3, sum, partial = TRUE, align = "right"),
           Number2 = rollapply(Number2, 3, sum, partial = TRUE, align = "right"))
  new_th6a <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(udb_roll_sum_tick(c("Number1", "Number2"), rev_ticks = 3), by = c("X", "Y"))
  expect_equal(as.data.frame(new_th6a), as.data.frame(new_tb6a))
  
  new_tb6b <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
    group_by(X, Y) %>%
    mutate(Number1 = rollapply(Number1, 3, sum, partial = TRUE, align = "left"),
           Number2 = rollapply(Number2, 3, sum, partial = TRUE, align = "left"))
  new_th6b <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(udb_roll_sum_tick(c("Number1", "Number2"), rev_ticks = 1, fwd_ticks = 2), by = c("X", "Y"))
  expect_equal(as.data.frame(new_th6b), as.data.frame(new_tb6b))
  
  new_tb6c <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
    group_by(X, Y) %>%
    mutate(Number1 = rollapply(Number1, 3, sum, partial = TRUE, align = "center"),
           Number2 = rollapply(Number2, 3, sum, partial = TRUE, align = "center"))
  new_th6c <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(udb_roll_sum_tick(c("Number1", "Number2"), rev_ticks = 2, fwd_ticks = 1), by = c("X", "Y"))
  expect_equal(as.data.frame(new_th6c), as.data.frame(new_tb6c))
  
  data$client$close()
})

test_that("udb_roll_sum_time behaves as expected", {
  data <- setup()
  data$client$close()
})

test_that("udb_roll_group_tick behaves as expected", {
  data <- setup()
  
  right_group <- list(1.65,
                    c(1.6500, 3.1234),
                    c(1.6500, 3.1234, 100000.5000),
                    c(3.1234, 100000.5000, 543.234567),
                    c(100000.5000, 543.234567, 0.0000))
  new_th1a <- data$th1$
    update_by(udb_roll_group_tick("dbl_col_group = dbl_col", rev_ticks = 3))
  expect_equal(as.list(as.data.frame(new_th1a)$dbl_col_group), right_group)
  
  
  left_group <- list(c(1.6500, 3.1234, 100000.5000),
                     c(3.1234, 100000.5000, 543.234567),
                     c(100000.5000, 543.234567, 0.0000),
                     c(543.234567, 0.0000),
                     0)
  new_th1b <- data$th1$
    update_by(udb_roll_group_tick("dbl_col_group = dbl_col", rev_ticks = 1, fwd_ticks = 2))
  expect_equal(as.list(as.data.frame(new_th1b)$dbl_col_group), left_group)
  
  
  center_group <- list(c(1.6500, 3.1234),
                       c(1.6500, 3.1234, 100000.5000),
                       c(3.1234, 100000.5000, 543.234567),
                       c(100000.5000, 543.234567, 0.0000),
                       c(543.234567, 0.0000))
  new_th1c <- data$th1$
    update_by(udb_roll_group_tick("dbl_col_group = dbl_col", rev_ticks = 2, fwd_ticks = 1))
  expect_equal(as.list(as.data.frame(new_th1c)$dbl_col_group), center_group)
  
  data$client$close()
})

test_that("udb_roll_group_time behaves as expected", {
  data <- setup()
  data$client$close()
})

test_that("udb_roll_avg_tick behaves as expected", {
  data <- setup()
  
  new_tb1a <- data$df1 %>%
    mutate(dbl_col = rollapply(dbl_col, 3, mean, partial = TRUE, align = "right"))
  new_th1a <- data$th1$
    update_by(udb_roll_avg_tick("dbl_col", rev_ticks = 3))
  expect_equal(as.data.frame(new_th1a), as.data.frame(new_tb1a))
  
  new_tb1b <- data$df1 %>%
    mutate(dbl_col = rollapply(dbl_col, 3, mean, partial = TRUE, align = "left"))
  new_th1b <- data$th1$
    update_by(udb_roll_avg_tick("dbl_col", rev_ticks = 1, fwd_ticks = 2))
  expect_equal(as.data.frame(new_th1b), as.data.frame(new_tb1b))
  
  new_tb1c <- data$df1 %>%
    mutate(dbl_col = rollapply(dbl_col, 3, mean, partial = TRUE, align = "center"))
  new_th1c <- data$th1$
    update_by(udb_roll_avg_tick("dbl_col", rev_ticks = 2, fwd_ticks = 1))
  expect_equal(as.data.frame(new_th1c), as.data.frame(new_tb1c))
  
  new_tb2a <- data$df2 %>%
    mutate(col1 = rollapply(col1, 5, mean, partial = TRUE, align = "right"),
           col3 = rollapply(col3, 5, mean, partial = TRUE, align = "right"))
  new_th2a <- data$th2$
    update_by(udb_roll_avg_tick(c("col1", "col3"), rev_ticks = 5))
  expect_equal(as.data.frame(new_th2a), as.data.frame(new_tb2a))
  
  new_tb2b <- data$df2 %>%
    mutate(col1 = rollapply(col1, 5, mean, partial = TRUE, align = "left"),
           col3 = rollapply(col3, 5, mean, partial = TRUE, align = "left"))
  new_th2b <- data$th2$
    update_by(udb_roll_avg_tick(c("col1", "col3"), rev_ticks = 1, fwd_ticks = 4))
  expect_equal(as.data.frame(new_th2b), as.data.frame(new_tb2b))
  
  new_tb2c <- data$df2 %>%
    mutate(col1 = rollapply(col1, 5, mean, partial = TRUE, align = "center"),
           col3 = rollapply(col3, 5, mean, partial = TRUE, align = "center"))
  new_th2c <- data$th2$
    update_by(udb_roll_avg_tick(c("col1", "col3"), rev_ticks = 3, fwd_ticks = 2))
  expect_equal(as.data.frame(new_th2c), as.data.frame(new_tb2c))
  
  new_tb3a <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(int_col = rollapply(int_col, 9, mean, partial = TRUE, align = "right"))
  new_th3a <- data$th3$
    update_by(udb_roll_avg_tick("int_col", rev_ticks = 9), by = "bool_col")
  expect_equal(as.data.frame(new_th3a), as.data.frame(new_tb3a))
  
  new_tb3b <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(int_col = rollapply(int_col, 9, mean, partial = TRUE, align = "left"))
  new_th3b <- data$th3$
    update_by(udb_roll_avg_tick("int_col", rev_ticks = 1, fwd_ticks = 8), by = "bool_col")
  expect_equal(as.data.frame(new_th3b), as.data.frame(new_tb3b))
  
  new_tb3c <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(int_col = rollapply(int_col, 9, mean, partial = TRUE, align = "center"))
  new_th3c <- data$th3$
    update_by(udb_roll_avg_tick("int_col", rev_ticks = 5, fwd_ticks = 4), by = "bool_col")
  expect_equal(as.data.frame(new_th3c), as.data.frame(new_tb3c))
  
  new_tb4a <- data$df4 %>%
    group_by(X) %>%
    mutate(Number1 = rollapply(Number1, 3, mean, partial = TRUE, align = "right"),
           Number2 = rollapply(Number2, 3, mean, partial = TRUE, align = "right"))
  new_th4a <- data$th4$
    update_by(udb_roll_avg_tick(c("Number1", "Number2"), rev_ticks = 3), by = "X")
  expect_equal(as.data.frame(new_th4a), as.data.frame(new_tb4a))
  
  new_tb4b <- data$df4 %>%
    group_by(X) %>%
    mutate(Number1 = rollapply(Number1, 3, mean, partial = TRUE, align = "left"),
           Number2 = rollapply(Number2, 3, mean, partial = TRUE, align = "left"))
  new_th4b <- data$th4$
    update_by(udb_roll_avg_tick(c("Number1", "Number2"), rev_ticks = 1, fwd_ticks = 2), by = "X")
  expect_equal(as.data.frame(new_th4b), as.data.frame(new_tb4b))
  
  new_tb4c <- data$df4 %>%
    group_by(X) %>%
    mutate(Number1 = rollapply(Number1, 3, mean, partial = TRUE, align = "center"),
           Number2 = rollapply(Number2, 3, mean, partial = TRUE, align = "center"))
  new_th4c <- data$th4$
    update_by(udb_roll_avg_tick(c("Number1", "Number2"), rev_ticks = 2, fwd_ticks = 1), by = "X")
  expect_equal(as.data.frame(new_th4c), as.data.frame(new_tb4c))
  
  new_tb5a <- data$df5 %>%
    group_by(Y) %>%
    mutate(Number1 = rollapply(Number1, 3, mean, partial = TRUE, align = "right"),
           Number2 = rollapply(Number2, 3, mean, partial = TRUE, align = "right"))
  new_th5a <- data$th5$
    update_by(udb_roll_avg_tick(c("Number1", "Number2"), rev_ticks = 3), by = "Y")
  expect_equal(as.data.frame(new_th5a), as.data.frame(new_tb5a))
  
  new_tb5b <- data$df5 %>%
    group_by(Y) %>%
    mutate(Number1 = rollapply(Number1, 3, mean, partial = TRUE, align = "left"),
           Number2 = rollapply(Number2, 3, mean, partial = TRUE, align = "left"))
  new_th5b <- data$th5$
    update_by(udb_roll_avg_tick(c("Number1", "Number2"), rev_ticks = 1, fwd_ticks = 2), by = "Y")
  expect_equal(as.data.frame(new_th5b), as.data.frame(new_tb5b))
  
  new_tb5c <- data$df5 %>%
    group_by(Y) %>%
    mutate(Number1 = rollapply(Number1, 3, mean, partial = TRUE, align = "center"),
           Number2 = rollapply(Number2, 3, mean, partial = TRUE, align = "center"))
  new_th5c <- data$th5$
    update_by(udb_roll_avg_tick(c("Number1", "Number2"), rev_ticks = 2, fwd_ticks = 1), by = "Y")
  expect_equal(as.data.frame(new_th5c), as.data.frame(new_tb5c))
  
  new_tb6a <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
    group_by(X, Y) %>%
    mutate(Number1 = rollapply(Number1, 3, mean, partial = TRUE, align = "right"),
           Number2 = rollapply(Number2, 3, mean, partial = TRUE, align = "right"))
  new_th6a <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(udb_roll_avg_tick(c("Number1", "Number2"), rev_ticks = 3), by = c("X", "Y"))
  expect_equal(as.data.frame(new_th6a), as.data.frame(new_tb6a))
  
  new_tb6b <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
    group_by(X, Y) %>%
    mutate(Number1 = rollapply(Number1, 3, mean, partial = TRUE, align = "left"),
           Number2 = rollapply(Number2, 3, mean, partial = TRUE, align = "left"))
  new_th6b <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(udb_roll_avg_tick(c("Number1", "Number2"), rev_ticks = 1, fwd_ticks = 2), by = c("X", "Y"))
  expect_equal(as.data.frame(new_th6b), as.data.frame(new_tb6b))
  
  new_tb6c <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
    group_by(X, Y) %>%
    mutate(Number1 = rollapply(Number1, 3, mean, partial = TRUE, align = "center"),
           Number2 = rollapply(Number2, 3, mean, partial = TRUE, align = "center"))
  new_th6c <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(udb_roll_avg_tick(c("Number1", "Number2"), rev_ticks = 2, fwd_ticks = 1), by = c("X", "Y"))
  expect_equal(as.data.frame(new_th6c), as.data.frame(new_tb6c))
  
  data$client$close()
})

test_that("udb_roll_avg_time behaves as expected", {
  data <- setup()
  data$client$close()
})

test_that("udb_roll_min_tick behaves as expected", {
  data <- setup()
  
  new_tb1a <- data$df1 %>%
    mutate(dbl_col = rollapply(dbl_col, 3, min, partial = TRUE, align = "right"))
  new_th1a <- data$th1$
    update_by(udb_roll_min_tick("dbl_col", rev_ticks = 3))
  expect_equal(as.data.frame(new_th1a), as.data.frame(new_tb1a))
  
  new_tb1b <- data$df1 %>%
    mutate(dbl_col = rollapply(dbl_col, 3, min, partial = TRUE, align = "left"))
  new_th1b <- data$th1$
    update_by(udb_roll_min_tick("dbl_col", rev_ticks = 1, fwd_ticks = 2))
  expect_equal(as.data.frame(new_th1b), as.data.frame(new_tb1b))
  
  new_tb1c <- data$df1 %>%
    mutate(dbl_col = rollapply(dbl_col, 3, min, partial = TRUE, align = "center"))
  new_th1c <- data$th1$
    update_by(udb_roll_min_tick("dbl_col", rev_ticks = 2, fwd_ticks = 1))
  expect_equal(as.data.frame(new_th1c), as.data.frame(new_tb1c))
  
  new_tb2a <- data$df2 %>%
    mutate(col1 = rollapply(col1, 5, min, partial = TRUE, align = "right"),
           col3 = rollapply(col3, 5, min, partial = TRUE, align = "right"))
  new_th2a <- data$th2$
    update_by(udb_roll_min_tick(c("col1", "col3"), rev_ticks = 5))
  expect_equal(as.data.frame(new_th2a), as.data.frame(new_tb2a))
  
  new_tb2b <- data$df2 %>%
    mutate(col1 = rollapply(col1, 5, min, partial = TRUE, align = "left"),
           col3 = rollapply(col3, 5, min, partial = TRUE, align = "left"))
  new_th2b <- data$th2$
    update_by(udb_roll_min_tick(c("col1", "col3"), rev_ticks = 1, fwd_ticks = 4))
  expect_equal(as.data.frame(new_th2b), as.data.frame(new_tb2b))
  
  new_tb2c <- data$df2 %>%
    mutate(col1 = rollapply(col1, 5, min, partial = TRUE, align = "center"),
           col3 = rollapply(col3, 5, min, partial = TRUE, align = "center"))
  new_th2c <- data$th2$
    update_by(udb_roll_min_tick(c("col1", "col3"), rev_ticks = 3, fwd_ticks = 2))
  expect_equal(as.data.frame(new_th2c), as.data.frame(new_tb2c))
  
  new_tb3a <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(int_col = rollapply(int_col, 9, min, partial = TRUE, align = "right"))
  new_th3a <- data$th3$
    update_by(udb_roll_min_tick("int_col", rev_ticks = 9), by = "bool_col")
  expect_equal(as.data.frame(new_th3a), as.data.frame(new_tb3a))
  
  new_tb3b <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(int_col = rollapply(int_col, 9, min, partial = TRUE, align = "left"))
  new_th3b <- data$th3$
    update_by(udb_roll_min_tick("int_col", rev_ticks = 1, fwd_ticks = 8), by = "bool_col")
  expect_equal(as.data.frame(new_th3b), as.data.frame(new_tb3b))
  
  new_tb3c <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(int_col = rollapply(int_col, 9, min, partial = TRUE, align = "center"))
  new_th3c <- data$th3$
    update_by(udb_roll_min_tick("int_col", rev_ticks = 5, fwd_ticks = 4), by = "bool_col")
  expect_equal(as.data.frame(new_th3c), as.data.frame(new_tb3c))
  
  new_tb4a <- data$df4 %>%
    group_by(X) %>%
    mutate(Number1 = rollapply(Number1, 3, min, partial = TRUE, align = "right"),
           Number2 = rollapply(Number2, 3, min, partial = TRUE, align = "right"))
  new_th4a <- data$th4$
    update_by(udb_roll_min_tick(c("Number1", "Number2"), rev_ticks = 3), by = "X")
  expect_equal(as.data.frame(new_th4a), as.data.frame(new_tb4a))
  
  new_tb4b <- data$df4 %>%
    group_by(X) %>%
    mutate(Number1 = rollapply(Number1, 3, min, partial = TRUE, align = "left"),
           Number2 = rollapply(Number2, 3, min, partial = TRUE, align = "left"))
  new_th4b <- data$th4$
    update_by(udb_roll_min_tick(c("Number1", "Number2"), rev_ticks = 1, fwd_ticks = 2), by = "X")
  expect_equal(as.data.frame(new_th4b), as.data.frame(new_tb4b))
  
  new_tb4c <- data$df4 %>%
    group_by(X) %>%
    mutate(Number1 = rollapply(Number1, 3, min, partial = TRUE, align = "center"),
           Number2 = rollapply(Number2, 3, min, partial = TRUE, align = "center"))
  new_th4c <- data$th4$
    update_by(udb_roll_min_tick(c("Number1", "Number2"), rev_ticks = 2, fwd_ticks = 1), by = "X")
  expect_equal(as.data.frame(new_th4c), as.data.frame(new_tb4c))
  
  new_tb5a <- data$df5 %>%
    group_by(Y) %>%
    mutate(Number1 = rollapply(Number1, 3, min, partial = TRUE, align = "right"),
           Number2 = rollapply(Number2, 3, min, partial = TRUE, align = "right"))
  new_th5a <- data$th5$
    update_by(udb_roll_min_tick(c("Number1", "Number2"), rev_ticks = 3), by = "Y")
  expect_equal(as.data.frame(new_th5a), as.data.frame(new_tb5a))
  
  new_tb5b <- data$df5 %>%
    group_by(Y) %>%
    mutate(Number1 = rollapply(Number1, 3, min, partial = TRUE, align = "left"),
           Number2 = rollapply(Number2, 3, min, partial = TRUE, align = "left"))
  new_th5b <- data$th5$
    update_by(udb_roll_min_tick(c("Number1", "Number2"), rev_ticks = 1, fwd_ticks = 2), by = "Y")
  expect_equal(as.data.frame(new_th5b), as.data.frame(new_tb5b))
  
  new_tb5c <- data$df5 %>%
    group_by(Y) %>%
    mutate(Number1 = rollapply(Number1, 3, min, partial = TRUE, align = "center"),
           Number2 = rollapply(Number2, 3, min, partial = TRUE, align = "center"))
  new_th5c <- data$th5$
    update_by(udb_roll_min_tick(c("Number1", "Number2"), rev_ticks = 2, fwd_ticks = 1), by = "Y")
  expect_equal(as.data.frame(new_th5c), as.data.frame(new_tb5c))
  
  new_tb6a <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
    group_by(X, Y) %>%
    mutate(Number1 = rollapply(Number1, 3, min, partial = TRUE, align = "right"),
           Number2 = rollapply(Number2, 3, min, partial = TRUE, align = "right"))
  new_th6a <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(udb_roll_min_tick(c("Number1", "Number2"), rev_ticks = 3), by = c("X", "Y"))
  expect_equal(as.data.frame(new_th6a), as.data.frame(new_tb6a))
  
  new_tb6b <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
    group_by(X, Y) %>%
    mutate(Number1 = rollapply(Number1, 3, min, partial = TRUE, align = "left"),
           Number2 = rollapply(Number2, 3, min, partial = TRUE, align = "left"))
  new_th6b <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(udb_roll_min_tick(c("Number1", "Number2"), rev_ticks = 1, fwd_ticks = 2), by = c("X", "Y"))
  expect_equal(as.data.frame(new_th6b), as.data.frame(new_tb6b))
  
  new_tb6c <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
    group_by(X, Y) %>%
    mutate(Number1 = rollapply(Number1, 3, min, partial = TRUE, align = "center"),
           Number2 = rollapply(Number2, 3, min, partial = TRUE, align = "center"))
  new_th6c <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(udb_roll_min_tick(c("Number1", "Number2"), rev_ticks = 2, fwd_ticks = 1), by = c("X", "Y"))
  expect_equal(as.data.frame(new_th6c), as.data.frame(new_tb6c))
  
  data$client$close()
})

test_that("udb_roll_min_time behaves as expected", {
  data <- setup()
  data$client$close()
})

# TODO: Wait for bug fix
# test_that("udb_roll_max_tick behaves as expected", {
#   data <- setup()
#   
#   new_tb1a <- data$df1 %>%
#     mutate(dbl_col = rollapply(dbl_col, 3, max, partial = TRUE, align = "right"))
#   new_th1a <- data$th1$
#     update_by(udb_roll_max_tick("dbl_col", rev_ticks = 3))
#   expect_equal(as.data.frame(new_th1a), as.data.frame(new_tb1a))
#   
#   new_tb1b <- data$df1 %>%
#     mutate(dbl_col = rollapply(dbl_col, 3, max, partial = TRUE, align = "left"))
#   new_th1b <- data$th1$
#     update_by(udb_roll_max_tick("dbl_col", rev_ticks = 1, fwd_ticks = 2))
#   expect_equal(as.data.frame(new_th1b), as.data.frame(new_tb1b))
#   
#   new_tb1c <- data$df1 %>%
#     mutate(dbl_col = rollapply(dbl_col, 3, max, partial = TRUE, align = "center"))
#   new_th1c <- data$th1$
#     update_by(udb_roll_max_tick("dbl_col", rev_ticks = 2, fwd_ticks = 1))
#   expect_equal(as.data.frame(new_th1c), as.data.frame(new_tb1c))
#   
#   new_tb2a <- data$df2 %>%
#     mutate(col1 = rollapply(col1, 5, max, partial = TRUE, align = "right"),
#            col3 = rollapply(col3, 5, max, partial = TRUE, align = "right"))
#   new_th2a <- data$th2$
#     update_by(udb_roll_max_tick(c("col1", "col3"), rev_ticks = 5))
#   expect_equal(as.data.frame(new_th2a), as.data.frame(new_tb2a))
# 
#   new_tb2b <- data$df2 %>%
#     mutate(col1 = rollapply(col1, 5, max, partial = TRUE, align = "left"),
#            col3 = rollapply(col3, 5, max, partial = TRUE, align = "left"))
#   new_th2b <- data$th2$
#     update_by(udb_roll_max_tick(c("col1", "col3"), rev_ticks = 1, fwd_ticks = 4))
#   expect_equal(as.data.frame(new_th2b), as.data.frame(new_tb2b))
# 
#   new_tb2c <- data$df2 %>%
#     mutate(col1 = rollapply(col1, 5, max, partial = TRUE, align = "center"),
#            col3 = rollapply(col3, 5, max, partial = TRUE, align = "center"))
#   new_th2c <- data$th2$
#     update_by(udb_roll_max_tick(c("col1", "col3"), rev_ticks = 3, fwd_ticks = 2))
#   expect_equal(as.data.frame(new_th2c), as.data.frame(new_tb2c))
#   
#   new_tb3a <- data$df3 %>%
#     group_by(bool_col) %>%
#     mutate(int_col = rollapply(int_col, 9, max, partial = TRUE, align = "right"))
#   new_th3a <- data$th3$
#     update_by(udb_roll_max_tick("int_col", rev_ticks = 9), by = "bool_col")
#   expect_equal(as.data.frame(new_th3a), as.data.frame(new_tb3a))
#   
#   new_tb3b <- data$df3 %>%
#     group_by(bool_col) %>%
#     mutate(int_col = rollapply(int_col, 9, max, partial = TRUE, align = "left"))
#   new_th3b <- data$th3$
#     update_by(udb_roll_max_tick("int_col", rev_ticks = 1, fwd_ticks = 8), by = "bool_col")
#   expect_equal(as.data.frame(new_th3b), as.data.frame(new_tb3b))
#   
#   new_tb3c <- data$df3 %>%
#     group_by(bool_col) %>%
#     mutate(int_col = rollapply(int_col, 9, max, partial = TRUE, align = "center"))
#   new_th3c <- data$th3$
#     update_by(udb_roll_max_tick("int_col", rev_ticks = 5, fwd_ticks = 4), by = "bool_col")
#   expect_equal(as.data.frame(new_th3c), as.data.frame(new_tb3c))
#   
#   new_tb4a <- data$df4 %>%
#     group_by(X) %>%
#     mutate(Number1 = rollapply(Number1, 3, max, partial = TRUE, align = "right"),
#            Number2 = rollapply(Number2, 3, max, partial = TRUE, align = "right"))
#   new_th4a <- data$th4$
#     update_by(udb_roll_max_tick(c("Number1", "Number2"), rev_ticks = 3), by = "X")
#   expect_equal(as.data.frame(new_th4a), as.data.frame(new_tb4a))
# 
#   new_tb4b <- data$df4 %>%
#     group_by(X) %>%
#     mutate(Number1 = rollapply(Number1, 3, max, partial = TRUE, align = "left"),
#            Number2 = rollapply(Number2, 3, max, partial = TRUE, align = "left"))
#   new_th4b <- data$th4$
#     update_by(udb_roll_max_tick(c("Number1", "Number2"), rev_ticks = 1, fwd_ticks = 2), by = "X")
#   expect_equal(as.data.frame(new_th4b), as.data.frame(new_tb4b))
# 
#   new_tb4c <- data$df4 %>%
#     group_by(X) %>%
#     mutate(Number1 = rollapply(Number1, 3, max, partial = TRUE, align = "center"),
#            Number2 = rollapply(Number2, 3, max, partial = TRUE, align = "center"))
#   new_th4c <- data$th4$
#     update_by(udb_roll_max_tick(c("Number1", "Number2"), rev_ticks = 2, fwd_ticks = 1), by = "X")
#   expect_equal(as.data.frame(new_th4c), as.data.frame(new_tb4c))
# 
#   new_tb5a <- data$df5 %>%
#     group_by(Y) %>%
#     mutate(Number1 = rollapply(Number1, 3, max, partial = TRUE, align = "right"),
#            Number2 = rollapply(Number2, 3, max, partial = TRUE, align = "right"))
#   new_th5a <- data$th5$
#     update_by(udb_roll_max_tick(c("Number1", "Number2"), rev_ticks = 3), by = "Y")
#   expect_equal(as.data.frame(new_th5a), as.data.frame(new_tb5a))
# 
#   new_tb5b <- data$df5 %>%
#     group_by(Y) %>%
#     mutate(Number1 = rollapply(Number1, 3, max, partial = TRUE, align = "left"),
#            Number2 = rollapply(Number2, 3, max, partial = TRUE, align = "left"))
#   new_th5b <- data$th5$
#     update_by(udb_roll_max_tick(c("Number1", "Number2"), rev_ticks = 1, fwd_ticks = 2), by = "Y")
#   expect_equal(as.data.frame(new_th5b), as.data.frame(new_tb5b))
# 
#   new_tb5c <- data$df5 %>%
#     group_by(Y) %>%
#     mutate(Number1 = rollapply(Number1, 3, max, partial = TRUE, align = "center"),
#            Number2 = rollapply(Number2, 3, max, partial = TRUE, align = "center"))
#   new_th5c <- data$th5$
#     update_by(udb_roll_max_tick(c("Number1", "Number2"), rev_ticks = 2, fwd_ticks = 1), by = "Y")
#   expect_equal(as.data.frame(new_th5c), as.data.frame(new_tb5c))
# 
#   new_tb6a <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
#     group_by(X, Y) %>%
#     mutate(Number1 = rollapply(Number1, 3, max, partial = TRUE, align = "right"),
#            Number2 = rollapply(Number2, 3, max, partial = TRUE, align = "right"))
#   new_th6a <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
#     update_by(udb_roll_max_tick(c("Number1", "Number2"), rev_ticks = 3), by = c("X", "Y"))
#   expect_equal(as.data.frame(new_th6a), as.data.frame(new_tb6a))
# 
#   new_tb6b <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
#     group_by(X, Y) %>%
#     mutate(Number1 = rollapply(Number1, 3, max, partial = TRUE, align = "left"),
#            Number2 = rollapply(Number2, 3, max, partial = TRUE, align = "left"))
#   new_th6b <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
#     update_by(udb_roll_max_tick(c("Number1", "Number2"), rev_ticks = 1, fwd_ticks = 2), by = c("X", "Y"))
#   expect_equal(as.data.frame(new_th6b), as.data.frame(new_tb6b))
# 
#   new_tb6c <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
#     group_by(X, Y) %>%
#     mutate(Number1 = rollapply(Number1, 3, max, partial = TRUE, align = "center"),
#            Number2 = rollapply(Number2, 3, max, partial = TRUE, align = "center"))
#   new_th6c <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
#     update_by(udb_roll_max_tick(c("Number1", "Number2"), rev_ticks = 2, fwd_ticks = 1), by = c("X", "Y"))
#   expect_equal(as.data.frame(new_th6c), as.data.frame(new_tb6c))
#   
#   data$client$close()
# })

test_that("udb_roll_max_time behaves as expected", {
  data <- setup()
  data$client$close()
})

test_that("udb_roll_prod_tick behaves as expected", {
  data <- setup()
  
  new_tb1a <- data$df1 %>%
    mutate(dbl_col = rollapply(dbl_col, 3, prod, partial = TRUE, align = "right"))
  new_th1a <- data$th1$
    update_by(udb_roll_prod_tick("dbl_col", rev_ticks = 3))
  expect_equal(as.data.frame(new_th1a), as.data.frame(new_tb1a))
  
  new_tb1b <- data$df1 %>%
    mutate(dbl_col = rollapply(dbl_col, 3, prod, partial = TRUE, align = "left"))
  new_th1b <- data$th1$
    update_by(udb_roll_prod_tick("dbl_col", rev_ticks = 1, fwd_ticks = 2))
  expect_equal(as.data.frame(new_th1b), as.data.frame(new_tb1b))
  
  new_tb1c <- data$df1 %>%
    mutate(dbl_col = rollapply(dbl_col, 3, prod, partial = TRUE, align = "center"))
  new_th1c <- data$th1$
    update_by(udb_roll_prod_tick("dbl_col", rev_ticks = 2, fwd_ticks = 1))
  expect_equal(as.data.frame(new_th1c), as.data.frame(new_tb1c))
  
  new_tb2a <- data$df2 %>%
    mutate(col1 = rollapply(col1, 5, prod, partial = TRUE, align = "right"),
           col3 = rollapply(col3, 5, prod, partial = TRUE, align = "right"))
  new_th2a <- data$th2$
    update_by(udb_roll_prod_tick(c("col1", "col3"), rev_ticks = 5))
  expect_equal(as.data.frame(new_th2a), as.data.frame(new_tb2a))
  
  new_tb2b <- data$df2 %>%
    mutate(col1 = rollapply(col1, 5, prod, partial = TRUE, align = "left"),
           col3 = rollapply(col3, 5, prod, partial = TRUE, align = "left"))
  new_th2b <- data$th2$
    update_by(udb_roll_prod_tick(c("col1", "col3"), rev_ticks = 1, fwd_ticks = 4))
  expect_equal(as.data.frame(new_th2b), as.data.frame(new_tb2b))
  
  new_tb2c <- data$df2 %>%
    mutate(col1 = rollapply(col1, 5, prod, partial = TRUE, align = "center"),
           col3 = rollapply(col3, 5, prod, partial = TRUE, align = "center"))
  new_th2c <- data$th2$
    update_by(udb_roll_prod_tick(c("col1", "col3"), rev_ticks = 3, fwd_ticks = 2))
  expect_equal(as.data.frame(new_th2c), as.data.frame(new_tb2c))
  
  new_tb3a <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(int_col = rollapply(int_col, 9, prod, partial = TRUE, align = "right"))
  new_th3a <- data$th3$
    update_by(udb_roll_prod_tick("int_col", rev_ticks = 9), by = "bool_col")
  expect_equal(as.data.frame(new_th3a), as.data.frame(new_tb3a))
  
  new_tb3b <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(int_col = rollapply(int_col, 9, prod, partial = TRUE, align = "left"))
  new_th3b <- data$th3$
    update_by(udb_roll_prod_tick("int_col", rev_ticks = 1, fwd_ticks = 8), by = "bool_col")
  expect_equal(as.data.frame(new_th3b), as.data.frame(new_tb3b))
  
  new_tb3c <- data$df3 %>%
    group_by(bool_col) %>%
    mutate(int_col = rollapply(int_col, 9, prod, partial = TRUE, align = "center"))
  new_th3c <- data$th3$
    update_by(udb_roll_prod_tick("int_col", rev_ticks = 5, fwd_ticks = 4), by = "bool_col")
  expect_equal(as.data.frame(new_th3c), as.data.frame(new_tb3c))
  
  new_tb4a <- data$df4 %>%
    group_by(X) %>%
    mutate(Number1 = rollapply(Number1, 3, prod, partial = TRUE, align = "right"),
           Number2 = rollapply(Number2, 3, prod, partial = TRUE, align = "right"))
  new_th4a <- data$th4$
    update_by(udb_roll_prod_tick(c("Number1", "Number2"), rev_ticks = 3), by = "X")
  expect_equal(as.data.frame(new_th4a), as.data.frame(new_tb4a))
  
  new_tb4b <- data$df4 %>%
    group_by(X) %>%
    mutate(Number1 = rollapply(Number1, 3, prod, partial = TRUE, align = "left"),
           Number2 = rollapply(Number2, 3, prod, partial = TRUE, align = "left"))
  new_th4b <- data$th4$
    update_by(udb_roll_prod_tick(c("Number1", "Number2"), rev_ticks = 1, fwd_ticks = 2), by = "X")
  expect_equal(as.data.frame(new_th4b), as.data.frame(new_tb4b))
  
  new_tb4c <- data$df4 %>%
    group_by(X) %>%
    mutate(Number1 = rollapply(Number1, 3, prod, partial = TRUE, align = "center"),
           Number2 = rollapply(Number2, 3, prod, partial = TRUE, align = "center"))
  new_th4c <- data$th4$
    update_by(udb_roll_prod_tick(c("Number1", "Number2"), rev_ticks = 2, fwd_ticks = 1), by = "X")
  expect_equal(as.data.frame(new_th4c), as.data.frame(new_tb4c))
  
  new_tb5a <- data$df5 %>%
    group_by(Y) %>%
    mutate(Number1 = rollapply(Number1, 3, prod, partial = TRUE, align = "right"),
           Number2 = rollapply(Number2, 3, prod, partial = TRUE, align = "right"))
  new_th5a <- data$th5$
    update_by(udb_roll_prod_tick(c("Number1", "Number2"), rev_ticks = 3), by = "Y")
  expect_equal(as.data.frame(new_th5a), as.data.frame(new_tb5a))
  
  new_tb5b <- data$df5 %>%
    group_by(Y) %>%
    mutate(Number1 = rollapply(Number1, 3, prod, partial = TRUE, align = "left"),
           Number2 = rollapply(Number2, 3, prod, partial = TRUE, align = "left"))
  new_th5b <- data$th5$
    update_by(udb_roll_prod_tick(c("Number1", "Number2"), rev_ticks = 1, fwd_ticks = 2), by = "Y")
  expect_equal(as.data.frame(new_th5b), as.data.frame(new_tb5b))
  
  new_tb5c <- data$df5 %>%
    group_by(Y) %>%
    mutate(Number1 = rollapply(Number1, 3, prod, partial = TRUE, align = "center"),
           Number2 = rollapply(Number2, 3, prod, partial = TRUE, align = "center"))
  new_th5c <- data$th5$
    update_by(udb_roll_prod_tick(c("Number1", "Number2"), rev_ticks = 2, fwd_ticks = 1), by = "Y")
  expect_equal(as.data.frame(new_th5c), as.data.frame(new_tb5c))
  
  new_tb6a <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
    group_by(X, Y) %>%
    mutate(Number1 = rollapply(Number1, 3, prod, partial = TRUE, align = "right"),
           Number2 = rollapply(Number2, 3, prod, partial = TRUE, align = "right"))
  new_th6a <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(udb_roll_prod_tick(c("Number1", "Number2"), rev_ticks = 3), by = c("X", "Y"))
  expect_equal(as.data.frame(new_th6a), as.data.frame(new_tb6a))
  
  new_tb6b <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
    group_by(X, Y) %>%
    mutate(Number1 = rollapply(Number1, 3, prod, partial = TRUE, align = "left"),
           Number2 = rollapply(Number2, 3, prod, partial = TRUE, align = "left"))
  new_th6b <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(udb_roll_prod_tick(c("Number1", "Number2"), rev_ticks = 1, fwd_ticks = 2), by = c("X", "Y"))
  expect_equal(as.data.frame(new_th6b), as.data.frame(new_tb6b))
  
  new_tb6c <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
    group_by(X, Y) %>%
    mutate(Number1 = rollapply(Number1, 3, prod, partial = TRUE, align = "center"),
           Number2 = rollapply(Number2, 3, prod, partial = TRUE, align = "center"))
  new_th6c <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
    update_by(udb_roll_prod_tick(c("Number1", "Number2"), rev_ticks = 2, fwd_ticks = 1), by = c("X", "Y"))
  expect_equal(as.data.frame(new_th6c), as.data.frame(new_tb6c))
  
  data$client$close()
})

test_that("udb_roll_prod_time behaves as expected", {
  data <- setup()
  data$client$close()
})

test_that("udb_roll_count_tick behaves as expected", {
  data <- setup()
  data$client$close()
})

test_that("udb_roll_count_time behaves as expected", {
  data <- setup()
  data$client$close()
})

# TODO: Wait for bug fix
# test_that("udb_roll_std_tick behaves as expected", {
#   data <- setup()
#   
#   new_tb1a <- data$df1 %>%
#     mutate(dbl_col = rollapply(dbl_col, 3, sd, partial = TRUE, align = "right"))
#   new_th1a <- data$th1$
#     update_by(udb_roll_std_tick("dbl_col", rev_ticks = 3))
#   expect_equal(as.data.frame(new_th1a), as.data.frame(new_tb1a))
#   
#   new_tb1b <- data$df1 %>%
#     mutate(dbl_col = rollapply(dbl_col, 3, sd, partial = TRUE, align = "left"))
#   new_th1b <- data$th1$
#     update_by(udb_roll_std_tick("dbl_col", rev_ticks = 1, fwd_ticks = 2))
#   expect_equal(as.data.frame(new_th1b), as.data.frame(new_tb1b))
#   
#   new_tb1c <- data$df1 %>%
#     mutate(dbl_col = rollapply(dbl_col, 3, sd, partial = TRUE, align = "center"))
#   new_th1c <- data$th1$
#     update_by(udb_roll_std_tick("dbl_col", rev_ticks = 2, fwd_ticks = 1))
#   expect_equal(as.data.frame(new_th1c), as.data.frame(new_tb1c))
#   
#   new_tb2a <- data$df2 %>%
#     mutate(col1 = rollapply(col1, 5, sd, partial = TRUE, align = "right"),
#            col3 = rollapply(col3, 5, sd, partial = TRUE, align = "right"))
#   new_th2a <- data$th2$
#     update_by(udb_roll_std_tick(c("col1", "col3"), rev_ticks = 5))
#   expect_equal(as.data.frame(new_th2a), as.data.frame(new_tb2a))
#   
#   new_tb2b <- data$df2 %>%
#     mutate(col1 = rollapply(col1, 5, sd, partial = TRUE, align = "left"),
#            col3 = rollapply(col3, 5, sd, partial = TRUE, align = "left"))
#   new_th2b <- data$th2$
#     update_by(udb_roll_std_tick(c("col1", "col3"), rev_ticks = 1, fwd_ticks = 4))
#   expect_equal(as.data.frame(new_th2b), as.data.frame(new_tb2b))
#   
#   new_tb2c <- data$df2 %>%
#     mutate(col1 = rollapply(col1, 5, sd, partial = TRUE, align = "center"),
#            col3 = rollapply(col3, 5, sd, partial = TRUE, align = "center"))
#   new_th2c <- data$th2$
#     update_by(udb_roll_std_tick(c("col1", "col3"), rev_ticks = 3, fwd_ticks = 2))
#   expect_equal(as.data.frame(new_th2c), as.data.frame(new_tb2c))
#   
#   new_tb3a <- data$df3 %>%
#     group_by(bool_col) %>%
#     mutate(int_col = rollapply(int_col, 9, sd, partial = TRUE, align = "right"))
#   new_th3a <- data$th3$
#     update_by(udb_roll_std_tick("int_col", rev_ticks = 9), by = "bool_col")
#   expect_equal(as.data.frame(new_th3a), as.data.frame(new_tb3a))
#   
#   new_tb3b <- data$df3 %>%
#     group_by(bool_col) %>%
#     mutate(int_col = rollapply(int_col, 9, sd, partial = TRUE, align = "left"))
#   new_th3b <- data$th3$
#     update_by(udb_roll_std_tick("int_col", rev_ticks = 1, fwd_ticks = 8), by = "bool_col")
#   expect_equal(as.data.frame(new_th3b), as.data.frame(new_tb3b))
#   
#   new_tb3c <- data$df3 %>%
#     group_by(bool_col) %>%
#     mutate(int_col = rollapply(int_col, 9, sd, partial = TRUE, align = "center"))
#   new_th3c <- data$th3$
#     update_by(udb_roll_std_tick("int_col", rev_ticks = 5, fwd_ticks = 4), by = "bool_col")
#   expect_equal(as.data.frame(new_th3c), as.data.frame(new_tb3c))
#   
#   new_tb4a <- data$df4 %>%
#     group_by(X) %>%
#     mutate(Number1 = rollapply(Number1, 3, sd, partial = TRUE, align = "right"),
#            Number2 = rollapply(Number2, 3, sd, partial = TRUE, align = "right"))
#   new_th4a <- data$th4$
#     update_by(udb_roll_std_tick(c("Number1", "Number2"), rev_ticks = 3), by = "X")
#   expect_equal(as.data.frame(new_th4a), as.data.frame(new_tb4a))
#   
#   new_tb4b <- data$df4 %>%
#     group_by(X) %>%
#     mutate(Number1 = rollapply(Number1, 3, sd, partial = TRUE, align = "left"),
#            Number2 = rollapply(Number2, 3, sd, partial = TRUE, align = "left"))
#   new_th4b <- data$th4$
#     update_by(udb_roll_std_tick(c("Number1", "Number2"), rev_ticks = 1, fwd_ticks = 2), by = "X")
#   expect_equal(as.data.frame(new_th4b), as.data.frame(new_tb4b))
#   
#   new_tb4c <- data$df4 %>%
#     group_by(X) %>%
#     mutate(Number1 = rollapply(Number1, 3, sd, partial = TRUE, align = "center"),
#            Number2 = rollapply(Number2, 3, sd, partial = TRUE, align = "center"))
#   new_th4c <- data$th4$
#     update_by(udb_roll_std_tick(c("Number1", "Number2"), rev_ticks = 2, fwd_ticks = 1), by = "X")
#   expect_equal(as.data.frame(new_th4c), as.data.frame(new_tb4c))
#   
#   new_tb5a <- data$df5 %>%
#     group_by(Y) %>%
#     mutate(Number1 = rollapply(Number1, 3, sd, partial = TRUE, align = "right"),
#            Number2 = rollapply(Number2, 3, sd, partial = TRUE, align = "right"))
#   new_th5a <- data$th5$
#     update_by(udb_roll_std_tick(c("Number1", "Number2"), rev_ticks = 3), by = "Y")
#   expect_equal(as.data.frame(new_th5a), as.data.frame(new_tb5a))
#   
#   new_tb5b <- data$df5 %>%
#     group_by(Y) %>%
#     mutate(Number1 = rollapply(Number1, 3, sd, partial = TRUE, align = "left"),
#            Number2 = rollapply(Number2, 3, sd, partial = TRUE, align = "left"))
#   new_th5b <- data$th5$
#     update_by(udb_roll_std_tick(c("Number1", "Number2"), rev_ticks = 1, fwd_ticks = 2), by = "Y")
#   expect_equal(as.data.frame(new_th5b), as.data.frame(new_tb5b))
#   
#   new_tb5c <- data$df5 %>%
#     group_by(Y) %>%
#     mutate(Number1 = rollapply(Number1, 3, sd, partial = TRUE, align = "center"),
#            Number2 = rollapply(Number2, 3, sd, partial = TRUE, align = "center"))
#   new_th5c <- data$th5$
#     update_by(udb_roll_std_tick(c("Number1", "Number2"), rev_ticks = 2, fwd_ticks = 1), by = "Y")
#   expect_equal(as.data.frame(new_th5c), as.data.frame(new_tb5c))
#   
#   new_tb6a <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
#     group_by(X, Y) %>%
#     mutate(Number1 = rollapply(Number1, 3, sd, partial = TRUE, align = "right"),
#            Number2 = rollapply(Number2, 3, sd, partial = TRUE, align = "right"))
#   new_th6a <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
#     update_by(udb_roll_std_tick(c("Number1", "Number2"), rev_ticks = 3), by = c("X", "Y"))
#   expect_equal(as.data.frame(new_th6a), as.data.frame(new_tb6a))
#   
#   new_tb6b <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
#     group_by(X, Y) %>%
#     mutate(Number1 = rollapply(Number1, 3, sd, partial = TRUE, align = "left"),
#            Number2 = rollapply(Number2, 3, sd, partial = TRUE, align = "left"))
#   new_th6b <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
#     update_by(udb_roll_std_tick(c("Number1", "Number2"), rev_ticks = 1, fwd_ticks = 2), by = c("X", "Y"))
#   expect_equal(as.data.frame(new_th6b), as.data.frame(new_tb6b))
#   
#   new_tb6c <- rbind(data$df4, data$df5, data$df4, data$df5) %>%
#     group_by(X, Y) %>%
#     mutate(Number1 = rollapply(Number1, 3, sd, partial = TRUE, align = "center"),
#            Number2 = rollapply(Number2, 3, sd, partial = TRUE, align = "center"))
#   new_th6c <- merge_tables(data$th4, data$th5, data$th4, data$th5)$
#     update_by(udb_roll_std_tick(c("Number1", "Number2"), rev_ticks = 2, fwd_ticks = 1), by = c("X", "Y"))
#   expect_equal(as.data.frame(new_th6c), as.data.frame(new_tb6c))
#   
#   data$client$close()
# })

test_that("udb_roll_std_time behaves as expected", {
  data <- setup()
  data$client$close()
})

test_that("udb_roll_wavg_tick behaves as expected", {
  data <- setup()
  data$client$close()
})

test_that("udb_roll_wavg_time behaves as expected", {
  data <- setup()
  data$client$close()
})