library(testthat)
library(rdeephaven)

##### TESTING BAD INPUTS #####

test_that("udb_cum_sum fails nicely when 'cols' is a bad type", {
  expect_error(
    udb_cum_sum(5),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    udb_cum_sum(TRUE),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("udb_cum_prod fails nicely when 'cols' is a bad type", {
  expect_error(
    udb_cum_prod(5),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    udb_cum_prod(TRUE),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("udb_cum_min fails nicely when 'cols' is a bad type", {
  expect_error(
    udb_cum_min(5),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    udb_cum_min(TRUE),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("udb_cum_max fails nicely when 'cols' is a bad type", {
  expect_error(
    udb_cum_max(5),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    udb_cum_max(TRUE),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("udb_forward_fill fails nicely when 'cols' is a bad type", {
  expect_error(
    udb_forward_fill(5),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    udb_forward_fill(TRUE),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("udb_delta fails nicely when 'cols' is a bad type", {
  expect_error(
    udb_delta(5),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    udb_delta(TRUE),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("udb_delta fails nicely when 'delta_control' is a bad type or value", {
  expect_error(
    udb_delta("column", 5),
    "'delta_control' must be one of 'null_dominates', 'value_dominates', or 'zero_dominates'. Got '5'."
  )
  expect_error(
    udb_delta("column", TRUE),
    "'delta_control' must be one of 'null_dominates', 'value_dominates', or 'zero_dominates'. Got 'TRUE'."
  )
  expect_error(
    udb_delta("column", "hello!"),
    "'delta_control' must be one of 'null_dominates', 'value_dominates', or 'zero_dominates'. Got 'hello!'."
  )
})

test_that("udb_ema_tick fails nicely when 'decay_ticks' is a bad type", {
  expect_error(
    udb_ema_tick("hello!", "column"),
    "'decay_ticks' must be a single numeric. Got an object of class character."
  )
  expect_error(
    udb_ema_tick(TRUE, "column"),
    "'decay_ticks' must be a single numeric. Got an object of class logical."
  )
  expect_error(
    udb_ema_tick(c(1, 2, 3), "column"),
    "'decay_ticks' must be a single numeric. Got a vector of length 3."
  )
})

test_that("udb_ema_tick fails nicely when 'cols' is a bad type", {
  expect_error(
    udb_ema_tick(5, 5),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    udb_ema_tick(5, TRUE),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("udb_ema_tick fails nicely when 'operation_control' is a bad type or value", {
  expect_error(
    udb_ema_tick(5, "column", 5),
    "'operation_control' must be a single Deephaven OperationControl. Got an object of class numeric."
  )
  expect_error(
    udb_ema_tick(5, "column", TRUE),
    "'operation_control' must be a single Deephaven OperationControl. Got an object of class logical."
  )
  expect_error(
    udb_ema_tick(5, "column", c(op_control(), op_control(), op_control())),
    "'operation_control' must be a single Deephaven OperationControl. Got a vector of length 3."
  )
})

test_that("udb_ema_time fails nicely when 'ts_col' is a bad type", {
  expect_error(
    udb_ema_time(5, "PT1s"),
    "'ts_col' must be a single string. Got an object of class numeric."
  )
  expect_error(
    udb_ema_time(TRUE, "PT1s"),
    "'ts_col' must be a single string. Got an object of class logical."
  )
  expect_error(
    udb_ema_time(c("Many", "strings"), "PT1s"),
    "'ts_col' must be a single string. Got a vector of length 2."
  )
})

test_that("udb_ema_time fails nicely when 'decay_time' is a bad type", {
  expect_error(
    udb_ema_time("PT00:00:00", 5),
    "'decay_time' must be a single string. Got an object of class numeric."
  )
  expect_error(
    udb_ema_time("PT00:00:00", TRUE),
    "'decay_time' must be a single string. Got an object of class logical."
  )
  expect_error(
    udb_ema_time("PT00:00:00", c("Many", "strings")),
    "'decay_time' must be a single string. Got a vector of length 2."
  )
})

test_that("udb_ema_time fails nicely when 'cols' is a bad type", {
  expect_error(
    udb_ema_time("PT00:00:00", "PT1s", 5),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    udb_ema_time("PT00:00:00", "PT1s", TRUE),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("udb_ema_time fails nicely when 'operation_control' is a bad type", {
  expect_error(
    udb_ema_time("PT00:00:00", "PT1s", "column", 5),
    "'operation_control' must be a single Deephaven OperationControl. Got an object of class numeric."
  )
  expect_error(
    udb_ema_time("PT00:00:00", "PT1s", "column", TRUE),
    "'operation_control' must be a single Deephaven OperationControl. Got an object of class logical."
  )
  expect_error(
    udb_ema_time("PT00:00:00", "PT1s", "column", c(op_control(), op_control(), op_control())),
    "'operation_control' must be a single Deephaven OperationControl. Got a vector of length 3."
  )
})

test_that("udb_ems_tick fails nicely when 'decay_ticks' is a bad type", {
  expect_error(
    udb_ems_tick("hello!", "column"),
    "'decay_ticks' must be a single numeric. Got an object of class character."
  )
  expect_error(
    udb_ems_tick(TRUE, "column"),
    "'decay_ticks' must be a single numeric. Got an object of class logical."
  )
  expect_error(
    udb_ems_tick(c(1, 2, 3), "column"),
    "'decay_ticks' must be a single numeric. Got a vector of length 3."
  )
})

test_that("udb_ems_tick fails nicely when 'cols' is a bad type", {
  expect_error(
    udb_ems_tick(5, 5),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    udb_ems_tick(5, TRUE),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("udb_ems_tick fails nicely when 'operation_control' is a bad type or value", {
  expect_error(
    udb_ems_tick(5, "column", 5),
    "'operation_control' must be a single Deephaven OperationControl. Got an object of class numeric."
  )
  expect_error(
    udb_ems_tick(5, "column", TRUE),
    "'operation_control' must be a single Deephaven OperationControl. Got an object of class logical."
  )
  expect_error(
    udb_ems_tick(5, "column", c(op_control(), op_control(), op_control())),
    "'operation_control' must be a single Deephaven OperationControl. Got a vector of length 3."
  )
})

test_that("udb_ems_time fails nicely when 'ts_col' is a bad type", {
  expect_error(
    udb_ems_time(5, "PT1s"),
    "'ts_col' must be a single string. Got an object of class numeric."
  )
  expect_error(
    udb_ems_time(TRUE, "PT1s"),
    "'ts_col' must be a single string. Got an object of class logical."
  )
  expect_error(
    udb_ems_time(c("Many", "strings"), "PT1s"),
    "'ts_col' must be a single string. Got a vector of length 2."
  )
})

test_that("udb_ems_time fails nicely when 'decay_time' is a bad type", {
  expect_error(
    udb_ems_time("PT00:00:00", 5),
    "'decay_time' must be a single string. Got an object of class numeric."
  )
  expect_error(
    udb_ems_time("PT00:00:00", TRUE),
    "'decay_time' must be a single string. Got an object of class logical."
  )
  expect_error(
    udb_ems_time("PT00:00:00", c("Many", "strings")),
    "'decay_time' must be a single string. Got a vector of length 2."
  )
})

test_that("udb_ems_time fails nicely when 'cols' is a bad type", {
  expect_error(
    udb_ems_time("PT00:00:00", "PT1s", 5),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    udb_ems_time("PT00:00:00", "PT1s", TRUE),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("udb_ems_time fails nicely when 'operation_control' is a bad type", {
  expect_error(
    udb_ems_time("PT00:00:00", "PT1s", "column", 5),
    "'operation_control' must be a single Deephaven OperationControl. Got an object of class numeric."
  )
  expect_error(
    udb_ems_time("PT00:00:00", "PT1s", "column", TRUE),
    "'operation_control' must be a single Deephaven OperationControl. Got an object of class logical."
  )
  expect_error(
    udb_ems_time("PT00:00:00", "PT1s", "column", c(op_control(), op_control(), op_control())),
    "'operation_control' must be a single Deephaven OperationControl. Got a vector of length 3."
  )
})

test_that("udb_emmin_tick fails nicely when 'cols' is a bad type", {})

test_that("udb_emmin_time fails nicely when 'cols' is a bad type", {})

test_that("udb_emmax_tick fails nicely when 'cols' is a bad type", {})

test_that("udb_emmax_time fails nicely when 'cols' is a bad type", {})

test_that("udb_emstd_tick fails nicely when 'cols' is a bad type", {})

test_that("udb_emstd_time fails nicely when 'cols' is a bad type", {})

test_that("udb_roll_sum_tick fails nicely when 'cols' is a bad type", {})

test_that("udb_roll_sum_time fails nicely when 'cols' is a bad type", {})

test_that("udb_roll_group_tick fails nicely when 'cols' is a bad type", {})

test_that("udb_roll_group_time fails nicely when 'cols' is a bad type", {})

test_that("udb_roll_avg_tick fails nicely when 'cols' is a bad type", {})

test_that("udb_roll_avg_time fails nicely when 'cols' is a bad type", {})

test_that("udb_roll_min_tick fails nicely when 'cols' is a bad type", {})

test_that("udb_roll_min_time fails nicely when 'cols' is a bad type", {})

test_that("udb_roll_max_tick fails nicely when 'cols' is a bad type", {})

test_that("udb_roll_max_time fails nicely when 'cols' is a bad type", {})

test_that("udb_roll_prod_tick fails nicely when 'cols' is a bad type", {})

test_that("udb_roll_prod_time fails nicely when 'cols' is a bad type", {})

test_that("udb_roll_prod_tick fails nicely when 'cols' is a bad type", {})

test_that("udb_roll_prod_time fails nicely when 'cols' is a bad type", {})

test_that("udb_roll_std_tick fails nicely when 'cols' is a bad type", {})

test_that("udb_roll_std_time fails nicely when 'cols' is a bad type", {})

test_that("udb_roll_wavg_tick fails nicely when 'cols' is a bad type", {})

test_that("udb_roll_wavg_time fails nicely when 'cols' is a bad type", {})