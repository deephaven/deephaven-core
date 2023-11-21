library(testthat)
library(rdeephaven)

##### TESTING BAD INPUTS #####

test_that("uby_cum_sum fails nicely when 'cols' is a bad type", {
  expect_error(
    uby_cum_sum(5),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    uby_cum_sum(TRUE),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("uby_cum_prod fails nicely when 'cols' is a bad type", {
  expect_error(
    uby_cum_prod(5),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    uby_cum_prod(TRUE),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("uby_cum_min fails nicely when 'cols' is a bad type", {
  expect_error(
    uby_cum_min(5),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    uby_cum_min(TRUE),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("uby_cum_max fails nicely when 'cols' is a bad type", {
  expect_error(
    uby_cum_max(5),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    uby_cum_max(TRUE),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("uby_forward_fill fails nicely when 'cols' is a bad type", {
  expect_error(
    uby_forward_fill(5),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    uby_forward_fill(TRUE),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("uby_delta fails nicely when 'cols' is a bad type", {
  expect_error(
    uby_delta(5),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    uby_delta(TRUE),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("uby_delta fails nicely when 'delta_control' is a bad type or value", {
  expect_error(
    uby_delta("column", 5),
    "'delta_control' must be one of 'null_dominates', 'value_dominates', or 'zero_dominates'. Got '5'."
  )
  expect_error(
    uby_delta("column", TRUE),
    "'delta_control' must be one of 'null_dominates', 'value_dominates', or 'zero_dominates'. Got 'TRUE'."
  )
  expect_error(
    uby_delta("column", "hello!"),
    "'delta_control' must be one of 'null_dominates', 'value_dominates', or 'zero_dominates'. Got 'hello!'."
  )
})

test_that("uby_ema_tick fails nicely when 'decay_ticks' is a bad type", {
  expect_error(
    uby_ema_tick("hello!", "column"),
    "'decay_ticks' must be a single real number. Got an object of class character."
  )
  expect_error(
    uby_ema_tick(TRUE, "column"),
    "'decay_ticks' must be a single real number. Got an object of class logical."
  )
  expect_error(
    uby_ema_tick(c(1, 2, 3), "column"),
    "'decay_ticks' must be a single real number. Got a vector of length 3."
  )
})

test_that("uby_ema_tick fails nicely when 'cols' is a bad type", {
  expect_error(
    uby_ema_tick(5, 5),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    uby_ema_tick(5, TRUE),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("uby_ema_tick fails nicely when 'operation_control' is a bad type or value", {
  expect_error(
    uby_ema_tick(5, "column", 5),
    "'operation_control' must be a single Deephaven OperationControl. Got an object of class numeric."
  )
  expect_error(
    uby_ema_tick(5, "column", TRUE),
    "'operation_control' must be a single Deephaven OperationControl. Got an object of class logical."
  )
  expect_error(
    uby_ema_tick(5, "column", c(op_control(), op_control(), op_control())),
    "'operation_control' must be a single Deephaven OperationControl. Got a vector of length 3."
  )
})

test_that("uby_ema_time fails nicely when 'ts_col' is a bad type", {
  expect_error(
    uby_ema_time(5, "PT1s"),
    "'ts_col' must be a single string. Got an object of class numeric."
  )
  expect_error(
    uby_ema_time(TRUE, "PT1s"),
    "'ts_col' must be a single string. Got an object of class logical."
  )
  expect_error(
    uby_ema_time(c("Many", "strings"), "PT1s"),
    "'ts_col' must be a single string. Got a vector of length 2."
  )
})

test_that("uby_ema_time fails nicely when 'decay_time' is a bad type", {
  expect_error(
    uby_ema_time("PT0s", 5),
    "'decay_time' must be a single string. Got an object of class numeric."
  )
  expect_error(
    uby_ema_time("PT0s", TRUE),
    "'decay_time' must be a single string. Got an object of class logical."
  )
  expect_error(
    uby_ema_time("PT0s", c("Many", "strings")),
    "'decay_time' must be a single string. Got a vector of length 2."
  )
})

test_that("uby_ema_time fails nicely when 'cols' is a bad type", {
  expect_error(
    uby_ema_time("PT0s", "PT1s", 5),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    uby_ema_time("PT0s", "PT1s", TRUE),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("uby_ema_time fails nicely when 'operation_control' is a bad type", {
  expect_error(
    uby_ema_time("PT0s", "PT1s", "column", 5),
    "'operation_control' must be a single Deephaven OperationControl. Got an object of class numeric."
  )
  expect_error(
    uby_ema_time("PT0s", "PT1s", "column", TRUE),
    "'operation_control' must be a single Deephaven OperationControl. Got an object of class logical."
  )
  expect_error(
    uby_ema_time("PT0s", "PT1s", "column", c(op_control(), op_control(), op_control())),
    "'operation_control' must be a single Deephaven OperationControl. Got a vector of length 3."
  )
})

test_that("uby_ems_tick fails nicely when 'decay_ticks' is a bad type", {
  expect_error(
    uby_ems_tick("hello!", "column"),
    "'decay_ticks' must be a single real number. Got an object of class character."
  )
  expect_error(
    uby_ems_tick(TRUE, "column"),
    "'decay_ticks' must be a single real number. Got an object of class logical."
  )
  expect_error(
    uby_ems_tick(c(1, 2, 3), "column"),
    "'decay_ticks' must be a single real number. Got a vector of length 3."
  )
})

test_that("uby_ems_tick fails nicely when 'cols' is a bad type", {
  expect_error(
    uby_ems_tick(5, 5),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    uby_ems_tick(5, TRUE),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("uby_ems_tick fails nicely when 'operation_control' is a bad type or value", {
  expect_error(
    uby_ems_tick(5, "column", 5),
    "'operation_control' must be a single Deephaven OperationControl. Got an object of class numeric."
  )
  expect_error(
    uby_ems_tick(5, "column", TRUE),
    "'operation_control' must be a single Deephaven OperationControl. Got an object of class logical."
  )
  expect_error(
    uby_ems_tick(5, "column", c(op_control(), op_control(), op_control())),
    "'operation_control' must be a single Deephaven OperationControl. Got a vector of length 3."
  )
})

test_that("uby_ems_time fails nicely when 'ts_col' is a bad type", {
  expect_error(
    uby_ems_time(5, "PT1s"),
    "'ts_col' must be a single string. Got an object of class numeric."
  )
  expect_error(
    uby_ems_time(TRUE, "PT1s"),
    "'ts_col' must be a single string. Got an object of class logical."
  )
  expect_error(
    uby_ems_time(c("Many", "strings"), "PT1s"),
    "'ts_col' must be a single string. Got a vector of length 2."
  )
})

test_that("uby_ems_time fails nicely when 'decay_time' is a bad type", {
  expect_error(
    uby_ems_time("PT0s", 5),
    "'decay_time' must be a single string. Got an object of class numeric."
  )
  expect_error(
    uby_ems_time("PT0s", TRUE),
    "'decay_time' must be a single string. Got an object of class logical."
  )
  expect_error(
    uby_ems_time("PT0s", c("Many", "strings")),
    "'decay_time' must be a single string. Got a vector of length 2."
  )
})

test_that("uby_ems_time fails nicely when 'cols' is a bad type", {
  expect_error(
    uby_ems_time("PT0s", "PT1s", 5),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    uby_ems_time("PT0s", "PT1s", TRUE),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("uby_ems_time fails nicely when 'operation_control' is a bad type", {
  expect_error(
    uby_ems_time("PT0s", "PT1s", "column", 5),
    "'operation_control' must be a single Deephaven OperationControl. Got an object of class numeric."
  )
  expect_error(
    uby_ems_time("PT0s", "PT1s", "column", TRUE),
    "'operation_control' must be a single Deephaven OperationControl. Got an object of class logical."
  )
  expect_error(
    uby_ems_time("PT0s", "PT1s", "column", c(op_control(), op_control(), op_control())),
    "'operation_control' must be a single Deephaven OperationControl. Got a vector of length 3."
  )
})

test_that("uby_emmin_tick fails nicely when 'decay_ticks' is a bad type", {
  expect_error(
    uby_emmin_tick("hello!", "column"),
    "'decay_ticks' must be a single real number. Got an object of class character."
  )
  expect_error(
    uby_emmin_tick(TRUE, "column"),
    "'decay_ticks' must be a single real number. Got an object of class logical."
  )
  expect_error(
    uby_emmin_tick(c(1, 2, 3), "column"),
    "'decay_ticks' must be a single real number. Got a vector of length 3."
  )
})

test_that("uby_emmin_tick fails nicely when 'cols' is a bad type", {
  expect_error(
    uby_emmin_tick(5, 5),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    uby_emmin_tick(5, TRUE),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("uby_emmin_tick fails nicely when 'operation_control' is a bad type or value", {
  expect_error(
    uby_emmin_tick(5, "column", 5),
    "'operation_control' must be a single Deephaven OperationControl. Got an object of class numeric."
  )
  expect_error(
    uby_emmin_tick(5, "column", TRUE),
    "'operation_control' must be a single Deephaven OperationControl. Got an object of class logical."
  )
  expect_error(
    uby_emmin_tick(5, "column", c(op_control(), op_control(), op_control())),
    "'operation_control' must be a single Deephaven OperationControl. Got a vector of length 3."
  )
})

test_that("uby_emmin_time fails nicely when 'ts_col' is a bad type", {
  expect_error(
    uby_emmin_time(5, "PT1s"),
    "'ts_col' must be a single string. Got an object of class numeric."
  )
  expect_error(
    uby_emmin_time(TRUE, "PT1s"),
    "'ts_col' must be a single string. Got an object of class logical."
  )
  expect_error(
    uby_emmin_time(c("Many", "strings"), "PT1s"),
    "'ts_col' must be a single string. Got a vector of length 2."
  )
})

test_that("uby_emmin_time fails nicely when 'decay_time' is a bad type", {
  expect_error(
    uby_emmin_time("PT0s", 5),
    "'decay_time' must be a single string. Got an object of class numeric."
  )
  expect_error(
    uby_emmin_time("PT0s", TRUE),
    "'decay_time' must be a single string. Got an object of class logical."
  )
  expect_error(
    uby_emmin_time("PT0s", c("Many", "strings")),
    "'decay_time' must be a single string. Got a vector of length 2."
  )
})

test_that("uby_emmin_time fails nicely when 'cols' is a bad type", {
  expect_error(
    uby_emmin_time("PT0s", "PT1s", 5),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    uby_emmin_time("PT0s", "PT1s", TRUE),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("uby_emmin_time fails nicely when 'operation_control' is a bad type", {
  expect_error(
    uby_emmin_time("PT0s", "PT1s", "column", 5),
    "'operation_control' must be a single Deephaven OperationControl. Got an object of class numeric."
  )
  expect_error(
    uby_emmin_time("PT0s", "PT1s", "column", TRUE),
    "'operation_control' must be a single Deephaven OperationControl. Got an object of class logical."
  )
  expect_error(
    uby_emmin_time("PT0s", "PT1s", "column", c(op_control(), op_control(), op_control())),
    "'operation_control' must be a single Deephaven OperationControl. Got a vector of length 3."
  )
})

test_that("uby_emmax_tick fails nicely when 'decay_ticks' is a bad type", {
  expect_error(
    uby_emmax_tick("hello!", "column"),
    "'decay_ticks' must be a single real number. Got an object of class character."
  )
  expect_error(
    uby_emmax_tick(TRUE, "column"),
    "'decay_ticks' must be a single real number. Got an object of class logical."
  )
  expect_error(
    uby_emmax_tick(c(1, 2, 3), "column"),
    "'decay_ticks' must be a single real number. Got a vector of length 3."
  )
})

test_that("uby_emmax_tick fails nicely when 'cols' is a bad type", {
  expect_error(
    uby_emmax_tick(5, 5),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    uby_emmax_tick(5, TRUE),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("uby_emmax_tick fails nicely when 'operation_control' is a bad type or value", {
  expect_error(
    uby_emmax_tick(5, "column", 5),
    "'operation_control' must be a single Deephaven OperationControl. Got an object of class numeric."
  )
  expect_error(
    uby_emmax_tick(5, "column", TRUE),
    "'operation_control' must be a single Deephaven OperationControl. Got an object of class logical."
  )
  expect_error(
    uby_emmax_tick(5, "column", c(op_control(), op_control(), op_control())),
    "'operation_control' must be a single Deephaven OperationControl. Got a vector of length 3."
  )
})

test_that("uby_emmax_time fails nicely when 'ts_col' is a bad type", {
  expect_error(
    uby_emmax_time(5, "PT1s"),
    "'ts_col' must be a single string. Got an object of class numeric."
  )
  expect_error(
    uby_emmax_time(TRUE, "PT1s"),
    "'ts_col' must be a single string. Got an object of class logical."
  )
  expect_error(
    uby_emmax_time(c("Many", "strings"), "PT1s"),
    "'ts_col' must be a single string. Got a vector of length 2."
  )
})

test_that("uby_emmax_time fails nicely when 'decay_time' is a bad type", {
  expect_error(
    uby_emmax_time("PT0s", 5),
    "'decay_time' must be a single string. Got an object of class numeric."
  )
  expect_error(
    uby_emmax_time("PT0s", TRUE),
    "'decay_time' must be a single string. Got an object of class logical."
  )
  expect_error(
    uby_emmax_time("PT0s", c("Many", "strings")),
    "'decay_time' must be a single string. Got a vector of length 2."
  )
})

test_that("uby_emmax_time fails nicely when 'cols' is a bad type", {
  expect_error(
    uby_emmax_time("PT0s", "PT1s", 5),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    uby_emmax_time("PT0s", "PT1s", TRUE),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("uby_emmax_time fails nicely when 'operation_control' is a bad type", {
  expect_error(
    uby_emmax_time("PT0s", "PT1s", "column", 5),
    "'operation_control' must be a single Deephaven OperationControl. Got an object of class numeric."
  )
  expect_error(
    uby_emmax_time("PT0s", "PT1s", "column", TRUE),
    "'operation_control' must be a single Deephaven OperationControl. Got an object of class logical."
  )
  expect_error(
    uby_emmax_time("PT0s", "PT1s", "column", c(op_control(), op_control(), op_control())),
    "'operation_control' must be a single Deephaven OperationControl. Got a vector of length 3."
  )
})

test_that("uby_emstd_tick fails nicely when 'decay_ticks' is a bad type", {
  expect_error(
    uby_emstd_tick("hello!", "column"),
    "'decay_ticks' must be a single real number. Got an object of class character."
  )
  expect_error(
    uby_emstd_tick(TRUE, "column"),
    "'decay_ticks' must be a single real number. Got an object of class logical."
  )
  expect_error(
    uby_emstd_tick(c(1, 2, 3), "column"),
    "'decay_ticks' must be a single real number. Got a vector of length 3."
  )
})

test_that("uby_emstd_tick fails nicely when 'cols' is a bad type", {
  expect_error(
    uby_emstd_tick(5, 5),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    uby_emstd_tick(5, TRUE),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("uby_emstd_tick fails nicely when 'operation_control' is a bad type or value", {
  expect_error(
    uby_emstd_tick(5, "column", 5),
    "'operation_control' must be a single Deephaven OperationControl. Got an object of class numeric."
  )
  expect_error(
    uby_emstd_tick(5, "column", TRUE),
    "'operation_control' must be a single Deephaven OperationControl. Got an object of class logical."
  )
  expect_error(
    uby_emstd_tick(5, "column", c(op_control(), op_control(), op_control())),
    "'operation_control' must be a single Deephaven OperationControl. Got a vector of length 3."
  )
})

test_that("uby_emstd_time fails nicely when 'ts_col' is a bad type", {
  expect_error(
    uby_emstd_time(5, "PT1s"),
    "'ts_col' must be a single string. Got an object of class numeric."
  )
  expect_error(
    uby_emstd_time(TRUE, "PT1s"),
    "'ts_col' must be a single string. Got an object of class logical."
  )
  expect_error(
    uby_emstd_time(c("Many", "strings"), "PT1s"),
    "'ts_col' must be a single string. Got a vector of length 2."
  )
})

test_that("uby_emstd_time fails nicely when 'decay_time' is a bad type", {
  expect_error(
    uby_emstd_time("PT0s", 5),
    "'decay_time' must be a single string. Got an object of class numeric."
  )
  expect_error(
    uby_emstd_time("PT0s", TRUE),
    "'decay_time' must be a single string. Got an object of class logical."
  )
  expect_error(
    uby_emstd_time("PT0s", c("Many", "strings")),
    "'decay_time' must be a single string. Got a vector of length 2."
  )
})

test_that("uby_emstd_time fails nicely when 'cols' is a bad type", {
  expect_error(
    uby_emstd_time("PT0s", "PT1s", 5),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    uby_emstd_time("PT0s", "PT1s", TRUE),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("uby_emstd_time fails nicely when 'operation_control' is a bad type", {
  expect_error(
    uby_emstd_time("PT0s", "PT1s", "column", 5),
    "'operation_control' must be a single Deephaven OperationControl. Got an object of class numeric."
  )
  expect_error(
    uby_emstd_time("PT0s", "PT1s", "column", TRUE),
    "'operation_control' must be a single Deephaven OperationControl. Got an object of class logical."
  )
  expect_error(
    uby_emstd_time("PT0s", "PT1s", "column", c(op_control(), op_control(), op_control())),
    "'operation_control' must be a single Deephaven OperationControl. Got a vector of length 3."
  )
})

test_that("uby_rolling_sum_tick fails nicely when 'cols' is a bad type", {
  expect_error(
    uby_rolling_sum_tick(5, 5),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    uby_rolling_sum_tick(TRUE, 5),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("uby_rolling_sum_tick fails nicely when 'rev_ticks' is a bad type", {
  expect_error(
    uby_rolling_sum_tick("column", "string"),
    "'rev_ticks' must be a single integer. Got an object of class character."
  )
  expect_error(
    uby_rolling_sum_tick("column", TRUE),
    "'rev_ticks' must be a single integer. Got an object of class logical."
  )
  expect_error(
    uby_rolling_sum_tick("column", c(1, 2, 3)),
    "'rev_ticks' must be a single integer. Got a vector of length 3."
  )
})

test_that("uby_rolling_sum_tick fails nicely when 'fwd_ticks' is a bad type", {
  expect_error(
    uby_rolling_sum_tick("column", 5, "string"),
    "'fwd_ticks' must be a single integer. Got an object of class character."
  )
  expect_error(
    uby_rolling_sum_tick("column", 5, TRUE),
    "'fwd_ticks' must be a single integer. Got an object of class logical."
  )
  expect_error(
    uby_rolling_sum_tick("column", 5, c(1, 2, 3)),
    "'fwd_ticks' must be a single integer. Got a vector of length 3."
  )
})

test_that("uby_rolling_sum_time fails nicely when 'ts_col' is a bad type", {
  expect_error(
    uby_rolling_sum_time(5, "col", "PT0s", "PT0s"),
    "'ts_col' must be a single string. Got an object of class numeric."
  )
  expect_error(
    uby_rolling_sum_time(TRUE, "col", "PT0s", "PT0s"),
    "'ts_col' must be a single string. Got an object of class logical."
  )
  expect_error(
    uby_rolling_sum_time(c("Many", "strings"), "col", "PT0s", "PT0s"),
    "'ts_col' must be a single string. Got a vector of length 2."
  )
})

test_that("uby_rolling_sum_time fails nicely when 'cols' is a bad type", {
  expect_error(
    uby_rolling_sum_time("PT0s", 5, "PT0s", "PT0s"),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    uby_rolling_sum_time("PT0s", TRUE, "PT0s", "PT0s"),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("uby_rolling_sum_time fails nicely when 'rev_time' is a bad type", {
  expect_error(
    uby_rolling_sum_time("PT0s", "col", 5, "PT0s"),
    "'rev_time' must be a single string. Got an object of class numeric."
  )
  expect_error(
    uby_rolling_sum_time("PT0s", "col", TRUE, "PT0s"),
    "'rev_time' must be a single string. Got an object of class logical."
  )
  expect_error(
    uby_rolling_sum_time("PT0s", "col", c("Many", "strings"), "PT0s"),
    "'rev_time' must be a single string. Got a vector of length 2."
  )
})

test_that("uby_rolling_sum_time fails nicely when 'fwd_time' is a bad type", {
  expect_error(
    uby_rolling_sum_time("PT0s", "col", "PT0s", 5),
    "'fwd_time' must be a single string. Got an object of class numeric."
  )
  expect_error(
    uby_rolling_sum_time("PT0s", "col", "PT0s", TRUE),
    "'fwd_time' must be a single string. Got an object of class logical."
  )
  expect_error(
    uby_rolling_sum_time("PT0s", "col", "PT0s", c("Many", "strings")),
    "'fwd_time' must be a single string. Got a vector of length 2."
  )
})

test_that("uby_rolling_group_tick fails nicely when 'cols' is a bad type", {
  expect_error(
    uby_rolling_group_tick(5, 5),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    uby_rolling_group_tick(TRUE, 5),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("uby_rolling_group_tick fails nicely when 'rev_ticks' is a bad type", {
  expect_error(
    uby_rolling_group_tick("column", "string"),
    "'rev_ticks' must be a single integer. Got an object of class character."
  )
  expect_error(
    uby_rolling_group_tick("column", TRUE),
    "'rev_ticks' must be a single integer. Got an object of class logical."
  )
  expect_error(
    uby_rolling_group_tick("column", c(1, 2, 3)),
    "'rev_ticks' must be a single integer. Got a vector of length 3."
  )
})

test_that("uby_rolling_group_tick fails nicely when 'fwd_ticks' is a bad type", {
  expect_error(
    uby_rolling_group_tick("column", 5, "string"),
    "'fwd_ticks' must be a single integer. Got an object of class character."
  )
  expect_error(
    uby_rolling_group_tick("column", 5, TRUE),
    "'fwd_ticks' must be a single integer. Got an object of class logical."
  )
  expect_error(
    uby_rolling_group_tick("column", 5, c(1, 2, 3)),
    "'fwd_ticks' must be a single integer. Got a vector of length 3."
  )
})

test_that("uby_rolling_group_time fails nicely when 'ts_col' is a bad type", {
  expect_error(
    uby_rolling_group_time(5, "col", "PT0s", "PT0s"),
    "'ts_col' must be a single string. Got an object of class numeric."
  )
  expect_error(
    uby_rolling_group_time(TRUE, "col", "PT0s", "PT0s"),
    "'ts_col' must be a single string. Got an object of class logical."
  )
  expect_error(
    uby_rolling_group_time(c("Many", "strings"), "col", "PT0s", "PT0s"),
    "'ts_col' must be a single string. Got a vector of length 2."
  )
})

test_that("uby_rolling_group_time fails nicely when 'cols' is a bad type", {
  expect_error(
    uby_rolling_group_time("PT0s", 5, "PT0s", "PT0s"),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    uby_rolling_group_time("PT0s", TRUE, "PT0s", "PT0s"),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("uby_rolling_group_time fails nicely when 'rev_time' is a bad type", {
  expect_error(
    uby_rolling_group_time("PT0s", "col", 5, "PT0s"),
    "'rev_time' must be a single string. Got an object of class numeric."
  )
  expect_error(
    uby_rolling_group_time("PT0s", "col", TRUE, "PT0s"),
    "'rev_time' must be a single string. Got an object of class logical."
  )
  expect_error(
    uby_rolling_group_time("PT0s", "col", c("Many", "strings"), "PT0s"),
    "'rev_time' must be a single string. Got a vector of length 2."
  )
})

test_that("uby_rolling_group_time fails nicely when 'fwd_time' is a bad type", {
  expect_error(
    uby_rolling_group_time("PT0s", "col", "PT0s", 5),
    "'fwd_time' must be a single string. Got an object of class numeric."
  )
  expect_error(
    uby_rolling_group_time("PT0s", "col", "PT0s", TRUE),
    "'fwd_time' must be a single string. Got an object of class logical."
  )
  expect_error(
    uby_rolling_group_time("PT0s", "col", "PT0s", c("Many", "strings")),
    "'fwd_time' must be a single string. Got a vector of length 2."
  )
})

test_that("uby_rolling_avg_tick fails nicely when 'cols' is a bad type", {
  expect_error(
    uby_rolling_avg_tick(5, 5),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    uby_rolling_avg_tick(TRUE, 5),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("uby_rolling_avg_tick fails nicely when 'rev_ticks' is a bad type", {
  expect_error(
    uby_rolling_avg_tick("column", "string"),
    "'rev_ticks' must be a single integer. Got an object of class character."
  )
  expect_error(
    uby_rolling_avg_tick("column", TRUE),
    "'rev_ticks' must be a single integer. Got an object of class logical."
  )
  expect_error(
    uby_rolling_avg_tick("column", c(1, 2, 3)),
    "'rev_ticks' must be a single integer. Got a vector of length 3."
  )
})

test_that("uby_rolling_avg_tick fails nicely when 'fwd_ticks' is a bad type", {
  expect_error(
    uby_rolling_avg_tick("column", 5, "string"),
    "'fwd_ticks' must be a single integer. Got an object of class character."
  )
  expect_error(
    uby_rolling_avg_tick("column", 5, TRUE),
    "'fwd_ticks' must be a single integer. Got an object of class logical."
  )
  expect_error(
    uby_rolling_avg_tick("column", 5, c(1, 2, 3)),
    "'fwd_ticks' must be a single integer. Got a vector of length 3."
  )
})

test_that("uby_rolling_avg_time fails nicely when 'ts_col' is a bad type", {
  expect_error(
    uby_rolling_avg_time(5, "col", "PT0s", "PT0s"),
    "'ts_col' must be a single string. Got an object of class numeric."
  )
  expect_error(
    uby_rolling_avg_time(TRUE, "col", "PT0s", "PT0s"),
    "'ts_col' must be a single string. Got an object of class logical."
  )
  expect_error(
    uby_rolling_avg_time(c("Many", "strings"), "col", "PT0s", "PT0s"),
    "'ts_col' must be a single string. Got a vector of length 2."
  )
})

test_that("uby_rolling_avg_time fails nicely when 'cols' is a bad type", {
  expect_error(
    uby_rolling_avg_time("PT0s", 5, "PT0s", "PT0s"),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    uby_rolling_avg_time("PT0s", TRUE, "PT0s", "PT0s"),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("uby_rolling_avg_time fails nicely when 'rev_time' is a bad type", {
  expect_error(
    uby_rolling_avg_time("PT0s", "col", 5, "PT0s"),
    "'rev_time' must be a single string. Got an object of class numeric."
  )
  expect_error(
    uby_rolling_avg_time("PT0s", "col", TRUE, "PT0s"),
    "'rev_time' must be a single string. Got an object of class logical."
  )
  expect_error(
    uby_rolling_avg_time("PT0s", "col", c("Many", "strings"), "PT0s"),
    "'rev_time' must be a single string. Got a vector of length 2."
  )
})

test_that("uby_rolling_avg_time fails nicely when 'fwd_time' is a bad type", {
  expect_error(
    uby_rolling_avg_time("PT0s", "col", "PT0s", 5),
    "'fwd_time' must be a single string. Got an object of class numeric."
  )
  expect_error(
    uby_rolling_avg_time("PT0s", "col", "PT0s", TRUE),
    "'fwd_time' must be a single string. Got an object of class logical."
  )
  expect_error(
    uby_rolling_avg_time("PT0s", "col", "PT0s", c("Many", "strings")),
    "'fwd_time' must be a single string. Got a vector of length 2."
  )
})

test_that("uby_rolling_min_tick fails nicely when 'cols' is a bad type", {
  expect_error(
    uby_rolling_min_tick(5, 5),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    uby_rolling_min_tick(TRUE, 5),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("uby_rolling_min_tick fails nicely when 'rev_ticks' is a bad type", {
  expect_error(
    uby_rolling_min_tick("column", "string"),
    "'rev_ticks' must be a single integer. Got an object of class character."
  )
  expect_error(
    uby_rolling_min_tick("column", TRUE),
    "'rev_ticks' must be a single integer. Got an object of class logical."
  )
  expect_error(
    uby_rolling_min_tick("column", c(1, 2, 3)),
    "'rev_ticks' must be a single integer. Got a vector of length 3."
  )
})

test_that("uby_rolling_min_tick fails nicely when 'fwd_ticks' is a bad type", {
  expect_error(
    uby_rolling_min_tick("column", 5, "string"),
    "'fwd_ticks' must be a single integer. Got an object of class character."
  )
  expect_error(
    uby_rolling_min_tick("column", 5, TRUE),
    "'fwd_ticks' must be a single integer. Got an object of class logical."
  )
  expect_error(
    uby_rolling_min_tick("column", 5, c(1, 2, 3)),
    "'fwd_ticks' must be a single integer. Got a vector of length 3."
  )
})

test_that("uby_rolling_min_time fails nicely when 'ts_col' is a bad type", {
  expect_error(
    uby_rolling_min_time(5, "col", "PT0s", "PT0s"),
    "'ts_col' must be a single string. Got an object of class numeric."
  )
  expect_error(
    uby_rolling_min_time(TRUE, "col", "PT0s", "PT0s"),
    "'ts_col' must be a single string. Got an object of class logical."
  )
  expect_error(
    uby_rolling_min_time(c("Many", "strings"), "col", "PT0s", "PT0s"),
    "'ts_col' must be a single string. Got a vector of length 2."
  )
})

test_that("uby_rolling_min_time fails nicely when 'cols' is a bad type", {
  expect_error(
    uby_rolling_min_time("PT0s", 5, "PT0s", "PT0s"),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    uby_rolling_min_time("PT0s", TRUE, "PT0s", "PT0s"),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("uby_rolling_min_time fails nicely when 'rev_time' is a bad type", {
  expect_error(
    uby_rolling_min_time("PT0s", "col", 5, "PT0s"),
    "'rev_time' must be a single string. Got an object of class numeric."
  )
  expect_error(
    uby_rolling_min_time("PT0s", "col", TRUE, "PT0s"),
    "'rev_time' must be a single string. Got an object of class logical."
  )
  expect_error(
    uby_rolling_min_time("PT0s", "col", c("Many", "strings"), "PT0s"),
    "'rev_time' must be a single string. Got a vector of length 2."
  )
})

test_that("uby_rolling_min_time fails nicely when 'fwd_time' is a bad type", {
  expect_error(
    uby_rolling_min_time("PT0s", "col", "PT0s", 5),
    "'fwd_time' must be a single string. Got an object of class numeric."
  )
  expect_error(
    uby_rolling_min_time("PT0s", "col", "PT0s", TRUE),
    "'fwd_time' must be a single string. Got an object of class logical."
  )
  expect_error(
    uby_rolling_min_time("PT0s", "col", "PT0s", c("Many", "strings")),
    "'fwd_time' must be a single string. Got a vector of length 2."
  )
})

test_that("uby_rolling_max_tick fails nicely when 'cols' is a bad type", {
  expect_error(
    uby_rolling_max_tick(5, 5),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    uby_rolling_max_tick(TRUE, 5),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("uby_rolling_max_tick fails nicely when 'rev_ticks' is a bad type", {
  expect_error(
    uby_rolling_max_tick("column", "string"),
    "'rev_ticks' must be a single integer. Got an object of class character."
  )
  expect_error(
    uby_rolling_max_tick("column", TRUE),
    "'rev_ticks' must be a single integer. Got an object of class logical."
  )
  expect_error(
    uby_rolling_max_tick("column", c(1, 2, 3)),
    "'rev_ticks' must be a single integer. Got a vector of length 3."
  )
})

test_that("uby_rolling_max_tick fails nicely when 'fwd_ticks' is a bad type", {
  expect_error(
    uby_rolling_max_tick("column", 5, "string"),
    "'fwd_ticks' must be a single integer. Got an object of class character."
  )
  expect_error(
    uby_rolling_max_tick("column", 5, TRUE),
    "'fwd_ticks' must be a single integer. Got an object of class logical."
  )
  expect_error(
    uby_rolling_max_tick("column", 5, c(1, 2, 3)),
    "'fwd_ticks' must be a single integer. Got a vector of length 3."
  )
})

test_that("uby_rolling_max_time fails nicely when 'ts_col' is a bad type", {
  expect_error(
    uby_rolling_max_time(5, "col", "PT0s", "PT0s"),
    "'ts_col' must be a single string. Got an object of class numeric."
  )
  expect_error(
    uby_rolling_max_time(TRUE, "col", "PT0s", "PT0s"),
    "'ts_col' must be a single string. Got an object of class logical."
  )
  expect_error(
    uby_rolling_max_time(c("Many", "strings"), "col", "PT0s", "PT0s"),
    "'ts_col' must be a single string. Got a vector of length 2."
  )
})

test_that("uby_rolling_max_time fails nicely when 'cols' is a bad type", {
  expect_error(
    uby_rolling_max_time("PT0s", 5, "PT0s", "PT0s"),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    uby_rolling_max_time("PT0s", TRUE, "PT0s", "PT0s"),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("uby_rolling_max_time fails nicely when 'rev_time' is a bad type", {
  expect_error(
    uby_rolling_max_time("PT0s", "col", 5, "PT0s"),
    "'rev_time' must be a single string. Got an object of class numeric."
  )
  expect_error(
    uby_rolling_max_time("PT0s", "col", TRUE, "PT0s"),
    "'rev_time' must be a single string. Got an object of class logical."
  )
  expect_error(
    uby_rolling_max_time("PT0s", "col", c("Many", "strings"), "PT0s"),
    "'rev_time' must be a single string. Got a vector of length 2."
  )
})

test_that("uby_rolling_max_time fails nicely when 'fwd_time' is a bad type", {
  expect_error(
    uby_rolling_max_time("PT0s", "col", "PT0s", 5),
    "'fwd_time' must be a single string. Got an object of class numeric."
  )
  expect_error(
    uby_rolling_max_time("PT0s", "col", "PT0s", TRUE),
    "'fwd_time' must be a single string. Got an object of class logical."
  )
  expect_error(
    uby_rolling_max_time("PT0s", "col", "PT0s", c("Many", "strings")),
    "'fwd_time' must be a single string. Got a vector of length 2."
  )
})

test_that("uby_rolling_prod_tick fails nicely when 'cols' is a bad type", {
  expect_error(
    uby_rolling_prod_tick(5, 5),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    uby_rolling_prod_tick(TRUE, 5),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("uby_rolling_prod_tick fails nicely when 'rev_ticks' is a bad type", {
  expect_error(
    uby_rolling_prod_tick("column", "string"),
    "'rev_ticks' must be a single integer. Got an object of class character."
  )
  expect_error(
    uby_rolling_prod_tick("column", TRUE),
    "'rev_ticks' must be a single integer. Got an object of class logical."
  )
  expect_error(
    uby_rolling_prod_tick("column", c(1, 2, 3)),
    "'rev_ticks' must be a single integer. Got a vector of length 3."
  )
})

test_that("uby_rolling_prod_tick fails nicely when 'fwd_ticks' is a bad type", {
  expect_error(
    uby_rolling_prod_tick("column", 5, "string"),
    "'fwd_ticks' must be a single integer. Got an object of class character."
  )
  expect_error(
    uby_rolling_prod_tick("column", 5, TRUE),
    "'fwd_ticks' must be a single integer. Got an object of class logical."
  )
  expect_error(
    uby_rolling_prod_tick("column", 5, c(1, 2, 3)),
    "'fwd_ticks' must be a single integer. Got a vector of length 3."
  )
})

test_that("uby_rolling_prod_time fails nicely when 'ts_col' is a bad type", {
  expect_error(
    uby_rolling_prod_time(5, "col", "PT0s", "PT0s"),
    "'ts_col' must be a single string. Got an object of class numeric."
  )
  expect_error(
    uby_rolling_prod_time(TRUE, "col", "PT0s", "PT0s"),
    "'ts_col' must be a single string. Got an object of class logical."
  )
  expect_error(
    uby_rolling_prod_time(c("Many", "strings"), "col", "PT0s", "PT0s"),
    "'ts_col' must be a single string. Got a vector of length 2."
  )
})

test_that("uby_rolling_prod_time fails nicely when 'cols' is a bad type", {
  expect_error(
    uby_rolling_prod_time("PT0s", 5, "PT0s", "PT0s"),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    uby_rolling_prod_time("PT0s", TRUE, "PT0s", "PT0s"),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("uby_rolling_prod_time fails nicely when 'rev_time' is a bad type", {
  expect_error(
    uby_rolling_prod_time("PT0s", "col", 5, "PT0s"),
    "'rev_time' must be a single string. Got an object of class numeric."
  )
  expect_error(
    uby_rolling_prod_time("PT0s", "col", TRUE, "PT0s"),
    "'rev_time' must be a single string. Got an object of class logical."
  )
  expect_error(
    uby_rolling_prod_time("PT0s", "col", c("Many", "strings"), "PT0s"),
    "'rev_time' must be a single string. Got a vector of length 2."
  )
})

test_that("uby_rolling_prod_time fails nicely when 'fwd_time' is a bad type", {
  expect_error(
    uby_rolling_prod_time("PT0s", "col", "PT0s", 5),
    "'fwd_time' must be a single string. Got an object of class numeric."
  )
  expect_error(
    uby_rolling_prod_time("PT0s", "col", "PT0s", TRUE),
    "'fwd_time' must be a single string. Got an object of class logical."
  )
  expect_error(
    uby_rolling_prod_time("PT0s", "col", "PT0s", c("Many", "strings")),
    "'fwd_time' must be a single string. Got a vector of length 2."
  )
})

test_that("uby_rolling_count_tick fails nicely when 'cols' is a bad type", {
  expect_error(
    uby_rolling_count_tick(5, 5),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    uby_rolling_count_tick(TRUE, 5),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("uby_rolling_count_tick fails nicely when 'rev_ticks' is a bad type", {
  expect_error(
    uby_rolling_count_tick("column", "string"),
    "'rev_ticks' must be a single integer. Got an object of class character."
  )
  expect_error(
    uby_rolling_count_tick("column", TRUE),
    "'rev_ticks' must be a single integer. Got an object of class logical."
  )
  expect_error(
    uby_rolling_count_tick("column", c(1, 2, 3)),
    "'rev_ticks' must be a single integer. Got a vector of length 3."
  )
})

test_that("uby_rolling_count_tick fails nicely when 'fwd_ticks' is a bad type", {
  expect_error(
    uby_rolling_count_tick("column", 5, "string"),
    "'fwd_ticks' must be a single integer. Got an object of class character."
  )
  expect_error(
    uby_rolling_count_tick("column", 5, TRUE),
    "'fwd_ticks' must be a single integer. Got an object of class logical."
  )
  expect_error(
    uby_rolling_count_tick("column", 5, c(1, 2, 3)),
    "'fwd_ticks' must be a single integer. Got a vector of length 3."
  )
})

test_that("uby_rolling_count_time fails nicely when 'ts_col' is a bad type", {
  expect_error(
    uby_rolling_count_time(5, "col", "PT0s", "PT0s"),
    "'ts_col' must be a single string. Got an object of class numeric."
  )
  expect_error(
    uby_rolling_count_time(TRUE, "col", "PT0s", "PT0s"),
    "'ts_col' must be a single string. Got an object of class logical."
  )
  expect_error(
    uby_rolling_count_time(c("Many", "strings"), "col", "PT0s", "PT0s"),
    "'ts_col' must be a single string. Got a vector of length 2."
  )
})

test_that("uby_rolling_count_time fails nicely when 'cols' is a bad type", {
  expect_error(
    uby_rolling_count_time("PT0s", 5, "PT0s", "PT0s"),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    uby_rolling_count_time("PT0s", TRUE, "PT0s", "PT0s"),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("uby_rolling_count_time fails nicely when 'rev_time' is a bad type", {
  expect_error(
    uby_rolling_count_time("PT0s", "col", 5, "PT0s"),
    "'rev_time' must be a single string. Got an object of class numeric."
  )
  expect_error(
    uby_rolling_count_time("PT0s", "col", TRUE, "PT0s"),
    "'rev_time' must be a single string. Got an object of class logical."
  )
  expect_error(
    uby_rolling_count_time("PT0s", "col", c("Many", "strings"), "PT0s"),
    "'rev_time' must be a single string. Got a vector of length 2."
  )
})

test_that("uby_rolling_count_time fails nicely when 'fwd_time' is a bad type", {
  expect_error(
    uby_rolling_count_time("PT0s", "col", "PT0s", 5),
    "'fwd_time' must be a single string. Got an object of class numeric."
  )
  expect_error(
    uby_rolling_count_time("PT0s", "col", "PT0s", TRUE),
    "'fwd_time' must be a single string. Got an object of class logical."
  )
  expect_error(
    uby_rolling_count_time("PT0s", "col", "PT0s", c("Many", "strings")),
    "'fwd_time' must be a single string. Got a vector of length 2."
  )
})

test_that("uby_rolling_std_tick fails nicely when 'cols' is a bad type", {
  expect_error(
    uby_rolling_std_tick(5, 5),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    uby_rolling_std_tick(TRUE, 5),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("uby_rolling_std_tick fails nicely when 'rev_ticks' is a bad type", {
  expect_error(
    uby_rolling_std_tick("column", "string"),
    "'rev_ticks' must be a single integer. Got an object of class character."
  )
  expect_error(
    uby_rolling_std_tick("column", TRUE),
    "'rev_ticks' must be a single integer. Got an object of class logical."
  )
  expect_error(
    uby_rolling_std_tick("column", c(1, 2, 3)),
    "'rev_ticks' must be a single integer. Got a vector of length 3."
  )
})

test_that("uby_rolling_std_tick fails nicely when 'fwd_ticks' is a bad type", {
  expect_error(
    uby_rolling_std_tick("column", 5, "string"),
    "'fwd_ticks' must be a single integer. Got an object of class character."
  )
  expect_error(
    uby_rolling_std_tick("column", 5, TRUE),
    "'fwd_ticks' must be a single integer. Got an object of class logical."
  )
  expect_error(
    uby_rolling_std_tick("column", 5, c(1, 2, 3)),
    "'fwd_ticks' must be a single integer. Got a vector of length 3."
  )
})

test_that("uby_rolling_std_time fails nicely when 'ts_col' is a bad type", {
  expect_error(
    uby_rolling_std_time(5, "col", "PT0s", "PT0s"),
    "'ts_col' must be a single string. Got an object of class numeric."
  )
  expect_error(
    uby_rolling_std_time(TRUE, "col", "PT0s", "PT0s"),
    "'ts_col' must be a single string. Got an object of class logical."
  )
  expect_error(
    uby_rolling_std_time(c("Many", "strings"), "col", "PT0s", "PT0s"),
    "'ts_col' must be a single string. Got a vector of length 2."
  )
})

test_that("uby_rolling_std_time fails nicely when 'cols' is a bad type", {
  expect_error(
    uby_rolling_std_time("PT0s", 5, "PT0s", "PT0s"),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    uby_rolling_std_time("PT0s", TRUE, "PT0s", "PT0s"),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("uby_rolling_std_time fails nicely when 'rev_time' is a bad type", {
  expect_error(
    uby_rolling_std_time("PT0s", "col", 5, "PT0s"),
    "'rev_time' must be a single string. Got an object of class numeric."
  )
  expect_error(
    uby_rolling_std_time("PT0s", "col", TRUE, "PT0s"),
    "'rev_time' must be a single string. Got an object of class logical."
  )
  expect_error(
    uby_rolling_std_time("PT0s", "col", c("Many", "strings"), "PT0s"),
    "'rev_time' must be a single string. Got a vector of length 2."
  )
})

test_that("uby_rolling_std_time fails nicely when 'fwd_time' is a bad type", {
  expect_error(
    uby_rolling_std_time("PT0s", "col", "PT0s", 5),
    "'fwd_time' must be a single string. Got an object of class numeric."
  )
  expect_error(
    uby_rolling_std_time("PT0s", "col", "PT0s", TRUE),
    "'fwd_time' must be a single string. Got an object of class logical."
  )
  expect_error(
    uby_rolling_std_time("PT0s", "col", "PT0s", c("Many", "strings")),
    "'fwd_time' must be a single string. Got a vector of length 2."
  )
})

test_that("uby_rolling_wavg_tick fails nicely when 'wcol' is a bad type", {
  expect_error(
    uby_rolling_wavg_tick(5, "column", 5),
    "'wcol' must be a single string. Got an object of class numeric."
  )
  expect_error(
    uby_rolling_wavg_tick(TRUE, "column", 5),
    "'wcol' must be a single string. Got an object of class logical."
  )
  expect_error(
    uby_rolling_wavg_tick(c("Many", "strings"), "column", 5),
    "'wcol' must be a single string. Got a vector of length 2."
  )
})

test_that("uby_rolling_wavg_tick fails nicely when 'cols' is a bad type", {
  expect_error(
    uby_rolling_wavg_tick("wcol", 5, 5),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    uby_rolling_wavg_tick("wcol", TRUE, 5),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("uby_rolling_wavg_tick fails nicely when 'rev_ticks' is a bad type", {
  expect_error(
    uby_rolling_wavg_tick("wcol", "column", "string"),
    "'rev_ticks' must be a single integer. Got an object of class character."
  )
  expect_error(
    uby_rolling_wavg_tick("wcol", "column", TRUE),
    "'rev_ticks' must be a single integer. Got an object of class logical."
  )
  expect_error(
    uby_rolling_wavg_tick("wcol", "column", c(1, 2, 3)),
    "'rev_ticks' must be a single integer. Got a vector of length 3."
  )
})

test_that("uby_rolling_wavg_tick fails nicely when 'fwd_ticks' is a bad type", {
  expect_error(
    uby_rolling_wavg_tick("wcol", "column", 5, "string"),
    "'fwd_ticks' must be a single integer. Got an object of class character."
  )
  expect_error(
    uby_rolling_wavg_tick("wcol", "column", 5, TRUE),
    "'fwd_ticks' must be a single integer. Got an object of class logical."
  )
  expect_error(
    uby_rolling_wavg_tick("wcol", "column", 5, c(1, 2, 3)),
    "'fwd_ticks' must be a single integer. Got a vector of length 3."
  )
})

test_that("uby_rolling_wavg_time fails nicely when 'ts_col' is a bad type", {
  expect_error(
    uby_rolling_wavg_time(5, "wcol", "col", "PT0s", "PT0s"),
    "'ts_col' must be a single string. Got an object of class numeric."
  )
  expect_error(
    uby_rolling_wavg_time(TRUE, "wcol", "col", "PT0s", "PT0s"),
    "'ts_col' must be a single string. Got an object of class logical."
  )
  expect_error(
    uby_rolling_wavg_time(c("Many", "strings"), "wcol", "col", "PT0s", "PT0s"),
    "'ts_col' must be a single string. Got a vector of length 2."
  )
})

test_that("uby_rolling_wavg_time fails nicely when 'wcol' is a bad type", {
  expect_error(
    uby_rolling_wavg_time("ts_col", 5, "col", "PT0s", "PT0s"),
    "'wcol' must be a single string. Got an object of class numeric."
  )
  expect_error(
    uby_rolling_wavg_time("ts_col", TRUE, "col", "PT0s", "PT0s"),
    "'wcol' must be a single string. Got an object of class logical."
  )
  expect_error(
    uby_rolling_wavg_time("ts_col", c("Many", "strings"), "col", "PT0s", "PT0s"),
    "'wcol' must be a single string. Got a vector of length 2."
  )
})

test_that("uby_rolling_wavg_time fails nicely when 'cols' is a bad type", {
  expect_error(
    uby_rolling_wavg_time("PT0s", "wcol", 5, "PT0s", "PT0s"),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    uby_rolling_wavg_time("PT0s", "wcol", TRUE, "PT0s", "PT0s"),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("uby_rolling_wavg_time fails nicely when 'rev_time' is a bad type", {
  expect_error(
    uby_rolling_wavg_time("PT0s", "wcol", "col", 5, "PT0s"),
    "'rev_time' must be a single string. Got an object of class numeric."
  )
  expect_error(
    uby_rolling_wavg_time("PT0s", "wcol", "col", TRUE, "PT0s"),
    "'rev_time' must be a single string. Got an object of class logical."
  )
  expect_error(
    uby_rolling_wavg_time("PT0s", "wcol", "col", c("Many", "strings"), "PT0s"),
    "'rev_time' must be a single string. Got a vector of length 2."
  )
})

test_that("uby_rolling_wavg_time fails nicely when 'fwd_time' is a bad type", {
  expect_error(
    uby_rolling_wavg_time("PT0s", "wcol", "col", "PT0s", 5),
    "'fwd_time' must be a single string. Got an object of class numeric."
  )
  expect_error(
    uby_rolling_wavg_time("PT0s", "wcol", "col", "PT0s", TRUE),
    "'fwd_time' must be a single string. Got an object of class logical."
  )
  expect_error(
    uby_rolling_wavg_time("PT0s", "wcol", "col", "PT0s", c("Many", "strings")),
    "'fwd_time' must be a single string. Got a vector of length 2."
  )
})
