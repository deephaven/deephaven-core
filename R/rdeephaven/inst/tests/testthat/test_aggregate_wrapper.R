library(testthat)
library(rdeephaven)

##### TESTING BAD INPUTS #####

test_that("agg_min fails nicely when 'cols' is a bad type", {
  expect_error(
    agg_min(5),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    agg_min(TRUE),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("agg_max fails nicely when 'cols' is a bad type", {
  expect_error(
    agg_max(5),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    agg_max(TRUE),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("agg_sum fails nicely when 'cols' is a bad type", {
  expect_error(
    agg_sum(5),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    agg_sum(TRUE),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("agg_abs_sum fails nicely when 'cols' is a bad type", {
  expect_error(
    agg_abs_sum(5),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    agg_abs_sum(TRUE),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("agg_avg fails nicely when 'cols' is a bad type", {
  expect_error(
    agg_avg(5),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    agg_avg(TRUE),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("agg_w_avg fails nicely when 'wcol' is a bad type", {
  expect_error(
    agg_w_avg(5, "string"),
    "'wcol' must be a single string. Got an object of class numeric."
  )
  expect_error(
    agg_w_avg(TRUE, "string"),
    "'wcol' must be a single string. Got an object of class logical."
  )
  expect_error(
    agg_w_avg(c("Multiple", "strings", "bad"), "string"),
    "'wcol' must be a single string. Got a vector of length 3."
  )
})

test_that("agg_w_avg fails nicely when 'cols' is a bad type", {
  expect_error(
    agg_w_avg("string", 5),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    agg_w_avg("string", TRUE),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("agg_var fails nicely when 'cols' is a bad type", {
  expect_error(
    agg_var(5),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    agg_var(TRUE),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("agg_std fails nicely when 'cols' is a bad type", {
  expect_error(
    agg_std(5),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    agg_std(TRUE),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("agg_first fails nicely when 'cols' is a bad type", {
  expect_error(
    agg_first(5),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    agg_first(TRUE),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("agg_last fails nicely when 'cols' is a bad type", {
  expect_error(
    agg_last(5),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    agg_last(TRUE),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("agg_median fails nicely when 'cols' is a bad type", {
  expect_error(
    agg_median(5),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    agg_median(TRUE),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("agg_percentile fails nicely when 'percentile' is bad", {
  expect_error(
    agg_percentile("string", "string"),
    "'percentile' must be a single numeric. Got an object of class character."
  )
  expect_error(
    agg_percentile(TRUE, "string"),
    "'percentile' must be a single numeric. Got an object of class logical."
  )
  expect_error(
    agg_percentile(5, "string"),
    "'percentile' must be between 0 and 1 inclusive. Got 'percentile' = 5."
  )
  expect_error(
    agg_percentile(c(5, 6, 7, 8), "string"),
    "'percentile' must be a single numeric. Got a vector of length 4."
  )
})

test_that("agg_percentile fails nicely when 'cols' is a bad type", {
  expect_error(
    agg_percentile(0.5, 5),
    "'cols' must be a string or a vector of strings. Got an object of class numeric."
  )
  expect_error(
    agg_percentile(0.5, TRUE),
    "'cols' must be a string or a vector of strings. Got an object of class logical."
  )
})

test_that("agg_count fails nicely when 'col' is a bad type", {
  expect_error(
    agg_count(5),
    "'col' must be a single string. Got an object of class numeric."
  )
  expect_error(
    agg_count(TRUE),
    "'col' must be a single string. Got an object of class logical."
  )
  expect_error(
    agg_count(c("Many", "strings")),
    "'col' must be a single string. Got a vector of length 2."
  )
})
