library(testthat)
library(rdeephaven)

##### TESTING BAD INPUTS #####

test_that("trying to instantiate an Aggregation fails nicely", {
    expect_error(Aggregation$new("hello"),
          "'aggregation' should be an internal Deephaven Aggregation. If you're seeing this,\n  you are trying to call the constructor of an Aggregation directly, which is not advised.\n  Please use one of the provided aggregation functions instead.")
})

test_that("agg_min fails nicely when 'columns' is a bad type", {
    expect_error(agg_min(5),
        "'columns' must be passed as a string or a vector of strings. Got an object of class numeric instead.")
    expect_error(agg_min(TRUE),
        "'columns' must be passed as a string or a vector of strings. Got an object of class logical instead.")
})

test_that("agg_max fails nicely when 'columns' is a bad type", {
    expect_error(agg_max(5),
        "'columns' must be passed as a string or a vector of strings. Got an object of class numeric instead.")
    expect_error(agg_max(TRUE),
        "'columns' must be passed as a string or a vector of strings. Got an object of class logical instead.")
})

test_that("agg_sum fails nicely when 'columns' is a bad type", {
    expect_error(agg_sum(5),
        "'columns' must be passed as a string or a vector of strings. Got an object of class numeric instead.")
    expect_error(agg_sum(TRUE),
        "'columns' must be passed as a string or a vector of strings. Got an object of class logical instead.")
})

test_that("agg_abs_sum fails nicely when 'columns' is a bad type", {
    expect_error(agg_abs_sum(5),
        "'columns' must be passed as a string or a vector of strings. Got an object of class numeric instead.")
    expect_error(agg_abs_sum(TRUE),
        "'columns' must be passed as a string or a vector of strings. Got an object of class logical instead.")
})

test_that("agg_avg fails nicely when 'columns' is a bad type", {
    expect_error(agg_avg(5),
        "'columns' must be passed as a string or a vector of strings. Got an object of class numeric instead.")
    expect_error(agg_avg(TRUE),
        "'columns' must be passed as a string or a vector of strings. Got an object of class logical instead.")
})

test_that("agg_w_avg fails nicely when 'weight_column' is a bad type", {
    expect_error(agg_w_avg(5, "string"),
        "'weight_column' must be passed as a single string. Got an object of class numeric instead.")
    expect_error(agg_w_avg(TRUE, "string"),
        "'weight_column' must be passed as a single string. Got an object of class logical instead.")
    expect_error(agg_w_avg(c("Multiple", "strings", "bad"), "string"),
        "'weight_column' must be passed as a single string. Got a character vector of length 3 instead.")
})

test_that("agg_w_avg fails nicely when 'columns' is a bad type", {
    expect_error(agg_w_avg("string", 5),
        "'columns' must be passed as a string or a vector of strings. Got an object of class numeric instead.")
    expect_error(agg_w_avg("string", TRUE),
        "'columns' must be passed as a string or a vector of strings. Got an object of class logical instead.")
})

test_that("agg_var fails nicely when 'columns' is a bad type", {
    expect_error(agg_var(5),
        "'columns' must be passed as a string or a vector of strings. Got an object of class numeric instead.")
    expect_error(agg_var(TRUE),
        "'columns' must be passed as a string or a vector of strings. Got an object of class logical instead.")
})

test_that("agg_std fails nicely when 'columns' is a bad type", {
    expect_error(agg_std(5),
        "'columns' must be passed as a string or a vector of strings. Got an object of class numeric instead.")
    expect_error(agg_std(TRUE),
        "'columns' must be passed as a string or a vector of strings. Got an object of class logical instead.")
})

test_that("agg_first fails nicely when 'columns' is a bad type", {
    expect_error(agg_first(5),
        "'columns' must be passed as a string or a vector of strings. Got an object of class numeric instead.")
    expect_error(agg_first(TRUE),
        "'columns' must be passed as a string or a vector of strings. Got an object of class logical instead.")
})

test_that("agg_last fails nicely when 'columns' is a bad type", {
    expect_error(agg_last(5),
        "'columns' must be passed as a string or a vector of strings. Got an object of class numeric instead.")
    expect_error(agg_last(TRUE),
        "'columns' must be passed as a string or a vector of strings. Got an object of class logical instead.")
})

test_that("agg_median fails nicely when 'columns' is a bad type", {
    expect_error(agg_median(5),
        "'columns' must be passed as a string or a vector of strings. Got an object of class numeric instead.")
    expect_error(agg_median(TRUE),
        "'columns' must be passed as a string or a vector of strings. Got an object of class logical instead.")
})

test_that("agg_percentile fails nicely when 'percentile' is bad", {
    expect_error(agg_percentile("string", "string"),
        "'percentile' must be a numeric type between 0 and 1 inclusive. Got an object of class character instead.")
    expect_error(agg_percentile(TRUE, "string"),
        "'percentile' must be a numeric type between 0 and 1 inclusive. Got an object of class logical instead.")
    expect_error(agg_percentile(5, "string"),
        "'percentile' must be a numeric type between 0 and 1 inclusive. Got a value of 5 instead.")
    expect_error(agg_percentile(c(5,6,7,8), "string"),
        "'percentile' must be a numeric type between 0 and 1 inclusive. Got a numeric vector of length 4 instead.")
})

test_that("agg_percentile fails nicely when 'columns' is a bad type", {
    expect_error(agg_percentile(0.5, 5),
        "'columns' must be passed as a string or a vector of strings. Got an object of class numeric instead.")
    expect_error(agg_percentile(0.5, TRUE),
        "'columns' must be passed as a string or a vector of strings. Got an object of class logical instead.")
})

test_that("agg_count fails nicely when 'count_column' is a bad type", {
    expect_error(agg_count(5),
        "'count_column' must be passed as a single string. Got an object of class numeric instead.")
    expect_error(agg_count(TRUE),
        "'count_column' must be passed as a single string. Got an object of class logical instead.")
    expect_error(agg_count(c("Many", "strings")),
        "'count_column' must be passed as a single string. Got a character vector of length 2 instead.")
})