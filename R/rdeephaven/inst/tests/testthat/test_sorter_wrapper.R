library(testthat)
library(rdeephaven)

##### TESTING BAD INPUTS #####

test_that("trying to instantiate a Sorter fails nicely", {
    expect_error(Sorter$new("hello"),
          "'sort_pair' should be an internal Deephaven SortPair. If you're seeing this,\n  you are trying to call the constructor of a Sorter directly, which is not advised.\n  Please use one of the provided sorting functions instead.")
})

test_that("sort_asc fails when 'column' is a bad type", {
    expect_error(sort_asc(5),
        "'column' must be passed as a single string. Got an object of class numeric instead.")
    expect_error(sort_asc(TRUE),
        "'column' must be passed as a single string. Got an object of class logical instead.")
    expect_error(sort_asc(c("a", "b", "c", "d")),
        "'column' must be passed as a single string. Got a character vector of length 4 instead.")
})

test_that("sort_asc fails when 'abs' is a bad type", {
    expect_error(sort_asc("string", abs=1),
        "'abs' must be passed as a single boolean. Got an object of class numeric instead.")
    expect_error(sort_asc("string", abs="hello!"),
        "'abs' must be passed as a single boolean. Got an object of class character instead.")
    expect_error(sort_asc("string", abs=c(TRUE, TRUE, FALSE)),
        "'abs' must be passed as a single boolean. Got a boolean vector of length 3 instead.")
})

test_that("sort_desc fails when 'column' is a bad type", {
    expect_error(sort_desc(5),
        "'column' must be passed as a single string. Got an object of class numeric instead.")
    expect_error(sort_desc(TRUE),
        "'column' must be passed as a single string. Got an object of class logical instead.")
    expect_error(sort_desc(c("a", "b", "c", "d")),
        "'column' must be passed as a single string. Got a character vector of length 4 instead.")
})

test_that("sort_desc fails when 'abs' is a bad type", {
    expect_error(sort_desc("string", abs=1),
        "'abs' must be passed as a single boolean. Got an object of class numeric instead.")
    expect_error(sort_desc("string", abs="hello!"),
        "'abs' must be passed as a single boolean. Got an object of class character instead.")
    expect_error(sort_desc("string", abs=c(TRUE, TRUE, FALSE)),
        "'abs' must be passed as a single boolean. Got a boolean vector of length 3 instead.")
})