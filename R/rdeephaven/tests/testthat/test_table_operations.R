library(testthat)
library(rdeephaven)

setup <- function() {

    df1 <- data.frame(string_col = c("I", "am", "a", "string", "column"),
                int_col    = c(0, 1, 2, 3, 4),
                dbl_col    = c(1.65, 3.1234, 100000.5, 543.234567, 0.00))

    df2 <- data.frame(col1 = rep(3.14, 100),
                        col2 = rep("hello!", 100),
                        col3 = rnorm(100))

    df3 <- data.frame(matrix(rnorm(10 * 1000), nrow = 10))

    df4 <- data.frame(time_col = seq.POSIXt(as.POSIXct(Sys.Date()), as.POSIXct(Sys.Date()+30), by = "1 sec")[250000],
                    bool_col = sample(c(TRUE, FALSE), 250000, TRUE),
                    int_col = sample(0:10000, 250000, TRUE))

    # in order to test TableHandle, we need to have tables on the server that we know everything about.
    # thus, we have to push these created tables to the server and get TableHandles to each of them.
    # thus, we depend on the correctness of client$import_table(), ClientOptions$new(), and Client$new()

    # set up client
    client_options <- ClientOptions$new()
    client <- Client$new(target="localhost:10000", client_options=client_options)

    # move dataframes to server and get TableHandles for testing
    th1 <- client$import_table(df1)
    th2 <- client$import_table(df2)
    th3 <- client$import_table(df3)
    th4 <- client$import_table(df4)

    return(list("client" = client,
                "df1" = df1, "df2" = df2, "df3" = df3, "df4" = df4,
                "th1" = th1, "th2" = th2, "th3" = th3, "th4" = th4))
}

##### TESTING GOOD INPUTS #####

test_that("select behaves as expected", {
    data <- setup()
})

test_that("view behaves as expected", {
    data <- setup()
})

test_that("drop_columns behaves as expected", {
    data <- setup()
})

test_that("update behaves as expected", {
    data <- setup()
})

test_that("update_view behaves as expected", {
    data <- setup()
})

test_that("where behaves as expected", {
    data <- setup()
})

test_that("by behaves as expected", {
    data <- setup()
})

test_that("min_by behaves as expected", {
    data <- setup()
})

test_that("max_by behaves as expected", {
    data <- setup()
})

test_that("sum_by behaves as expected", {
    data <- setup()
})

test_that("abs_sum_by behaves as expected", {
    data <- setup()
})

test_that("var_by behaves as expected", {
    data <- setup()
})

test_that("std_by behaves as expected", {
    data <- setup()
})

test_that("avg_by behaves as expected", {
    data <- setup()
})

test_that("first_by behaves as expected", {
    data <- setup()
})

test_that("last_by behaves as expected", {
    data <- setup()
})

test_that("median_by behaves as expected", {
    data <- setup()
})

test_that("percentile_by behaves as expected", {
    data <- setup()
})

test_that("count_by behaves as expected", {
    data <- setup()
})

test_that("w_avg_by behaves as expected", {
    data <- setup()
})

test_that("tail_by behaves as expected", {
    data <- setup()
})

test_that("head_by behaves as expected", {
    data <- setup()
})

test_that("cross_join behaves as expected", {
    data <- setup()
})

test_that("natural_join behaves as expected", {
    data <- setup()
})

test_that("exact_join behaves as expected", {
    data <- setup()
})
