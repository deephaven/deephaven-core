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

    df5 <- data.frame(X = c("A", "B", "A", "C", "B", "A", "B", "B", "C"),
                      Y = c("M", "N", "O", "N", "P", "M", "O", "P", "M"),
                      Number1 = c(100, -44, 49, 11, -66, 50, 29, 18, -70),
                      Number2 = c(-55, 76, 20, 130, 230, -50, 73, 137, 214))

    df6 <- data.frame(X = c("B", "C", "B", "A", "A", "C", "B", "C", "B", "A"),
                      Y = c("N", "N", "M", "P", "O", "P", "O", "N", "O", "O"),
                      Number1 = c(55, 72, 86, -45, 1, 0, 345, -65, 99, -5),
                      Number2 = c(76, 4, -6, 34, 12, -76, 45, -5, 34, 6))

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
    th5 <- client$import_table(df5)
    th6 <- client$import_table(df6)

    return(list("client" = client,
                "df1" = df1, "df2" = df2, "df3" = df3, "df4" = df4, "df5" = df5, "df6" = df6,
                "th1" = th1, "th2" = th2, "th3" = th3, "th4" = th4, "th5" = th5, "th6" = th6))
}

test_that("agg_first behaves as expected", {
})

test_that("agg_last behaves as expected", {
})

test_that("agg_min behaves as expected", {
})

test_that("agg_max behaves as expected", {
})

test_that("agg_sum behaves as expected", {
})

test_that("agg_abs_sum behaves as expected", {
})

test_that("agg_avg behaves as expected", {
})

test_that("agg_w_avg behaves as expected", {
})

test_that("agg_median behaves as expected", {
})

test_that("agg_var behaves as expected", {
})

test_that("agg_std behaves as expected", {
})

test_that("agg_percentile behaves as expected", {
})

test_that("agg_count behaves as expected", {
})