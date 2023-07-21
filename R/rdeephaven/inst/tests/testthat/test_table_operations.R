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

    return(list("client" = client,
                "df1" = df1, "df2" = df2, "df3" = df3, "df4" = df4, "df5" = df5,
                "th1" = th1, "th2" = th2, "th3" = th3, "th4" = th4, "th5" = th5))
}

##### TESTING GOOD INPUTS #####

test_that("select behaves as expected", {
    data <- setup()
    
    expect_equal(data$th1$select("string_col")$to_data_frame(), data$df1["string_col"])
    
    expect_equal(data$th2$select(c("col2", "col3"))$to_data_frame(), data$df2[c("col2", "col3")])
    
    renamed_col_df3 <- data$df3[c("X1", "X2")]
    colnames(renamed_col_df3)[1] = "first_col"
    expect_equal(data$th3$select(c("first_col = X1", "X2"))$to_data_frame(), renamed_col_df3)
    
    new_col_df4 <- data$df4["int_col"] + 1
    colnames(new_col_df4) = "new_col"
    expect_equal(data$th4$select("new_col = int_col + 1")$to_data_frame(), new_col_df4)
    
})

test_that("view behaves as expected", {
    data <- setup()
    
    expect_equal(data$th1$view("string_col")$to_data_frame(), data$df1["string_col"])
    
    expect_equal(data$th2$view(c("col2", "col3"))$to_data_frame(), data$df2[c("col2", "col3")])
    
    renamed_col_df3 <- data$df3[c("X1", "X2")]
    colnames(renamed_col_df3)[1] = "first_col"
    expect_equal(data$th3$view(c("first_col = X1", "X2"))$to_data_frame(), renamed_col_df3)
    
    new_col_df4 <- data$df4["int_col"] + 1
    colnames(new_col_df4) = "new_col"
    expect_equal(data$th4$view("new_col = int_col + 1")$to_data_frame(), new_col_df4)
})

test_that("update behaves as expected", {
    data <- setup()
    
    new_df1 <- data$df1
    new_df1["dbl_col_again"] = new_df1["dbl_col"]
    expect_equal(data$th1$update("dbl_col_again = dbl_col")$to_data_frame(), new_df1)
    
    new_df2 <- data$df2
    new_df2["col4"] = new_df2["col3"] * 2
    expect_equal(data$th2$update("col4 = col3 * 2")$to_data_frame(), new_df2)
    
    new_df3 <- data$df3
    new_df3["X1001"] = new_df3["X1000"]
    new_df3["X1002"] = new_df3["X1001"]
    expect_equal(data$th3$update(c("X1001 = X1000", "X1002 = X1001"))$to_data_frame(), new_df3)
    
    new_df4 <- data$df4
    new_df4["new_col"] = sqrt(3 * new_df4["int_col"])
    expect_equal(data$th4$update("new_col = sqrt(3 * int_col)")$to_data_frame(), new_df4)
})

test_that("update_view behaves as expected", {
    data <- setup()
    
    new_df1 <- data$df1
    new_df1["dbl_col_again"] = new_df1["dbl_col"]
    expect_equal(data$th1$update_view("dbl_col_again = dbl_col")$to_data_frame(), new_df1)
    
    new_df2 <- data$df2
    new_df2["col4"] = new_df2["col3"] * 2
    expect_equal(data$th2$update_view("col4 = col3 * 2")$to_data_frame(), new_df2)
    
    new_df3 <- data$df3
    new_df3["X1001"] = new_df3["X1000"]
    new_df3["X1002"] = new_df3["X1001"]
    expect_equal(data$th3$update_view(c("X1001 = X1000", "X1002 = X1001"))$to_data_frame(), new_df3)
    
    new_df4 <- data$df4
    new_df4["new_col"] = sqrt(3 * new_df4["int_col"])
    expect_equal(data$th4$update_view("new_col = sqrt(3 * int_col)")$to_data_frame(), new_df4)
})

test_that("drop_columns behaves as expected", {
  data <- setup()
  
  expect_equal(data$th1$drop_columns("string_col")$to_data_frame(), data$df1[c("int_col", "dbl_col")])
  
  expect_equal(data$th2$drop_columns(c("col1", "col2"))$to_data_frame(), data$df2["col3"])
  
  expect_equal(data$th3$drop_columns(paste0("X", seq(2, 1000)))$to_data_frame(), data$df3["X1"])
})

test_that("where behaves as expected", {
    data <- setup()

    expect_equal(data$th1$where("int_col < 3")$to_data_frame(), data$df1[-c(4,5),])
    
    expect_equal(data$th2$where("col2 == `hello!`")$to_data_frame(), data$df2)
    
    new_df3 <- data$df3[data$df3["X1"] - data$df3["X4"] + data$df3["X8"] + data$df3["X32"] - 2*data$df3["X5"] >= 0,]
    rownames(new_df3) <- NULL
    expect_equal(data$th3$where("X1 - X4 + X8 + X32 - 2*X5 >= 0")$to_data_frame(), new_df3)
})

test_that("min_by behaves as expected", {
    data <- setup()
    
    expect_equal(data$th1$min_by("int_col")$to_data_frame(), data$df1[c("int_col", "string_col", "dbl_col")])
    
    new_df2 <- data$df2 %>%
      group_by(col2) %>%
      summarise(across(everything(), min))
    expect_equal(data$th2$min_by("col2")$to_data_frame(), as.data.frame(new_df2))
    
    new_df3 <- data$df3 %>%
      mutate(bool_col1 = X1 >= 0, bool_col2 = X2 >= 0) %>%
      group_by(bool_col1, bool_col2) %>%
      summarise(across(everything(), min)) %>%
      arrange(bool_col1, bool_col2) # need to sort because resulting row orders are not the same
    new_th3 <- data$th3$
      update(c("bool_col1 = X1 >= 0", "bool_col2 = X2 >= 0"))$
      min_by(c("bool_col1", "bool_col2"))$
      sort(c(sort_asc("bool_col1"), sort_asc("bool_col2"))) # need to sort because resulting row orders are not the same
    expect_equal(new_th3$to_data_frame(), as.data.frame(new_df3))
    
    new_df4 <- data$df4 %>%
      group_by(bool_col) %>%
      summarise(across(everything(), min)) %>%
      arrange(bool_col)
    new_th4 <- data$th4$
      min_by("bool_col")$
      sort(sort_asc("bool_col"))
    expect_equal(new_th4$to_data_frame(), as.data.frame(new_df4))
})

test_that("max_by behaves as expected", {
    data <- setup()
    
    expect_equal(data$th1$max_by("int_col")$to_data_frame(), data$df1[c("int_col", "string_col", "dbl_col")])
    
    new_df2 <- data$df2 %>%
      group_by(col2) %>%
      summarise(across(everything(), max))
    expect_equal(data$th2$max_by("col2")$to_data_frame(), as.data.frame(new_df2))
    
    new_df3 <- data$df3 %>%
      mutate(bool_col1 = X1 >= 0, bool_col2 = X2 >= 0) %>%
      group_by(bool_col1, bool_col2) %>%
      summarise(across(everything(), max)) %>%
      arrange(bool_col1, bool_col2) # need to sort because resulting row orders are not the same
    new_th3 <- data$th3$
      update(c("bool_col1 = X1 >= 0", "bool_col2 = X2 >= 0"))$
      max_by(c("bool_col1", "bool_col2"))$
      sort(c(sort_asc("bool_col1"), sort_asc("bool_col2"))) # need to sort because resulting row orders are not the same
    expect_equal(new_th3$to_data_frame(), as.data.frame(new_df3))
    
    new_df4 <- data$df4 %>%
      group_by(bool_col) %>%
      summarise(across(everything(), max)) %>%
      arrange(bool_col)
    new_th4 <- data$th4$
      max_by("bool_col")$
      sort(sort_asc("bool_col"))
    expect_equal(new_th4$to_data_frame(), as.data.frame(new_df4))
})

test_that("sum_by behaves as expected", {
    data <- setup()
    
    new_df5_1 <- data$df5 %>%
      select(-Y) %>%
      group_by(X) %>%
      summarise(across(everything(), sum))
    new_th5_1 <- data$th5$
      drop_columns("Y")$
      sum_by("X")
    expect_equal(new_th5_1$to_data_frame(), as.data.frame(new_df5_1))
    
    new_df5_2 <- data$df5 %>%
      group_by(X, Y) %>%
      summarise(across(everything(), sum)) %>%
      arrange(X, Y)
    new_th5_2 <- data$th5$
      sum_by(c("X", "Y"))$
      sort(c(sort_asc("X"), sort_asc("Y")))
    expect_equal(new_th5_2$to_data_frame(), as.data.frame(new_df5_2))
})

test_that("abs_sum_by behaves as expected", {
    data <- setup()
    
    new_df5_1 <- data$df5 %>%
      mutate(Number1 = abs(Number1), Number2 = abs(Number2)) %>%
      select(-Y) %>%
      group_by(X) %>%
      summarise(across(everything(), sum))
    new_th5_1 <- data$th5$
      drop_columns("Y")$
      abs_sum_by("X")
    expect_equal(new_th5_1$to_data_frame(), as.data.frame(new_df5_1))
    
    new_df5_2 <- data$df5 %>%
      mutate(Number1 = abs(Number1), Number2 = abs(Number2)) %>%
      group_by(X, Y) %>%
      summarise(across(everything(), sum)) %>%
      arrange(X, Y)
    new_th5_2 <- data$th5$
      abs_sum_by(c("X", "Y"))$
      sort(c(sort_asc("X"), sort_asc("Y")))
    expect_equal(new_th5_2$to_data_frame(), as.data.frame(new_df5_2))
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
