CLIENT_ARGS <<- c("192.168.1.241:10000", "python", "default", "", "")

test_that("arrow_to_dh_table doesn't die on invalid arg types", {

    client <- setup_client(client_args=CLIENT_ARGS)
    df <- data.frame(col1 = rep(3.14, 100),
                     col2 = rep("hello!", 100),
                     col3 = rnorm(100))
    arrow_tbl <- arrow_table(df)

    #### testing first arg ####
    expect_error(arrow_to_dh_table(NULL, arrow_tbl, "test_table"))
    expect_error(arrow_to_dh_table(1, arrow_tbl, "test_table"))
    expect_error(arrow_to_dh_table("hello!", arrow_tbl, "test_table"))
    expect_error(arrow_to_dh_table(arrow_tbl, arrow_tbl, "test_table"))

    #### testing second arg ####
    expect_error(arrow_to_dh_table(client, NULL, "test_table"))
    expect_error(arrow_to_dh_table(client, 1, "test_table"))
    expect_error(arrow_to_dh_table(client, "hello!", "test_table"))
    expect_error(arrow_to_dh_table(client, client, "test_table"))

    #### testing third arg ####
    expect_error(arrow_to_dh_table(client, arrow_tbl, NULL))
    expect_error(arrow_to_dh_table(client, arrow_tbl, 1))
    expect_error(arrow_to_dh_table(client, arrow_tbl, client))
    expect_error(arrow_to_dh_table(client, arrow_tbl, arrow_tbl))
})


test_that("arrow_to_dh_table pushes tables to the server", {

    client <- setup_client(client_args=CLIENT_ARGS)
    df <- data.frame(col1 = rep(3.14, 100),
                     col2 = rep("hello!", 100),
                     col3 = rnorm(100))
    arrow_tbl <- arrow_table(df)

    expect_false(client$check_for_table("arrow_table1"))
    arrow_to_dh_table(client, arrow_tbl, "arrow_table1")
    expect_true(client$check_for_table("arrow_table1"))

    client$delete_table("arrow_table1") # this assumes correctness of client$delete_table
})


test_that("arrow_to_dh_table pushes the correct data to the server", {

    client <- setup_client(client_args=CLIENT_ARGS)
    df <- data.frame(time_col = seq.POSIXt(as.POSIXct(Sys.Date()), as.POSIXct(Sys.Date()+30), by = "1 sec")[2500000],
                      bool_col = sample(c(TRUE, FALSE), 2500000, TRUE), 
                      int_col = sample(0:100000, 2500000, TRUE))
    arrow_tbl <- arrow_table(df)

    pulled_table_handle <- arrow_to_dh_table(client, arrow_tbl, "arrow_table2")
    pulled_df <- dh_to_data_frame(pulled_table_handle) # this assumes correctness of dh_to_data_frame

    expect_equal(df, pulled_df)

    client$delete_table("arrow_table2") # this assumes correctness of client$delete_table
})
