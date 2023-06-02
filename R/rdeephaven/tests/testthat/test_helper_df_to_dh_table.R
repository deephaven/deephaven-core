CLIENT_ARGS <<- c("192.168.1.241:10000", "python", "default", "", "")

test_that("df_to_dh_table doesn't die on invalid arg types", {

    client <- setup_client(client_args=CLIENT_ARGS)
    df <- data.frame(col1 = rep(3.14, 100),
                     col2 = rep("hello!", 100),
                     col3 = rnorm(100))

    #### testing first arg ####
    expect_error(df_to_dh_table(NULL, df, "test_table"))
    expect_error(df_to_dh_table(1, df, "test_table"))
    expect_error(df_to_dh_table("hello!", df, "test_table"))
    expect_error(df_to_dh_table(df, df, "test_table"))

    #### testing second arg ####
    expect_error(df_to_dh_table(client, NULL, "test_table"))
    expect_error(df_to_dh_table(client, 1, "test_table"))
    expect_error(df_to_dh_table(client, "hello!", "test_table"))
    expect_error(df_to_dh_table(client, client, "test_table"))

    #### testing third arg ####
    expect_error(df_to_dh_table(client, df, NULL))
    expect_error(df_to_dh_table(client, df, 1))
    expect_error(df_to_dh_table(client, df, client))
    expect_error(df_to_dh_table(client, df, df))
})


test_that("df_to_dh_table pushes tables to the server", {

    client <- setup_client(client_args=CLIENT_ARGS)
    df <- data.frame(col1 = rep(3.14, 100),
                     col2 = rep("hello!", 100),
                     col3 = rnorm(100))

    expect_false(client$check_for_table("data_frame1")) # this assumes correctness of client$check_for_table
    df_to_dh_table(client, df, "data_frame1")
    expect_true(client$check_for_table("data_frame1")) # this assumes correctness of client$check_for_table

    client$delete_table("data_frame1") # this assumes correctness of client$delete_table
})


test_that("df_to_dh_table pushes the correct data to the server", {

    client <- setup_client(client_args=CLIENT_ARGS)
    df <- data.frame(time_col = seq.POSIXt(as.POSIXct(Sys.Date()), as.POSIXct(Sys.Date()+30), by = "1 sec")[2500000],
                      bool_col = sample(c(TRUE, FALSE), 2500000, TRUE), 
                      int_col = sample(0:100000, 2500000, TRUE))

    pulled_table_handle <- df_to_dh_table(client, df, "data_frame2")
    pulled_df <- dh_to_data_frame(pulled_table_handle) # this assumes correctness of dh_to_data_frame

    expect_equal(df, pulled_df)

    client$delete_table("data_frame2") # this assumes correctness of client$delete_table
})
