CLIENT_ARGS <<- c("192.168.1.241:10000", "python", "default", "", "")

test_that("dh_to_rbs_reader doesn't die on invalid arg types", {

    client <- setup_client(client_args=CLIENT_ARGS)

    expect_error(dh_to_rbs_reader(NULL))
    expect_error(dh_to_rbs_reader(1))
    expect_error(dh_to_rbs_reader("hello!"))

    rm(client) # just in case
})


test_that("dh_to_rbs_reader correctly pulls data from the server", {

    dataframes <- generate_dataframes()
    client <- setup_client(client_args=CLIENT_ARGS, dataframes=dataframes) # passing dataframes pushes them to the server, assumes correctness of client$df_to_dh_table
    
    # ensure server-side tables for testing actually exist, not really the point of this testing block but can only be done in this scope
    expect_true(client$check_for_table("table1"))
    expect_true(client$check_for_table("table2"))
    expect_true(client$check_for_table("table3"))
    expect_true(client$check_for_table("table4")) # these all assume correctness of client$check_for_table

    # get tables as deephaven TableHandles
    th1 <- client$open_table("table1")
    th2 <- client$open_table("table2")
    th3 <- client$open_table("table3")
    th4 <- client$open_table("table4")

    # convert to record batch readers
    rbr1 <- dh_to_rbs_reader(th1)
    rbr2 <- dh_to_rbs_reader(th2)
    rbr3 <- dh_to_rbs_reader(th3)
    rbr4 <- dh_to_rbs_reader(th4)

    expect_equal(rbr1, as_record_batch_reader(arrow_table(dataframes[[1]])))
    expect_equal(rbr2, as_record_batch_reader(arrow_table(dataframes[[2]])))
    expect_equal(rbr3, as_record_batch_reader(arrow_table(dataframes[[3]])))
    expect_equal(rbr4, as_record_batch_reader(arrow_table(dataframes[[4]])))

    client$delete_table("table1")
    client$delete_table("table2")
    client$delete_table("table3")
    client$delete_table("table4") # these all assume correctness of client$delete_table

    rm(th1, th2, th3, th4, client) # just in case
})
