library(testthat)

# Load the file containing the TableHandle class definition
source("path_to_tablehandle.R")

# Define the tests
test_that("TableHandle class testing good inputs", {

    # Set up the test environment
    setup <- function() {

        # Create known data frames for testing
        df1 <- data.frame(string_col = c("I", "am", "a", "string", "column"),
                    int_col    = c(0, 1, 2, 3, 4),
                    dbl_col    = c(1.65, 3.1234, 100000.5, 543.234567, 0.00))

        df2 <- data.frame(col1 = rep(3.14, 100),
                            col2 = rep("hello!", 100),
                            col3 = rnorm(100))

        df3 <- data.frame(matrix(rnorm(10 * 1000), nrow = 10))

        df4 <- data.frame(time_col = seq.POSIXt(as.POSIXct(Sys.Date()), as.POSIXct(Sys.Date()+30), by = "1 sec")[2500000],
                        bool_col = sample(c(TRUE, FALSE), 2500000, TRUE), 
                        int_col = sample(0:100000, 2500000, TRUE))

        # in order to test TableHandle, we need to have tables on the server that we know everything about.
        # thus, we have to push these created tables to the server and get TableHandles to each of them.
        # thus, we depend on the correctness of client$import_table(), ClientOptions$new(), and Client$new()
        
        # set up client
        client_options <- ClientOptions$new()
        client <- Client$new(target="localhost:10000", client_options=client_options)

        # move dataframes to server and get TableHandles for testing
        th1 <- Client$import_table(df1)
        th2 <- Client$import_table(df2)
        th3 <- Client$import_table(df3)
        th4 <- Client$import_table(df4)

        return(list("client" = client,
                    "df1" = df1, "df2" = df2, "df3" = df3, "df4" = df4,
                    "th1" = th1, "th2" = th2, "th3" = th3, "th4" = th4))
    }

    # Test the `is_static` method
    test_that("is_static returns the correct value", {
        data <- setup()

        expect_false(data$th1$is_static())
        expect_false(data$th2$is_static())
        expect_false(data$th3$is_static())
        expect_false(data$th4$is_static())

        # TODO: test ticking tables when they can be created from R
    })

    # Test the `nrow` method
        test_that("nrow returns the correct number of rows", {
        data <- setup()

        expect_equal(data$th1$nrow(), nrow(data$df1))
        expect_equal(data$th2$nrow(), nrow(data$df2))
        expect_equal(data$th3$nrow(), nrow(data$df3))
        expect_equal(data$th4$nrow(), nrow(data$df4))

        # TODO: test nrow(data$th) when it is implemented
    })

    # Test the `bind_to_variable` method
    test_that("bind_to_variable binds the table to a variable", {
        data <- setup()

        data$th1$bind_to_variable("table1")
        new_th1 <- data$client$open_table("table1")
        expect_equal(new_th1, data$th1) # this is probably dumb

        data$th2$bind_to_variable("table2")
        new_th2 <- data$client$open_table("table2")
        expect_equal(new_th2, data$th2) # this is probably dumb

        data$th3$bind_to_variable("table3")
        new_th3 <- data$client$open_table("table3")
        expect_equal(new_th3, data$th3) # this is probably dumb

        data$th4$bind_to_variable("table4")
        new_th4 <- data$client$open_table("table4")
        expect_equal(new_th4, data$th4) # this is probably dumb

        # TODO: figure out how to test equality of table handles
    })

    # Test the `to_arrow_record_batch_stream_reader` method
    test_that("to_arrow_record_batch_stream_reader returns a valid stream reader", {
        data <- setup()

        rbr1 <- data$th1$to_arrow_record_batch_stream_reader()
        expected_rbr1 <- as_record_batch_reader(arrow_table(data$df1))
        expect_equal(rbr1, expected_rbr1)
        
        rbr2 <- data$th2$to_arrow_record_batch_stream_reader()
        expected_rbr2 <- as_record_batch_reader(arrow_table(data$df2))
        expect_equal(rbr2, expected_rbr2)

        rbr3 <- data$th3$to_arrow_record_batch_stream_reader()
        expected_rbr3 <- as_record_batch_reader(arrow_table(data$df3))
        expect_equal(rbr3, expected_rbr3)

        rbr4 <- data$th4$to_arrow_record_batch_stream_reader()
        expected_rbr4 <- as_record_batch_reader(arrow_table(data$df4))
        expect_equal(rbr4, expected_rbr4)
    })

    # Test the `to_arrow_table` method
    test_that("to_arrow_table returns a valid Arrow table", {
        data <- setup()

        arrow_tbl1 <- data$th1$to_arrow_table()
        expected_arrow_tbl1 <- arrow_table(data$df1)
        expect_equal(arrow_tbl1, expected_arrow_tbl1)
        
        arrow_tbl2 <- data$th2$to_arrow_table()
        expected_arrow_tbl2 <- arrow_table(data$df2)
        expect_equal(arrow_tbl2, expected_arrow_tbl2)

        arrow_tbl3 <- data$th3$to_arrow_table()
        expected_arrow_tbl3 <- arrow_table(data$df3)
        expect_equal(arrow_tbl3, expected_arrow_tbl3)

        arrow_tbl4 <- data$th4$to_arrow_table()
        expected_arrow_tbl4 <- arrow_table(data$df4)
        expect_equal(arrow_tbl4, expected_arrow_tbl4)
    })

    # Test the `to_tibble` method
    test_that("to_tibble returns a valid Tibble", {
        data <- setup()

        tibble1 <- data$th1$to_tibble()
        expected_tibble1 <- as_tibble(data$df1)
        expect_equal(tibble1, expected_tibble1)
        
        tibble2 <- data$th2$to_arrow_table()
        expected_tibble2 <- arrow_table(data$df2)
        expect_equal(tibble2, expected_tibble2)

        tibble3 <- data$th3$to_arrow_table()
        expected_tibble3 <- arrow_table(data$df3)
        expect_equal(tibble3, expected_tibble3)

        tibble4 <- data$th4$to_arrow_table()
        expected_tibble4 <- arrow_table(data$df4)
        expect_equal(tibble4, expected_tibble4)
    })

    # Test the `to_data_frame` method
    test_that("to_data_frame returns a valid data frame", {
        data <- setup()
        # Add assertions to check the behavior of the method

        # Add more test cases for different scenarios
    })
})

# Run the tests
test_file("path_to_tablehandle_tests.R")