generate_dataframes <- function() {
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

    return(list(df1, df2, df3, df4))
}

setup_client <- function(target, dataframes=NULL) {

    client_options <- ClientOptions$new()
    client_options$set_default_authentication()
    client <- Client$new(target=target, client_options=client_options))

    if (!is.null(dataframes)) {
        for (i in 1:length(dataframes)) {
            th = client$import_table(dataframes[i])
            th$bind_to_variable(paste0("table", i))
        }
    }
    return(client)
}
