Rcpp::compileAttributes()
roxygen2::roxygenise()
install.packages("/home/user/rdeephaven", repos=NULL, type="source")

# load deephaven library
library(rdeephaven)

# connect to server using Client object
client <- DeephavenClient$new(target="192.168.1.241:10000")

# open DH table and convert to R dataframe
static_table <- client$open_table("static_table1")
data_frame1 <- static_table$to_data_frame()
data_frame1

# modify dataframe with regular R stuff
data_frame1$Name_Float_Col <- c(3.14, 2.71, 0.00)
data_frame1

# push new dataframe back to DH server and retrieve as DH table
new_static_table <- client$import_table(data_frame1)
new_static_table$bind_to_variable("static_table2")
new_static_table

# verify table was pushed
test <- client$open_table("static_table2")
test$to_data_frame()
