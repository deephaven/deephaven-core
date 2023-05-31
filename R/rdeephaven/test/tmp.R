Rcpp::compileAttributes()
install.packages("/home/user/rdeephaven", repos=NULL, type="source")

# load deephaven library
library(rdeephaven)

# connect to server using Client object
# I hate this api more than Rcpp hates default arguments
client <- new(Client, target="192.168.1.241:10000", auth_type="default", credentials=c("",""), session_type="python")

# open DH table and convert to R dataframe
static_table <- client$open_table("static_table1")
data_frame1 <- dh_to_data_frame(static_table)
data_frame1

# modify dataframe with regular R stuff
data_frame1$Name_Float_Col <- c(3.14, 2.71, 0.00)
data_frame1

# push new dataframe back to DH server and retrieve as DH table
new_static_table <- df_to_dh_table(client, data_frame1, "static_table2")
new_static_table
