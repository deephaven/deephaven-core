Rcpp::compileAttributes()
install.packages("/home/user/rdeephaven", repos=NULL, type="source")

# load deephaven library
library(rdeephaven)

# connect to server using Client object
# I hate this api more than Rcpp hates default arguments
client <- new(Client, target="192.168.1.241:10000", auth_type="default", credentials=c("",""), session_type="python")

client$run_script("print('Hello world!')")

# open DH table and convert to arrow table
static_table <- client$open_table("static_table1")
arrow_table <- to_arrow_table(static_table)
data_frame <- as.data.frame(arrow_table)
data_frame
