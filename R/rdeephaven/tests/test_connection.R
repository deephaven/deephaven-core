#Rcpp::compileAttributes()
#install.packages("/home/user/rdeephaven", repos=NULL, type="source")

# load deephaven library
library(rdeephaven)

# connect to server using Client object
client <- new(Client, target="localhost:10000")

# open DH table and convert to arrow table
static_table <- client$open_table("static_table1")
arrow_table <- to_arrow_table(static_table)
data_frame <- as.data.frame(arrow_table)

static_table <- 0
