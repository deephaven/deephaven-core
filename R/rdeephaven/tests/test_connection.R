Rcpp::compileAttributes()
install.packages("/home/user/rdeephaven", repos=NULL, type="source")

# load deephaven library
library(rdeephaven)

# connect to server using Client object
client <- new(Client, target="localhost:10000")


###################################################

static_table1 <- client$open_table("static_table1")
static_table1$print_stream()

ptr1 <- static_table1$get_stream()
ptr1
rbr1 <- RecordBatchStreamReader$import_from_c(ptr1)

arrow_table1 <- rbr1$read_table()
arrow_table1

data_frame1 <- as.data.frame(arrow_table1)
data_frame1

static_table1$print_stream()
static_table1

############

static_table2 <- client$open_table("static_table2")
static_table2$print_stream()

ptr2 <- static_table2$get_stream()
ptr2
rbr2 <- RecordBatchStreamReader$import_from_c(ptr2)

arrow_table2 <- rbr2$read_table()
arrow_table2

data_frame2 <- as.data.frame(arrow_table2)
data_frame2

static_table2$print_stream()
static_table2

############

static_table3 <- client$open_table("static_table3")
static_table3$print_stream()

ptr3 <- static_table3$get_stream()
ptr3
rbr3 <- RecordBatchStreamReader$import_from_c(ptr3)

arrow_table3 <- rbr3$read_table()
arrow_table3

data_frame3 <- as.data.frame(arrow_table3)
data_frame3

static_table3$print_stream()
static_table3

############

static_table4 <- client$open_table("static_table4")
static_table4$print_stream()

ptr4 <- static_table4$get_stream()
ptr4
rbr4 <- RecordBatchStreamReader$import_from_c(ptr4)

arrow_table4 <- rbr4$read_table()
arrow_table4

data_frame4 <- as.data.frame(arrow_table4)
data_frame4

static_table4$print_stream()
static_table4
