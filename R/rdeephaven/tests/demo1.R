# load required libraries
library(Rcpp)
library(arrow)
library(rdeephaven)

# connect to server using Client object
client <- new(Client, target="localhost:10000")

# pull static table and print
static_table <- client$open_table("static_table")
static_table$print()

# chain table operations together, can use vectors of queries
downstream_table <- static_table$
                        select("Name_Int_Col")$
                        update(c("New_Col1 = Name_Int_Col * 2", "New_Col2 = New_Col1 - 1"))
downstream_table$print()

# updates to server table do not yet get pulled automatically, must update manually
static_table <- client$open_table("static_table")
static_table$print()

# can also pull dynamic tables
ticking_table <- client$open_table("updating_table")
ticking_table$print()
ticking_table$print()
ticking_table$print()
ticking_table$print()

# updates to ticking tables "work", but there's a bug here
downstream_ticking_table <- ticking_table$update(c("New_Col1 = Row + 1", "New_Col2 = Some_Int * 2"))
downstream_ticking_table$print()
downstream_ticking_table$print()
downstream_ticking_table$print()
downstream_ticking_table$print()
