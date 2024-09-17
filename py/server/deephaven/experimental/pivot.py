import jpy
import random
from deephaven.table import Table
from deephaven import time_table, empty_table
import deephaven.table
from deephaven.table import multi_join
from deephaven import numpy as dhnp
import re
from deephaven.server.executors import submit_task
from typing import Sequence, List, Union, Protocol

# Instead of legalizing the column name, it would be preferable to assign known names, and index them by the names table; using some kind of display name.
# We don't know that we'll actually replace stuff to be unique here, which means this is not actually correct.
def legalize_column(s : str):
    if re.match("^[_a-zA-Z][_a-zA-Z0-9]*$", s):
        return s
    if re.match("^[_a-zA-Z].*$", s):
        return re.sub("[^_a-zA-Z0-9]", "_", s)
    return "_" + re.sub("[^_a-zA-Z0-9]", "_", s)

def do_multijoin(partitions_table, row_column_names : list[str], col_column_name : str, value_column_name : str):
    # Define the columns for a multi-join

    keys = partitions_table.keys()

    key_values = dhnp.to_numpy(table=keys, cols=[col_column_name])

    if len(key_values) == 0:
        return empty_table(0)

    tables = []
    ki = 0

    for con in partitions_table.constituent_tables:
        tables.append(con.view(row_column_names + ["%s=%s" % (legalize_column(str(key_values[ki][0])), value_column_name)]))
        ki = ki + 1

    return multi_join(input=tables, on=row_column_names).table()

def pivot(source, row_column_names : list[str], col_column_name : str, value_column_name : str):
    # Partition the source by column
    partitioned_source=source.partition_by(col_column_name)
    pvt = do_multijoin(partitioned_source, row_column_names, col_column_name, value_column_name)
    return pvt


# # Java wrappers
# random_class = jpy.get_type("java.util.Random")
# random_inst = random_class(0)
# 
# y=empty_table(1000).select(["Row=ii%10", "Col=(int)((ii/10) % 30)", "Sentinel=random_inst.nextDouble()"]).where("Sentinel > 0.3")
# 
# # first part of the pivot is getting unique row and column values
# ys = y.sum_by(["Row", "Col"])
# 
# pvt=pivot(ys, ["Row"], "Col", "Sentinel")
# 
# # we have no real sector data, but it is nice for an example
# #sectors = ["Apples", "Bananas", "Carrots", "Eggplant", "Fig"]
# #sec_map = dict()
# #def get_sector(sym : str) -> str:
#     #if not sym in sec_map:
#         #sec_map[sym] = sectors[random.randint(0, 2)]
#     #return sec_map[sym]
# 
# feedos=db.live_table("FeedOS", "EquityTradeL1").where("Date=today()")
# feedos_agg=feedos.view(["LocalCodeStr", "Dollars=Price*Size", "Size", "MarketId"]).sum_by(["LocalCodeStr", "MarketId"])#.update("Sector=get_sector(LocalCodeStr)")
# fp = pivot(feedos_agg, ["MarketId"], "LocalCodeStr", "Dollars")