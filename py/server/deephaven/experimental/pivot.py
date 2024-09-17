from typing import Sequence, List, Union, Protocol
import random
import re
import jpy
from deephaven import time_table, empty_table
from deephaven.table import Table, PartitionedTable, multi_join
from deephaven.numpy import to_numpy
from deephaven.update_graph import auto_locking_ctx

def _legalize_column(s: str) -> str:
    """Legalize a column name.

    Args:
        s (str): The column name to legalize.

    Returns:
        str: The legalized column name.

    Raises:
        ValueError: If the column name is empty.
    """

    #TODO: This is not a good way to legalize a column name.  It is not guaranteed to be unique.
    # Instead of legalizing the column name, it would be preferable to assign known names, and index them by the names table;
    # using some kind of display name.  We don't know that we'll actually replace stuff to be unique here,
    # which means this is not actually correct.

    if re.match("^[_a-zA-Z][_a-zA-Z0-9]*$", s):
        return s
    if re.match("^[_a-zA-Z].*$", s):
        return re.sub("[^_a-zA-Z0-9]", "_", s)
    return "_" + re.sub("[^_a-zA-Z0-9]", "_", s)

#TODO: redo all parameter names
#TODO: pydoc

def _do_multijoin(partitions_table: PartitionedTable, row_column_names: list[str], col_column_name: str,
                  value_column_name: str) -> Table:
    # Define the columns for a multi-join

    #TODO: this stuff is not synchronized

    # Locking to ensure that the partitioned table doesn't change while we're working with it
    with auto_locking_ctx(partitions_table):
        #TODO: this does not handle key changes in the constituent tables.  It should.
        keys = partitions_table.keys()
        key_values = to_numpy(table=keys, cols=[col_column_name])

        if len(key_values) == 0:
            return empty_table(0)

        tables = [
            con.view(row_column_names + [f"{_legalize_column(str(key_values[ki][0]))}={value_column_name}"])
                for ki, con in enumerate(partitions_table.constituent_tables)
        ]

    return multi_join(input=tables, on=row_column_names).table()


#TODO: handle list or value for row_column_names
def pivot(source: Table, row_column_names: list[str], col_column_name: str, value_column_name: str) -> Table:
    # Partition the source by column
    partitioned_source = source.partition_by(col_column_name)
    pvt = _do_multijoin(partitioned_source, row_column_names, col_column_name, value_column_name)
    return pvt


#TODO: delete below here

# # Java wrappers
random_class = jpy.get_type("java.util.Random")
random_inst = random_class(0)

y=empty_table(1000).select(["Row=ii%10", "Col=(int)((ii/10) % 30)", "Sentinel=random_inst.nextDouble()"]).where("Sentinel > 0.3")

# first part of the pivot is getting unique row and column values
ys = y.sum_by(["Row", "Col"])

pvt=pivot(ys, ["Row"], "Col", "Sentinel")
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
