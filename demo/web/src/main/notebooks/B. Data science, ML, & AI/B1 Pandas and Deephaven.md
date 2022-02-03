# Dynamic data and Pandas

This notebook demonstrates some of the options for working with [Pandas](http://pandas.pydata.org/) in Deephaven. You'll see how to use familiar DataFrame tools the Deephaven way, such as switching from DataFrames to tables.
\
With Deephaven, you have all the familiar tools from Pandas but with added flexibility, efficiency, and better visualization. One of the reasons to use Deephaven from Pandas is Deephaven allows multithreading, easy partitioning, and collecting data.

Not only that - these tasks are possible in Deephaven with very large data.
\

## Table to DataFrame

A [`DataFrame`](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html?highlight=dataframe#pandas.DataFrame) is a two-dimensional tabular data structure that is native to Pandas. With Deephaven, we can convert between Deephaven tables and Pandas DataFrames.
\
First, let's create a Deephaven table.


```python
from deephaven.TableTools import newTable, stringCol, intCol, floatCol, doubleCol

source = newTable(
   stringCol("Strings", "String 1", "String 2", "String 3"),
   intCol("Ints", 4, 5, 6),
   floatCol("Floats", 9.9, 8.8, 7.7),
   doubleCol("Doubles", 0.1, 0.2, 0.3)
)
```

\
To convert the Deephaven table to a DataFrame, import the [`tableToDataFrame`](https://deephaven.io/core/pydoc/code/deephaven.html#deephaven.tableToDataFrame) method and then perform the conversion. To see the DataFrame, we print it.

```python
from deephaven import tableToDataFrame

data_frame = tableToDataFrame(source)
print(data_frame)
```


## DataFrame to Table

Users often perform analysis which results in a Pandas DataFrame. To convert this to a Deephaven table, we start with the DataFrame created above and map that to a Deephaven table using the [`dataFrameToTable`](https://deephaven.io/core/pydoc/code/deephaven.html#deephaven.dataFrameToTable) method.

```python
from deephaven import dataFrameToTable

new_table = dataFrameToTable(data_frame)
```
The new Deephaven table will display in the IDE and the data will match the original data. To check that the data type conversions are accurate, we can look at the [table metadata](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#getMeta()).

For the DataFrame, we print the data types in the Console. For the Deephaven table, we create a new table containing the metadata information.

```python
print(data_frame.dtypes)

meta_table = new_table.getMeta()
```

Pandas uses `float32` and `float64` data types, which are equivalent to `float` and `double` in Deephaven. These are the same type and require the same memory. A `String` in Deephaven is an `object` in Pandas.
\
\
Pandas has fewer data types than Deephaven. To learn more about creating tables with specific types, see our guide [How to create a table with newTable](https://deephaven.io/core/docs/how-to-guides/new-table/).
\
\

## Table operations

Deephaven tables and Pandas DataFrames both contain tabular data. In both cases, users want to perform the same kinds of operations, such as creating tables, filtering tables, and aggregating tables. Below we present how to do the same operations with both Pandas and Deephaven.
\
\
In these examples, keep in mind that Pandas DataFrames are mutable while Deephaven tables are immutable but can have data that changes dynamically. This results in differences in how some operations are approached.
\
Creating a Pandas DataFrame or Deephaven table is very similar.

```python
import pandas as pd

data_frame = pd.DataFrame(
    {'A': [1, 2, 3],
     'B': ['X', 'Y', 'Z']}
)

print(data_frame)


from deephaven.TableTools import col, newTable

table = newTable(
   col('A', 1, 2, 3),
   col('B', 'X', 'Y', 'Z')
)
```

You'll often want to perform operations on whole columns. Deephaven has various methods for viewing, selecting, updating, eliminating, changing, and creating columns of data in tables. The choice of each can result in performance differences. See our guide [Choosing the right selection method for your query](https://deephaven.io/core/docs/conceptual/choose-select-view-update/) or [How to select, view, and update data in tables](https://deephaven.io/core/docs/how-to-guides/use-select-view-update/) for detailed advice.


In this case, we wish to add a column `C` that is equal to column `A` plus 5.

```python
added_data_frame = data_frame.assign(C = data_frame['A'] + 5)
print(added_data_frame)


added_table = table.update("C = A + 5")
```

We can remove whole columns with [`drop`](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.drop.html) in Pandas or [`dropColumns`](https://deephaven.io/core/docs/reference/table-operations/select/drop-columns/) in Deephaven.

```python
dropped_data_frame = data_frame.drop(columns = ['A'])
print(dropped_data_frame)


dropped_table = table.dropColumns("A")
```


[Renaming](https://deephaven.io/core/docs/reference/table-operations/select/rename-columns/) columns in a DataFrame or Deephaven table is simple:

```python
data_frame.rename(columns={"A": "X", "B": "B"}, inplace = True)
print(data_frame)


renamed_table = table.renameColumns("X = A")
```

Deephaven offers several types of filters. See our article, [How to use filters](https://deephaven.io/core/docs/how-to-guides/use-filters/).
\
Filtering in Deephaven and Pandas has the same logic.


First, define a data set:

```python
import pandas as pd

data_frame = pd.DataFrame(
    {'A': [1, 2, 3],
     'B': ['X', 'Y', 'Z']}
)

print(data_frame)


from deephaven.TableTools import col, newTable

table = newTable(
   col('A', 1, 2, 3),
   col('B', 'X', 'Y', 'Z')
)
```

We can limit the columns to certain values that match a formula. See our article, [How to use formulas](https://deephaven.io/core/docs/formulas-how-to/).

```python
filtered_data_frame = data_frame[data_frame.A < 2]
print(filtered_data_frame)

filtered_table = table.where("A < 2")
```


We can also return just the first rows ([head](https://deephaven.io/core/docs/reference/table-operations/filter/head/)) or last rows ([tail](https://deephaven.io/core/docs/reference/table-operations/filter/tail/)) of the DataFrame or table. Below, we request the first three rows:

```python
head_data_frame = data_frame.iloc[:3]
print(head_data_frame)


head_table = table.head(3)
```

Below, we request the last three rows:

```python
tail_data_frame = data_frame.iloc[-3:]
print(tail_data_frame)

tail_table = table.tail(3)
```


Sorting changes the order of values in a dataset based upon comparison operations. All data is still in the dataset but in a different order. In Deephaven, data can be sorted by query or from the UI - UI sorting only changes how the data is displayed. It does not change the underlying data.
\
For this example, we want a slightly larger dataset:

```python
import pandas as pd

data_frame = pd.DataFrame(
    {'A': [1, 2, 3, 4, 5, 6],
     'B': ['Z', 'Y', 'X', 'X', 'Y', 'Z'],
     'C': [7, 2, 1, 5, 3, 4]}
)

print(data_frame)


from deephaven.TableTools import col, newTable

table = newTable(
   col('A', 1, 2, 3, 4, 5, 6),
   col('B', 'Z', 'Y', 'X', 'X', 'Y', 'Z'),
   col('C', 7, 2, 1, 5, 3, 4)
)

```


We can [sort](https://deephaven.io/core/docs/reference/table-operations/sort/sort/) in ascending order on a dataset for a DataFrame or table:

```python
sorted_data_frame = data_frame.sort_values(by = 'B')
print(sorted_data_frame)


sorted_table = table.sort("B")
```


We can [sort descending](https://deephaven.io/core/docs/reference/table-operations/sort/sort-descending/) on a dataset for a DataFrame or table:

```python
sorted_data_frame = data_frame.sort_values(by = 'B', ascending = False)
print(sorted_data_frame)


sorted_table = table.sortDescending("B")
```

To sort on different directions with one query, use `SortColumn`s in the [`sort`](https://deephaven.io/core/docs/reference/table-operations/sort/sort) argument:

```python
sorted_data_frame = data_frame.sort_values(by=['B','C'], ascending=[True,False])
print(sorted_data_frame)

from deephaven import SortColumn, as_list
from deephaven.TableManipulation import ColumnName

sort_columns = as_list([
    SortColumn.asc(ColumnName.of("B")),
    SortColumn.desc(ColumnName.of("C"))
])

sorted_table = table.sort(sort_columns)
```


In Pandas, [`concat`](https://pandas.pydata.org/docs/reference/api/pandas.concat.html?highlight=concat#pandas.concat) allows tables to be vertically combined, stacked on top of each other. The same operation can be performed using [`merge`](https://deephaven.io/core/docs/reference/table-operations/merge/merge/) on Deephaven tables. The combined columns should have the same data type.

```python
import pandas as pd

data_frame1 = pd.DataFrame({'A': [1, 2]})
data_frame2 = pd.DataFrame({'A': [3, 4]})
data_frame = pd.concat([data_frame1, data_frame2])

print(data_frame)


from deephaven.TableTools import col, newTable, merge

table1 = newTable(col('A', 1, 2))
table2 = newTable(col('A', 3, 4))
table = merge(table1, table2)
```


Deephaven's many join methods combine data by appending the columns of one data set to another. See our guide [How to join tables](https://deephaven.io/core/docs/how-to-guides/joins-overview/) to learn more.



Pandas and Deephaven provide many of the same join methods, but there is not a one-to-one mapping of methods. In addition to the common join methods, Deephaven also provides inexact joins , such as [`aj` (as-of join)](https://deephaven.io/core/docs/reference/table-operations/join/aj/) and [`raj` (reverse as-of join)](https://deephaven.io/core/docs/reference/table-operations/join/raj/), for analyzing time series, which are not present in Pandas.

```python
import pandas as pd

data_frameLeft = pd.DataFrame({'A': [1, 2, 3], 'B': ['X', 'Y', 'Z']})
data_frameRight = pd.DataFrame({'A': [3, 4, 5], 'C': ['L', 'M', 'N']})
data_frame = pd.merge(data_frameLeft, data_frameRight, on = 'A')

print( data_frame)


from deephaven.TableTools import col, newTable

tableLeft = newTable(col("A", 1, 2, 3), col("B", 'X', 'Y', 'Z'))
tableRight = newTable(col("A", 3, 4, 5), col("C", 'L', 'M', 'N'))
table = tableLeft.join(tableRight, "A")
```


You'll often want to partition your data into groups and then compute values for the groups. Deephaven supports many kinds of data aggregations. There are more methods than can be covered here, so see our guides
[How to perform dedicated aggregations for groups](https://deephaven.io/core/docs/how-to-guides/dedicated-aggregations/) and [How to perform combined aggregations](https://deephaven.io/core/docs/how-to-guides/combined-aggregations/).

```python
import pandas as pd

data_frame = pd.DataFrame(
   {'A': [1, 3, 5],
    'B': [5, 7, 9]}
)

avg_data_frame = data_frame.mean()
print(avg_data_frame)


from deephaven.TableTools import col, newTable

table = newTable(
   col("A", 1, 3, 5),
   col("B", 5, 7, 9)
)

avg_table = table.avgBy()
```


In this example, we first group the data, then apply a [sum](https://deephaven.io/core/docs/reference/table-operations/group-and-aggregate/AggSum/) on that group. For more information on grouping, see our [How to group and ungroup data](https://deephaven.io/core/docs/how-to-guides/grouping-data/) guide.

```python
import pandas as pd

data_frame = pd.DataFrame(
    {'A': [1, 2, 1, 2, 1, 2],
     'B': [2, 2, 5, 1, 3, 4]}
)

grouped_data_frame = data_frame.groupby(['A']).sum()
print(grouped_data_frame)


from deephaven.TableTools import col, newTable

table = newTable(
   col('A', 1, 2, 1, 2, 1, 2),
   col('B', 2, 2, 5, 1, 3, 4)
)

grouped_table1 = table.groupBy("A").view("Sum = sum(B)")

from deephaven import Aggregation as agg, as_list

grouped_table2 = table.aggBy(as_list([agg.AggSum("B")]), "A")
```


If your data set has [null](https://deephaven.io/core/docs/reference/query-language/types/nulls/) or [NaN](https://deephaven.io/core/docs/reference/query-language/types/NaNs/) values, you'll probably want to remove or replace them before performing analysis. See our guide [How to handle null, infinity, and not-a-number values](https://deephaven.io/core/docs/how-to-guides/handle-null-inf-nan) for information on these data types in Deephaven.
\
\
In this example, we define a data set with a missing value. Pandas uses `np.nan` to represent missing double values, while Deephaven uses `NULL_DOUBLE`.

```python
import pandas as pd
import numpy as np

data_frame = pd.DataFrame(
    {'A': [1.0, 2.0, 3.0],
     'B': [4.0, 2.0, np.nan]}
)

print(data_frame)

from deephaven.TableTools import col, newTable
from deephaven.conversion_utils import NULL_DOUBLE

table = newTable(
   col('A', 1.0, 2.0, 3.0),
   col('B', 4.0, 2.0, NULL_DOUBLE)
)
```

We can filter the datasets to remove the missing values.

```python
remove_values_data_frame = data_frame.dropna()
print(remove_values_data_frame)


remove_values_table = table.where("!isNull(B)")
```

Or we can replace the missing values.

```python
replace_values_data_frame = data_frame.fillna(value = 0.0)
print(replace_values_data_frame)


replace_values_table = table.update("B = isNull(B) ? 0.0 : B")
```

The [Deephaven documentation](https://deephaven.io/core/docs/) has many more examples.

```python
print("Go to https://deephaven.io/core/docs/tutorials/quickstart/ to download pre-built Docker images.")
```
