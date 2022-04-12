# Dynamic data and Pandas

This notebook demonstrates some of the options for working with [Pandas](http://pandas.pydata.org/) in Deephaven. You'll see how to use DataFrame tools the Deephaven way, such as switching from DataFrames to tables.

With Deephaven, you have all the familiar tools from Pandas with added flexibility, efficiency, and better visualization. Deephaven can handle dynamic data. Plus, Deephaven allows multithreading, easy partitioning, and collecting data.

Not only that - these tasks are possible in Deephaven with very large data, be it streaming or otherwise.

## Table to DataFrame

A [`DataFrame`](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html?highlight=dataframe#pandas.DataFrame) is a two-dimensional tabular data structure that is native to Pandas. With Deephaven, we can convert between Deephaven tables and Pandas DataFrames.

First, let's create a Deephaven table.


```python
from deephaven import new_table
from deephaven.column import double_col, float_col, int_col, string_col

source = new_table([
    string_col("Strings", ["String 1", "String 2", "String 3"]),
    int_col("Ints", [4, 5, 6]),
    float_col("Floats", [9.9, 8.8, 7.7]),
    double_col("Doubles", [0.1, 0.2, 0.3])
])
```


To convert the Deephaven table to a DataFrame, import the [`to_pandas`](https://deephaven.io/core/pydoc/code/deephaven.pandas.html?highlight=to_pandas#deephaven.pandas.to_pandas) method and then perform the conversion. To see the DataFrame, we print it.

```python
from deephaven.pandas import to_pandas

data_frame = to_pandas(source)
```

## DataFrame to Table

Users often perform analysis which results in a Pandas DataFrame. To convert this to a Deephaven table, we start with the DataFrame created above and map that to a Deephaven table using the [`to_table`](https://deephaven.io/core/pydoc/code/deephaven.pandas.html?highlight=to_table#deephaven.pandas.to_table) method.

```python
from deephaven.pandas import to_table

new_table = to_table(data_frame)
```
The new Deephaven table will display in the IDE and the data will match the original data. To check that the data type conversions are accurate, we can look at the [column metadata](https://deephaven.io/core/pydoc/code/deephaven.table.html?highlight=columns#deephaven.table.Table.columns).

For the DataFrame, we print the data types in the Console. For the Deephaven table, we create a new table containing the metadata information.

```python
print(data_frame.dtypes)

print(new_table.columns)
```

Pandas uses `float32` and `float64` data types, which are equivalent to `float` and `double` in Deephaven. These are the same type and require the same memory. A `String` in Deephaven is an `object` in Pandas.

\
Pandas has fewer data types than Deephaven. To learn more about creating tables with specific types, see our guide [How to create a table with new_table](https://deephaven.io/core/docs/how-to-guides/new-table/).

\

## Table operations

Deephaven tables and Pandas DataFrames both contain tabular data. In both cases, users want to perform the same kinds of operations, such as creating tables, filtering tables, and aggregating tables. Below we present how to do the same operations with both Pandas and Deephaven.


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

from deephaven import new_table
from deephaven.column import int_col, string_col

table = new_table([
    int_col("A", [1, 2, 3]),
    string_col("B", ["X", "Y", "Z"])
])
```

You'll often want to perform operations on whole columns. Deephaven has various methods for viewing, selecting, updating, eliminating, changing, and creating columns of data in tables. The choice of each can result in performance differences. See our guide [Choosing the right selection method for your query](https://deephaven.io/core/docs/conceptual/choose-select-view-update/) or [How to select, view, and update data in tables](https://deephaven.io/core/docs/how-to-guides/use-select-view-update/) for detailed advice.


In this case, we wish to add a column `C` that is equal to column `A` plus 5.

```python
added_data_frame = data_frame.assign(C=data_frame['A'] + 5)
print(added_data_frame)

added_table = table.update(formulas=["C = A + 5"])
```

We can remove whole columns with [`drop`](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.drop.html) in Pandas or [`drop_columns`](https://deephaven.io/core/docs/reference/table-operations/select/drop-columns/) in Deephaven.

```python
dropped_data_frame = data_frame.drop(columns=['A'])
print(dropped_data_frame)

dropped_table = table.drop_columns(cols=["A"])
```

[Renaming](https://deephaven.io/core/docs/reference/table-operations/select/rename-columns/) columns in a DataFrame or Deephaven table is simple:

```python
data_frame.rename(columns={"A": "X", "B": "B"}, inplace=True)
print(data_frame)

renamed_table = table.rename_columns(cols=["X = A"])
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

from deephaven import new_table
from deephaven.column import int_col, string_col

table = new_table([
    int_col("A", [1, 2, 3]),
    string_col("B", ["X", "Y", "Z"])
])
```

We can limit the columns to certain values that match a formula. See our article, [How to use formulas](https://deephaven.io/core/docs/formulas-how-to/).

```python
filtered_data_frame = data_frame[data_frame.A < 2]
print(filtered_data_frame)

filtered_table = table.where(filters=["A < 2"])
```


We can also return just the first rows ([head](https://deephaven.io/core/docs/reference/table-operations/filter/head/)) or last rows ([tail](https://deephaven.io/core/docs/reference/table-operations/filter/tail/)) of the DataFrame or table. Below, we request the first three rows:

```python
head_data_frame = data_frame.iloc[:3]
print(head_data_frame)


head_table = table.head(num_rows=3)
```

Below, we request the last three rows:

```python
tail_data_frame = data_frame.iloc[-3:]
print(tail_data_frame)

tail_table = table.tail(num_rows=3)
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

from deephaven import new_table
from deephaven.column import int_col, string_col

table = new_table([
   int_col('A', [1, 2, 3, 4, 5, 6]),
   string_col('B', ['Z', 'Y', 'X', 'X', 'Y', 'Z']),
   int_col('C', [7, 2, 1, 5, 3, 4])
])
```


We can [sort](https://deephaven.io/core/docs/reference/table-operations/sort/sort/) in ascending order on a dataset for a DataFrame or table:

```python
sorted_data_frame = data_frame.sort_values(by='B')
print(sorted_data_frame)


sorted_table = table.sort(order_by=["B"])
```


We can [sort descending](https://deephaven.io/core/docs/reference/table-operations/sort/sort-descending/) on a dataset for a DataFrame or table:

```python
sorted_data_frame = data_frame.sort_values(by='B', ascending=False)
print(sorted_data_frame)


sorted_table = table.sort_descending(order_by=["B"])
```



In Pandas, [`concat`](https://pandas.pydata.org/docs/reference/api/pandas.concat.html?highlight=concat#pandas.concat) allows tables to be vertically combined, stacked on top of each other. The same operation can be performed using [`merge`](https://deephaven.io/core/docs/reference/table-operations/merge/merge/) on Deephaven tables. The combined columns should have the same data type.

```python
import pandas as pd

data_frame1 = pd.DataFrame({'A': [1, 2]})
data_frame2 = pd.DataFrame({'A': [3, 4]})
data_frame = pd.concat([data_frame1, data_frame2])

print(data_frame)


from deephaven import new_table
from deephaven.column import int_col
from deephaven.table_factory import merge

table1 = new_table([
    int_col("A", [1, 2])
])
table2 = new_table([
    int_col("A", [3, 4])
])

table = merge(tables=[table1, table2])
```


Deephaven's many join methods combine data by appending the columns of one data set to another. See our guide [How to join tables](https://deephaven.io/core/docs/how-to-guides/joins-overview/) to learn more.



Pandas and Deephaven provide many of the same join methods, but there is not a one-to-one mapping of methods. Here we do a basic join with Pandas and Deephaven.

```python
import pandas as pd

data_frame_left = pd.DataFrame({'A': [1, 2, 3], 'B': ['X', 'Y', 'Z']})
data_frame_right = pd.DataFrame({'A': [3, 4, 5], 'C': ['L', 'M', 'N']})
data_frame = pd.merge(data_frame_left, data_frame_right, on='A')

print(data_frame)

from deephaven import new_table
from deephaven.column import int_col, string_col

table_left = new_table([
    int_col("A", [1, 2, 3]),
    string_col("B", ["X", "Y", "Z"])
])
table_right = new_table([
    int_col("A", [3, 4, 5]),
    string_col("C", ["L", "M", "N"])
])

table = table_left.join(table=table_right, on=["A"])
```


Inexact joins are also a common operation, made possible by [`aj` (as-of join)](https://deephaven.io/core/docs/reference/table-operations/join/aj/) and [`raj` (reverse as-of join)](https://deephaven.io/core/docs/reference/table-operations/join/raj/), for analyzing time-series data. 

To join these tables together based on the timestamps, we need to use an as-of join. As-of joins perform exact matches across all given columns except for the last one, which instead matches based on the closest values.

In an as-of-join, the values in the right table are matched to the closest values in the left table without going over the value in the left table.

For example, if the right table contains a value 5 and the left table contains values 4 and 6, the right table's 5 will be matched on the left table's 6.

Let's join these tables using the `aj` method to get a single table with all of our information.

```python
import pandas as pd
import random, time

ch = "ABCDE"

data_frame_left = pd.DataFrame(
    {'A': pd.date_range(start='2022-01-01 00:00:00+09:00', periods=365),
     'B': [random.choice(ch) for i in range(0, 365)],
     'C': [random.randint(0, 100) for j in range(0, 1) for i in range(0, 365)]
    })

data_frame_right = pd.DataFrame(
    {'A': pd.date_range(start='2022-01-01 00:00:02+09:00', periods=365),
     'B': [random.choice(ch) for i in range(0, 365)],
     'C': [random.randint(0, 100) for j in range(0, 1) for i in range(0, 365)]
    })

data_frame_aj = pd.merge_asof(data_frame_left, data_frame_right, on='A')

print(data_frame_aj)

from deephaven import empty_table
from deephaven.time import to_datetime, to_period, plus_period

def period(i):
    return to_period(f"{i}D")

start_times = [
    to_datetime("2020-01-01T00:00:00 NY"),
    to_datetime("2020-01-01T00:00:02 NY")
]

deephaven_table_left = empty_table(size = 365).update(formulas=[
    "A = plus_period(start_times[0], period(i))",
    "B = random.choice(ch)",
    "C = (int)(byte)random.randint(1, 100)"
])
deephaven_table_right = empty_table(size = 365).update(formulas=[
    "A = plus_period(start_times[1], period(i))",
    "B = random.choice(ch)",
    "C = (int)(byte)random.randint(1, 100)"
])

joined_data_aj = deephaven_table_left.aj(table=deephaven_table_right, on=["A"], joins=["B_y = B", "C_y = C"])
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

from deephaven import new_table
from deephaven.column import int_col

table = new_table([
    int_col("A", [1, 3, 5]),
    int_col("B", [5, 7, 9])
])

avg_table = table.avg_by()
```


In this example, we first group the data, then apply a [sum](https://deephaven.io/core/docs/reference/table-operations/group-and-aggregate/AggSum/) on that group. For more information on grouping, see our [How to group and ungroup data](https://deephaven.io/core/docs/how-to-guides/grouping-data/) guide.

```python
import pandas as pd
import numpy as np 

data_frame = pd.DataFrame(
    {'A': ['X', 'Y', 'X', 'Z', 'Z', 'X'],
     'B': [2, 2, 5, 1, 3, 4],
     'C': [12, 22, 13, 12, 8, 2]}
)

def agg_list(data):
    d = {}
    d['Sum'] = np.sum(data['C'])
    d['Min'] = np.amin(data['C'])
    d['Std'] = np.std(data['C'])
    d['WAvg'] = np.average(data['C'], weights=data['B'])
    return pd.Series(d)

grouped_data_frame = data_frame.groupby('A').apply(agg_list)
print(grouped_data_frame)

from deephaven import new_table
from deephaven.column import int_col, string_col
from deephaven import agg

table = new_table([
    string_col("A", ["X", "Y", "X", "Z", "Z", "X"]),
    int_col("B", [2, 2, 5, 1, 3, 4]),
    int_col("C", [12, 22, 13, 12, 8, 2])
])

agg_list = [
    agg.sum_(cols=["Sum = C"]),
    agg.min_(cols=["Min = C"]),
    agg.std(cols=["Std = C"]),
    agg.weighted_avg(wcol="B", cols=["WAvg = C"])
]

grouped_table = table.agg_by(aggs=agg_list, by=["A"])
```


If your data set has [null](https://deephaven.io/core/docs/reference/query-language/types/nulls/) or [NaN](https://deephaven.io/core/docs/reference/query-language/types/NaNs/) values, you'll probably want to remove or replace them before performing analysis. See our guide [How to handle null, infinity, and not-a-number values](https://deephaven.io/core/docs/how-to-guides/handle-null-inf-nan) for information on these data types in Deephaven.
\
\
In this example, we define a data set with a missing value. Pandas uses `np.nan` to represent missing double values, while Deephaven uses `NULL_` followed by the data type.  In the code below, `NULL_DOUBLE` is used for a column of double values.

```python
import pandas as pd
import numpy as np

data_frame = pd.DataFrame(
    {'A': [1.0, 2.0, 3.0],
     'B': [4.0, 2.0, np.nan]}
)

print(data_frame)

from deephaven import new_table
from deephaven.column import double_col
from deephaven.constants import NULL_DOUBLE

table = new_table([
    double_col("A", [1., 2., 3.]),
    double_col("B", [4., 2., NULL_DOUBLE])
])
```

We can filter the datasets to remove the missing values.

```python
remove_values_data_frame = data_frame.dropna()
print(remove_values_data_frame)


remove_values_table = table.where(filters=["!isNull(B)"])
```

Or we can replace the missing values.

```python
replace_values_data_frame = data_frame.fillna(value=0.0)
print(replace_values_data_frame)


replace_values_table = table.update(formulas=["B = isNull(B) ? 0.0 : B"])
```

The [Deephaven documentation](https://deephaven.io/core/docs/) has many more examples.

Pandas is a great tool for any Python programmer to have at hand. However, people use Deephaven for use cases that involve streaming, updating, and real-time data, or volumes beyond in-memory scale. In the following notebooks, we demo examples where data just flows from one table to another in real time,  which is impossible in Pandas. 


```python
print("Go to https://deephaven.io/core/docs/tutorials/quickstart/ to download pre-built Docker images.")
```