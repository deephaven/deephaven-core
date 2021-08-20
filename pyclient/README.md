
# Deephaven Python Client 

Deephaven Python Client is a Python package by Deephaven Data Labs. It is a client API that allows Python applications to remotely access Deephaven data servers.

## Installation
### from the root of the deephaven-core repository
pip3 install pyclient/dist/pydeephaven-0.0.1-py3-none-any.whl

## Dev environment setup

pip3 install -r pyclient/requirements.txt

## Quick start

```python    
    >>> from pydeephaven import Session
    >>> from pyarrow import csv
    >>> session = Session() # assuming Deephaven Community Edition is running locally with the default configuration
    >>> table1 = session.import_table(csv.read_csv("data1.csv")
    >>> table2 = session.import_table(csv.read_csv("data2.csv")
    >>> joined_table = table1.join(table2, keys=["key_col_1", "key_col_2"], columns_to_add=["data_col1"])
    >>> df = joined_table.snapshot().to_pandas())
    >>> print(df)
    >>> session.close())

```

## Resources
* https://deephaven.io/
* https://arrow.apache.org/docs/python/index.html

## API Reference
[start here] file://docs/build/html/index.html


