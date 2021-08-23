
# Deephaven Python Client 

Deephaven Python Client is a Python package by Deephaven Data Labs. It is a client API that allows Python applications to remotely access Deephaven data servers.

## Source Directory

### From the deephaven-core repository root
``` shell
$ cd pyclient
```
## Dev environment setup
``` shell
$ pip3 install -r pyclient/requirements.txt
```

## Build
``` shell
$ python3 setup.py
```
## Run tests
``` shell
$ python3 -m unittest discover tests
```
## Install
``` shell
$ pip3 install dist/pydeephaven-0.0.1-py3-none-any.whl
```
## Quick start

```python    
    >>> from pydeephaven import Session
    >>> session = Session() # assuming Deephaven Community Edition is running locally with the default configuration
    >>> table1 = session.time_table(period=1000000).update(column_specs=["Col1 = i % 2"])
    >>> daf = table1.snapshot().to_pandas()
    >>> print(df)
                        Timestamp  Col1
    0     1629681525690000000     0
    1     1629681525700000000     1
    2     1629681525710000000     0
    3     1629681525720000000     1
    4     1629681525730000000     0
    ...                   ...   ...
    1498  1629681540670000000     0
    1499  1629681540680000000     1
    1500  1629681540690000000     0
    1501  1629681540700000000     1
    1502  1629681540710000000     0

    >>> session.close())

```

## Resources
* https://deephaven.io/
* https://arrow.apache.org/docs/python/index.html

## API Reference
[start here] file://docs/build/html/index.html


