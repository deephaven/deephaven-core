# extensions-parquet-table

## Test data

Some of the test data under [src/test/resources](src/test/resources) was generated with the following snippet:

```python
import pandas as pd
import numpy as np

df = pd.DataFrame(
    {
        "a": list("abc"),
        "b": list(range(1, 4)),
        "c": np.arange(3, 6).astype("u1"),
        "d": np.arange(4.0, 7.0, dtype="float64"),
        "e": [True, False, True],
        "f": pd.date_range("20130101", periods=3),
        "g": pd.date_range("20130101", periods=3, tz="US/Eastern"),
        "h": pd.Categorical(list("abc")),
        "i": pd.Categorical(list("abc"), ordered=True),
    }
)

df.to_parquet("uncompressed.parquet", compression=None)
df.to_parquet("brotli.parquet", compression="brotli")
df.to_parquet("gzip.parquet", compression="gzip")
df.to_parquet("lz4.parquet", compression="lz4")
df.to_parquet("snappy.parquet", compression="snappy")
df.to_parquet("zstd.parquet", compression="zstd")
```

Using the following requirements:

```requirements
# for src/test/resources/e0
numpy==1.24.2
pandas==1.5.3
pyarrow==5.0.0
python-dateutil==2.8.2
pytz==2022.7.1
six==1.16.0
```

```requirements
# for src/test/resources/e1
numpy==1.24.2
pandas==1.5.3
pyarrow==11.0.0
python-dateutil==2.8.2
pytz==2022.7.1
six==1.16.0
```

```requirements
# for src/test/resources/e2
cramjam==2.6.2
fastparquet==2023.2.0
fsspec==2023.3.0
numpy==1.24.2
packaging==23.0
pandas==1.5.3
python-dateutil==2.8.2
pytz==2022.7.1
six==1.16.0
```
