# pyiceberg-1

This was the setup for generating the pyiceberg-1 data.

```shell
pip install --only-binary=":all:" "pyiceberg[sql-sqlite, pyarrow]==0.7.1"
```

```shell
$ pip list
Package            Version
------------------ -----------
annotated-types    0.7.0
certifi            2024.8.30
charset-normalizer 3.3.2
click              8.1.7
fsspec             2024.9.0
greenlet           3.1.1
idna               3.10
markdown-it-py     3.0.0
mdurl              0.1.2
mmh3               4.1.0
numpy              1.26.4
pip                23.3.2
pyarrow            17.0.0
pydantic           2.9.2
pydantic_core      2.23.4
Pygments           2.18.0
pyiceberg          0.7.1
pyparsing          3.1.4
python-dateutil    2.9.0.post0
requests           2.32.3
rich               13.8.1
setuptools         69.0.3
six                1.16.0
sortedcontainers   2.4.0
SQLAlchemy         2.0.35
strictyaml         1.7.3
tenacity           8.5.0
typing_extensions  4.12.2
urllib3            2.2.3
```

```shell
python generate-pyiceberg-1.py
```