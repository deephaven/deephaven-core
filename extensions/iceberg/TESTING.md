# sqlite catalogs

The sqlite JDBC catalog is able to support multiple catalogs through a single database file.
As such, the following convention has been established for testing purposes:

* database file: `<rootDir>/dh-iceberg-test.db`
* warehouse directory: `<rootDir>/catalogs/<catalogName>/`

Both writers and readers of this catalog need to be setup to support relative metadata locations to ensure portability.

A root directory for extension-iceberg testing has been established at `extensions/iceberg/src/test/resources/io/deephaven/iceberg/sqlite/db_resource`.

## Usage

### Java

```java
import org.apache.iceberg.catalog.Catalog;
import io.deephaven.iceberg.sqlite.DbResource;

Catalog catalog = DbResource.openCatalog("<catalogName>");
```

### pyiceberg

To setup in [pyiceberg](https://py.iceberg.apache.org/):

```python
from pyiceberg.catalog.sql import SqlCatalog

catalog = SqlCatalog(
    "<catalogName>",
    **{
        "uri": f"sqlite:///dh-iceberg-test.db",
        "warehouse": f"catalogs/<catalogName>",
    },
)
```

## Generating data

Note that any scripts that write data should be run relative to
[db_resource](src/test/resources/io/deephaven/iceberg/sqlite/db_resource) working directory to ensure unit testability.

### pyiceberg-1

Here's an example of what was needed to generate this data:

```shell
$ cd extensions/iceberg/src/test/resources/io/deephaven/iceberg/sqlite/db_resource

# Note: 3.10 explicitly chosen b/c it doesn't seem like pyiceberg produces 3.12 wheels yet
$ python3.10 -m venv /tmp/iceberg

$ source /tmp/iceberg/bin/activate

$ pip install --only-binary=":all:" "pyiceberg[sql-sqlite, pyarrow]"

$ pip freeze
annotated-types==0.7.0
certifi==2024.8.30
charset-normalizer==3.3.2
click==8.1.7
fsspec==2024.9.0
greenlet==3.1.1
idna==3.10
markdown-it-py==3.0.0
mdurl==0.1.2
mmh3==4.1.0
numpy==1.26.4
pyarrow==17.0.0
pydantic==2.9.2
pydantic_core==2.23.4
Pygments==2.18.0
pyiceberg==0.7.1
pyparsing==3.1.4
python-dateutil==2.9.0.post0
requests==2.32.3
rich==13.8.1
six==1.16.0
sortedcontainers==2.4.0
SQLAlchemy==2.0.35
strictyaml==1.7.3
tenacity==8.5.0
typing_extensions==4.12.2
urllib3==2.2.3

$ python generate-pyiceberg-1.py

$ sqlite3 dh-iceberg-test.db 
SQLite version 3.45.1 2024-01-30 16:01:20
Enter ".help" for usage hints.
sqlite> .dump
PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE iceberg_tables (
        catalog_name VARCHAR(255) NOT NULL, 
        table_namespace VARCHAR(255) NOT NULL, 
        table_name VARCHAR(255) NOT NULL, 
        metadata_location VARCHAR(1000), 
        previous_metadata_location VARCHAR(1000), 
        PRIMARY KEY (catalog_name, table_namespace, table_name)
);
INSERT INTO iceberg_tables VALUES('pyiceberg-1','dh-default','cities','catalogs/pyiceberg-1/dh-default.db/cities/metadata/00003-68091f71-d3c5-42bb-8161-e2e187dece14.metadata.json','catalogs/pyiceberg-1/dh-default.db/cities/metadata/00002-106b37f8-8818-439d-87c5-3cae608d1972.metadata.json');
CREATE TABLE iceberg_namespace_properties (
        catalog_name VARCHAR(255) NOT NULL, 
        namespace VARCHAR(255) NOT NULL, 
        property_key VARCHAR(255) NOT NULL, 
        property_value VARCHAR(1000) NOT NULL, 
        PRIMARY KEY (catalog_name, namespace, property_key)
);
INSERT INTO iceberg_namespace_properties VALUES('pyiceberg-1','dh-default','exists','true');
COMMIT;
sqlite> 
```

### sqlite

If we add a lot of catalogs to the database, we may want to look into vacuuming the database to keep the file size small.

`sqlite3 dh-iceberg-test.db 'VACUUM;'`

Ideally, the sqlite database can be a small collection of catalogs that were created via external tooling to verify that
we can integrate with them successfully.

