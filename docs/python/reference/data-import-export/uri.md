---
title: URI
---

A URI, short for [Uniform Resource Identifier](https://en.wikipedia.org/wiki/Uniform_Resource_Identifier), is a sequence of characters that identifies a resource on the web. Deephaven's URIs provide the means to receive tables and other Deephaven objects through a sequence of characters.

## Syntax

```
dh+plain://<authority>/<path>
```

## Parameters

<ParamTable>
<Param name="scheme" type="str">

`dh+plain://` is the scheme. The type of script to run. This can be one of `script`, `static`, `dynamic`, or `qst`.

</Param>
<Param name="authority" type="str">

The path on the authority. This will be a Docker container name if sharing locally or hostname/IP and port of the web resource; e.g., `deephaven.io:10000`.

</Param>
<Param name="path" type="str">

The path to a Deephaven object, which is generally `scope/<object_name>`. Tables, CSV, and Parquet files can be resolved locally with a URI. When sharing over a network, only tables and CSV files can be resolved via a URI.

</Param>
</ParamTable>

## Examples

This first example illustrates sharing a table locally. It assumes that two Docker containers have been set up, each running Deephaven. The first, `table-producer`, which runs on port 10000, creates a real-time table. The second, `table-consumer`, which runs on a different port, resolves it via a URI.

The following code creates a time table:

```python order=null
from deephaven import time_table

source = time_table("PT1S").update(["X = 0.1 * i", "Y = sin(X)"])
```

![The above ticking table, here labeled `real_time_data`](../../assets/reference/uris/uri1.gif)

The following code resolves it:

```python skip-test
from deephaven.uri import resolve

result = resolve("dh+plain://table-producer:10000/scope/source")
```

![The above `real_time_data` table, resolved on another port as `real_time_data_resolved`](../../assets/reference/uris/uri2.gif)

> [!NOTE]
> If no port is specified when resolving a local URI, Deephaven tries port 10000. Thus, to resolve a local asset that is found on port 10000, this can be omitted. Otherwise, the port must be explicitly set.

In the following example, a table is shared across a network. When sharing remotely, the IP/hostname, port, and table name are needed. In this case, the hypothetical IP is `192.168.5.1`, and Deephaven is being run on port `10000`. The table is the same as the local sharing example above.

The following code creates the source table:

```python test-set=1 order=source
from deephaven import empty_table

source = empty_table(50).update(["X = i", "Y = i % 2"])
```

The following code resolves it:

```python skip-test
from deephaven.uri import resolve

result = resolve("dh+plain://192.168.5.1:10000/scope/source")
```

## Related documentation

- [How to use URIs](../../how-to-guides/use-uris.md)
- [empty_table](../table-operations/create/emptyTable.md)
- [time_table](../table-operations/create/timeTable.md)
