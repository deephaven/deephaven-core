# Finding 12 — a partition value containing a colon could not be written at all

**Severity:** medium — two of the partitioning types Deephaven claims to support could never be used;
the write failed before producing anything.
**Status:** **FIXED**, with regression test.
**Repro test:** `extensions/parquet/table/src/test/java/io/deephaven/parquet/table/TimePartitionColumnTest.java`
**Fuzzer seeds (3, all now passing):** `-2281078010550439077L` and `-1767017146706312469L` (`Instant`),
`3579704455286775782L` (`LocalTime`). The first is `OLD_FINDINGS.md` finding 7d; the `LocalTime` case is
in that file's write-path table, where it had been worked around by making the type non-partitionable.

## Symptom

```
parquet write failed: io.deephaven.UncheckedDeephavenException:
    Failed to create URI from relative path: Col0=2023-11-14T22:13:21Z/
  caused by: java.net.URISyntaxException:
    Illegal character in scheme name at index 4: Col0=2023-11-14T22:13:21Z/
```

```java
ParquetTools.writeKeyValuePartitionedTable(
        TableTools.newTable(
                TableTools.col("Key", LocalTime.parse("00:00:02")),
                TableTools.intCol("Value", 1)).partitionBy("Key"),
        dir, ParquetInstructions.EMPTY);   // throws
```

## Root cause

`ParquetUtils.resolve` built the partition directory's relative URI as:

```java
relativeURI = new URI(null, null, relativePath.replace(WINDOWS_FILE_SEPARATOR, URI_SEPARATOR), null);
```

RFC 3986 §4.2 is explicit about why that fails:

> A path segment that contains a colon character (e.g., "this:that") cannot be used as the first
> segment of a relative-path reference, as it would be mistaken for a scheme name. Such a segment must
> be preceded by a dot-segment (e.g., "./this:that").

`Col0=2023-11-14T22:13:21Z/` has a colon in its first segment, so the parser reads the leading text as
the start of a scheme and rejects the `=` at index 4 — which is what "index 4" in the message refers
to, not anything about the value itself.

Both types are registered in `PartitionFormatter`'s **and** `PartitionParser`'s type maps, i.e. both
are claimed as supported partitioning types, and `PartitionFormatter.ForInstant`/`ForLocalTime` format
them with `toString()`, which always yields colons. So the claim of support could never be honoured.

## Fix

[`ParquetUtils.resolve`](../../../../../../../../base/src/main/java/io/deephaven/parquet/base/ParquetUtils.java)
prefixes the dot-segment the RFC calls for, and only when it is needed:

```java
private static String dotPrefixIfFirstSegmentHasColon(final String relativePath) {
    final int firstSeparator = relativePath.indexOf(URI_SEPARATOR);
    final String firstSegment = firstSeparator < 0 ? relativePath : relativePath.substring(0, firstSeparator);
    return firstSegment.indexOf(':') < 0 ? relativePath : "." + URI_SEPARATOR + relativePath;
}
```

`URI.resolve` removes the dot-segment while resolving, so the resulting URI — and the directory laid
down on disk — is exactly what it would have been had the colon been legal. A colon-free path takes no
prefix and is bit-for-bit unchanged, which `colonFreePartitionValuesUnchanged` pins by asserting the
directory listing.

Only the first segment is checked, because that is the only position the RFC restricts; a colon deeper
in the path was always fine, which `twoPartitioningLevelsWithColons` covers.

## A caveat worth stating

The partition value still reaches the filesystem verbatim, so these directories are named
`Key=00:00:02`. That is fine on POSIX and **not** representable on Windows, where `:` is not a legal
filename character. Making such a dataset portable would mean percent-encoding partition values in
`PartitionFormatter` and decoding them in `PartitionParser` — a change to the on-disk layout, affecting
interoperability with other Hive-style readers, and well beyond this defect. What is fixed here is that
a supported type no longer fails outright on the platforms that can represent it.

## Verification

- `TimePartitionColumnTest`, 6 tests: `LocalTime` and `Instant` partitioning columns written, read back
  with the right column type, and filtered on the partitioning column; sub-second `Instant` precision;
  two partitioning levels, so a colon appears in a non-first segment as well; the same write with
  `generateMetadataFiles`, which resolves further paths through the same helper; and a colon-free
  control asserting the directory layout is untouched. Five of the six fail without the fix.
- Full `:extensions-parquet-base:test`, `:extensions-parquet-table:test` and `:engine-table:test` pass.
- All 3 fuzzer seeds pass and join `INTERESTING_SEEDS`. The bench no longer needs its
  `LOCAL_TIME.partitionable() == false` workaround, which was removed at the start of this campaign;
  `LOCAL_DATE_TIME` stays non-partitionable because `PartitionFormatter` has no entry for it at all.
