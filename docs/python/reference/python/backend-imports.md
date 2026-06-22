---
title: Automatic Backend Imports
sidebar_label: Backend Imports
---

This guide outlines the backend packages and classes a new Deephaven instance automatically imports during startup.

The imports on this list are not standard Python imports, like `from deephaven import empty_table`. Backend imports are methods that Deephaven's engine uses behind the scenes, and aren't meant to be called directly by users. Most users won't need to know about these imports, but they are listed here for reference for the exceptionally curious or extremely advanced.

## Backend Imports

<details>
<summary>Chunks</summary>

Chunks are data structures that Deephaven employs to store and move data around. For a detailed explanation of the benefits that chunks provide, see [this](../../conceptual/deephaven-design.md#chunk-oriented-architecture) document. Packages and classes that handle chunks are:

- [io.deephaven.chunk.attributes.\*](/core/javadoc/io/deephaven/chunk/attributes/package-summary.html)
- [io.deephaven.engine.rowset.chunkattributes.\*](/core/javadoc/io/deephaven/engine/rowset/chunkattributes/package-summary.html)
- [io.deephaven.chunk.ByteChunk](/core/javadoc/io/deephaven/chunk/ByteChunk.html)
- [io.deephaven.chunk.CharChunk](/core/javadoc/io/deephaven/chunk/CharChunk.html)
- [io.deephaven.chunk.Chunk](/core/javadoc/io/deephaven/chunk/Chunk.html)
- [io.deephaven.chunk.DoubleChunk](/core/javadoc/io/deephaven/chunk/DoubleChunk.html)
- [io.deephaven.chunk.FloatChunk](/core/javadoc/io/deephaven/chunk/FloatChunk.html)
- [io.deephaven.chunk.IntChunk](/core/javadoc/io/deephaven/chunk/IntChunk.html)
- [io.deephaven.chunk.LongChunk](/core/javadoc/io/deephaven/chunk/LongChunk.html)
- [io.deephaven.chunk.ObjectChunk](/core/javadoc/io/deephaven/chunk/ObjectChunk.html)
- [io.deephaven.chunk.ShortChunk](/core/javadoc/io/deephaven/chunk/ShortChunk.html)
- [io.deephaven.chunk.WritableByteChunk](/core/javadoc/io/deephaven/chunk/WritableByteChunk.html)
- [io.deephaven.chunk.WritableCharChunk](/core/javadoc/io/deephaven/chunk/WritableCharChunk.html)
- [io.deephaven.chunk.WritableChunk](/core/javadoc/io/deephaven/chunk/WritableChunk.html)
- [io.deephaven.chunk.WritableDoubleChunk](/core/javadoc/io/deephaven/chunk/WritableDoubleChunk.html)
- [io.deephaven.chunk.WritableFloatChunk](/core/javadoc/io/deephaven/chunk/WritableFloatChunk.html)
- [io.deephaven.chunk.WritableIntChunk](/core/javadoc/io/deephaven/chunk/WritableIntChunk.html)
- [io.deephaven.chunk.WritableLongChunk](/core/javadoc/io/deephaven/chunk/WritableLongChunk.html)
- [io.deephaven.chunk.WritableObjectChunk](/core/javadoc/io/deephaven/chunk/WritableObjectChunk.html)
- [io.deephaven.chunk.WritableShortChunk](/core/javadoc/io/deephaven/chunk/WritableShortChunk.html)

</details>

<details>
<summary>io.deephaven.function.Cast.*</summary>

- [io.deephaven.function.Cast.\*](/core/javadoc/io/deephaven/function/Cast.html)

This class contains methods used for casting between numerical types. Other Deephaven classes call these methods behind-the-scenes.

</details>

<details>
<summary>io.deephaven.base.string.cache.CompressedString</summary>

- [io.deephaven.base.string.cache.CompressedString](/core/javadoc/io/deephaven/base/string/cache/CompressedString.html)

This class converts String immutable byte arrays, which Deephaven uses to perform certain processes more quickly.

</details>

<details>
<summary>io.deephaven.base.string.cache.CompressedString.*</summary>

- [io.deephaven.base.string.cache.CompressedString.\*](/core/javadoc/io/deephaven/base/string/cache/CompressedString.html)

</details>

<details>
<summary>io.deephaven.engine.context.QueryScopeParam</summary>

- [io.deephaven.engine.context.QueryScopeParam](/core/javadoc/io/deephaven/engine/context/QueryScopeParam.html)

</details>

<details>
<summary>io.deephaven.engine.rowset.RowSequence</summary>

- [io.deephaven.engine.rowset.RowSequence](/core/javadoc/io/deephaven/engine/rowset/RowSequence.html)

</details>

<details>
<summary>io.deephaven.engine.rowset.RowSet</summary>

- [io.deephaven.engine.rowset.RowSet](/core/javadoc/io/deephaven/engine/rowset/RowSet.html)

`RowSet` is a class that Deephaven uses to handle row keys and sets of row keys.

</details>

<details>
<summary>io.deephaven.engine.rowset.RowSetBuilderRandom</summary>

- [io.deephaven.engine.rowset.RowSetBuilderRandom](/core/javadoc/io/deephaven/engine/rowset/RowSetBuilderRandom.html)

`RowSetBuilderRandom` is an interface used for constructing RowSets in an arbitrary order.

</details>

<details>
<summary>io.deephaven.engine.rowset.RowSetBuilderSequential</summary>

- [io.deephaven.engine.rowset.RowSetBuilderSequential](/core/javadoc/io/deephaven/engine/rowset/RowSetBuilderSequential.html)

`RowSetBuilderSequential` is an interface used for constructing RowSets in a strict sequential order.

</details>

<details>
<summary>io.deephaven.engine.rowset.TrackingRowSet</summary>

- [io.deephaven.engine.rowset.TrackingRowSet](/core/javadoc/io/deephaven/engine/rowset/TrackingRowSet.html)

This class creates a row set that tracks changes and maintains a consistent snapshot of the row set's previous state.

</details>

<details>
<summary>io.deephaven.engine.rowset.TrackingWritableRowSet</summary>

- [io.deephaven.engine.rowset.TrackingWritableRowSet](/core/javadoc/io/deephaven/engine/rowset/TrackingWritableRowSet.html)

This class helps Deephaven create and modify RowSets.

</details>

<details>
<summary>io.deephaven.engine.rowset.WritableRowSet</summary>

- [io.deephaven.engine.rowset.WritableRowSet](/core/javadoc/io/deephaven/engine/rowset/WritableRowSet.html)

This class contains methods that the Deephaven engine calls internally to create and modify RowSets.

</details>

<details>
<summary>io.deephaven.engine.rowset.ColumnSource</summary>

- [io.deephaven.engine.table.ColumnSource](/core/javadoc/io/deephaven/engine/table/ColumnSource.html)

A [`ColumnSource`](../../conceptual/deephaven-design.md#tables-designed-for-sharing-and-updating) is a key part of Deephaven's architecture, and this class contains methods that Deephaven calls internally to create and modify ColumnSources.

</details>

<details>
<summary>java.util.concurrent.ConcurrentHashMap</summary>

- [java.util.concurrent.ConcurrentHashMap](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/util/concurrent/ConcurrentHashMap.html)

</details>

<details>
<summary>io.deephaven.engine.table.Context</summary>

- [io.deephaven.engine.table.Context](/core/javadoc/io/deephaven/engine/table/Context.html)

</details>

<details>
<summary>io.deephaven.engine.table.vectors.ColumnVectors</summary>

- [io.deephaven.engine.table.vectors.ColumnVectors](/core/javadoc/io/deephaven/engine/table/vectors/ColumnVectors.html)

This is an interface that allows the Deephaven engine positional access to columns. Users will not interact with this class directly.

</details>

<details>
<summary>io.deephaven.function.BinSearch.*</summary>

- [io.deephaven.function.BinSearch.\*](/core/javadoc/io/deephaven/function/BinSearch.html)

A statically-imported interface that Deephaven uses to perform binary searches.

</details>

<details>
<summary>io.deephaven.function.BinSearchAlgo.*</summary>

- [io.deephaven.function.BinSearchAlgo.\*](/core/javadoc/io/deephaven/function/BinSearchAlgo.html)

An algorithm that Deephaven uses to resolve ties when performing binary searches.

</details>

<details>
<summary>io.deephaven.util.datastructures.LongSizedDataStructure</summary>

- [io.deephaven.util.datastructures.LongSizedDataStructure](/core/javadoc/io/deephaven/util/datastructures/LongSizedDataStructure.html)

</details>

<details>
<summary>org.jpy.PyObject</summary>

- [org.jpy.PyObject](https://jpy.readthedocs.io/en/0.7.2/_static/java-apidocs/org/jpy/PyObject.html)

A `PyObject` is a generic Java object that holds a Python object of any type. It can hold _any_ Python object, but that flexibility comes at the cost of speed. `PyObject` columns - and the use of `PyObjects` in general - should be avoided. See [this guide](../../how-to-guides/pyobjects.md) for how to handle `PyObjects`.

</details>

<details>
<summary>io.deephaven.engine.rowset.RowSetFactory</summary>

- [io.deephaven.engine.rowset.RowSetFactory](/core/javadoc/io/deephaven/engine/rowset/RowSetFactory.html)

This is a class for constructing [RowSets](/core/javadoc/io/deephaven/engine/rowset/RowSet.html), an important part of Deephaven's architecture. Users will not interact directly with the methods contained in these classes.

</details>

<details>
<summary>io.deephaven.time.DateTimeUtils</summary>

- [io.deephaven.time.DateTimeUtils](/core/javadoc/io/deephaven/time/DateTimeUtils.html)

<!-- This package is imported as a regular import, AND as a static import. This is likely a bug, and one or the other will likely be removed from the auto-import list at some point. -->

</details>

<details>
<summary>io.deephaven.util.type.ArrayTypeUtils</summary>

- [io.deephaven.util.type.ArrayTypeUtils](/core/javadoc/io/deephaven/util/type/ArrayTypeUtils.html)

This class contains common utility methods for working with arrays. Users will not typically interact directly with the methods contained in this class.

</details>

<details>
<summary>io.deephaven.util.type.TypeUtils</summary>

- [io.deephaven.util.type.TypeUtils](/core/javadoc/io/deephaven/util/type/TypeUtils.html)

This class contains methods for converting objects between primitive types. Users will not interact with this class directly.

</details>

<details>
<summary>io.deephaven.vector.VectorConversions</summary>

- [io.deephaven.vector.VectorConversions](/core/javadoc/io/deephaven/vector/VectorConversions.html)

This class contains methods for converting vectors to native arrays. Users will not interact with this class directly.

</details>

<details>
<summary>io.deephaven.engine.table.impl.select.ConditionFilter.FilterKernel</summary>

- [io.deephaven.engine.table.impl.select.ConditionFilter.FilterKernel](/core/javadoc/io/deephaven/engine/table/impl/select/ConditionFilter.FilterKernel.html)

This statically-imported interface is part of Deephaven's backend, and is used for filtering tables.

</details>

<details>
<summary>io.deephaven.engine.table.impl.lang.QueryLanguageFunctionUtils.*</summary>

- [io.deephaven.engine.table.impl.lang.QueryLanguageFunctionUtils.\*](/core/javadoc/io/deephaven/engine/table/impl/lang/QueryLanguageFunctionUtils.html)

This class contains static methods that Deephaven employs behind the scenes to convert simple logic expressions (such as '-' for 'minus') into the corresponding Java method for the type(s) of data being manipulated.

</details>

<details>
<summary>io.deephaven.engine.table.impl.verify.TableAssertions.*</summary>

- [io.deephaven.engine.table.impl.verify.TableAssertions.\*](/core/javadoc/io/deephaven/engine/table/impl/verify/TableAssertions.html)

This class contains static methods that Deephaven employs behind the scenes to verify that tables are valid according to certain properties. Users will not interact with this class directly.

</details>

<details>
<summary>java.lang.reflect.Array</summary>

- [java.lang.reflect.Array](https://docs.oracle.com/javase/7/docs/api/java/lang/reflect/Array.html)

This class contains methods for creating and manipulating Java arrays, a key element of Deephaven's backend. Users will not interact with this class directly.

</details>

<details>
<summary>java.util.*</summary>

- [java.util.\*](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/util/package-summary.html)

This package contains Java's [collections framework](https://docs.oracle.com/javase/8/docs/technotes/guides/collections/overview.html), a crucial part of Deephaven's architecture. Deephaven users will not need to interact with this package directly.

</details>

## Related documentation

- [Query Language Function Reference](../../reference/query-language/query-library/auto-imported/index.md)
