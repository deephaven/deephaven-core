---
title: JavaScript API concepts
---

Ticking tables are the core feature of Deephaven. The [Deephaven JS API](/core/client-api/javascript/modules/dh.html) provides the means to create or discover tables in a format that is familiar to JS developers. It generally works through asynchronous tools like promises and events.

The configuration of a Deephaven table is immutable, but new tables can easily be created. Sorting, filtering, or adjusting the columns of a table are all performed by passing the original table back to the server with one or more of those operations. The server then sends back a new table, and the client can request data based on the results of the operation.

A table subscription consists of the client specifying which rows or columns it wants data from. The server then sends updates as needed. These updates can take the form of:

- A snapshot: the data for all rows and columns that were requested.
- A delta: only the added, modified, and removed rows and columns.

The client can cancel its subscription to a table. It can also request a snapshot without any subscription.

While the server's table is immutable, the JS API provides [`Table`](/core/client-api/javascript/classes/dh.Table.html) objects that seem to be mutable. A given UI component can therefore keep a single `Table` instance while its sort, filter, or columns change. If something goes wrong while applying a change, the table reverts to the previously working configuration. It then emits a `"requestfailed"` event to indicate that something went wrong.

## Table subscriptions

Data can be obtained from tables in two basic ways: a subscription to the entire table, or a subscription to a specific viewport. A viewport is a range of rows and a set of columns. Recall that Deephaven table data is column-oriented, so both kinds of subscription let you specify the columns. Only a viewport subscription lets you specify the rows.

In either case, a subscription fires an `"updated"` event each time its data changes. The event data implements the [`TableData`](/core/client-api/javascript/interfaces/dh.TableData.html) interface, which provides access to all data currently available.

Viewports are generally small enough that it is safe to walk all of their values. Use this API with care on full-table subscriptions. Since the data is column-oriented (even on the client), it is generally more efficient to iterate over all data within a column than to read by row. However, that is not always realistic when it comes to rendering data.

A `TableData` instance offers several ways to read values:

- The `rows` property exposes `Row` instances. Read a value from a row with either `row.get(column)` or `column.get(row)`, where `column` is one of the `Table`'s columns.
- If the desired row is known, `TableData.get(index)` reads a `Row`.
- `TableData.getData(index, column)` reads the value itself.

### Viewports

The usual mode of operation is to subscribe to a viewport and display it in a UI. To create a viewport subscription, call [`Table.createViewportSubscription()`](/core/client-api/javascript/classes/dh.Table.html#createViewportSubscription) with an options object:

- The `rows` property sets the rows to include. It is either a [`RangeSet`](/core/client-api/javascript/classes/dh.RangeSet.html) or an object with `first` and `last` properties.
- The `columns` property sets the columns to include.

Both properties are required. The method returns a [`TableViewportSubscription`](/core/client-api/javascript/classes/dh.TableViewportSubscription.html). This object is the source of the viewport's data:

- It fires the `"updated"` event whenever data from the server arrives. The `Table` instance does not fire this event for the subscription.
- Its [`getViewportData()`](/core/client-api/javascript/classes/dh.TableViewportSubscription.html#getViewportData) method returns a promise of the most recent data available. The data is a [`ViewportData`](/core/client-api/javascript/interfaces/dh.ViewportData.html) instance. It also has an `offset` property, which is the position of its first row within the table.
- Its [`update()`](/core/client-api/javascript/classes/dh.TableViewportSubscription.html#update) method changes the rows and columns of the viewport. It takes the same `rows` and `columns` properties.
- Its [`close()`](/core/client-api/javascript/classes/dh.TableViewportSubscription.html#close) method stops the subscription and releases its resources.

The following example subscribes to the first 10 rows of every column. Each time the data changes, it logs the offset and the number of rows received:

```javascript
const subscription = table.createViewportSubscription({
  rows: { first: 0, last: 9 },
  columns: table.columns,
});

subscription.addEventListener("updated", (event) => {
  const data = event.detail;
  console.log(`offset ${data.offset}, ${data.rows.length} rows`);
});
```

When the viewport is no longer needed, close it:

```javascript
subscription.close();
```

Each call to `createViewportSubscription()` creates a new, independent subscription. A `Table` can have several viewport subscriptions open at once. Every `TableViewportSubscription` must have `close()` called on it to release its resources.

A viewport subscription reads the table as it was when the subscription was created. It does not close or change automatically when the table's sort, filter, or columns change. To view the table's new configuration, close the old subscription and create a new one.

> [!NOTE]
> Older code may call `Table.setViewport()`, `Table.getViewportData()`, or `Table.subscribe()`. These methods are deprecated. Use `Table.createViewportSubscription()` and `Table.createSubscription()` instead.

### Full-table subscriptions

For some applications, it does not make sense to subscribe to only a limited range of rows, because the entire table is needed. In a browser-based JS application, charts are probably the most common example of this. Take care to avoid inundating the client with data. JS is a singly threaded language, and millions of rows could pause it for seconds, depending on how many columns are being read.

To create a full-table subscription, call [`Table.createSubscription()`](/core/client-api/javascript/classes/dh.Table.html#createSubscription) with an options object. Its `columns` property is required and sets the columns to include. The method returns a [`TableSubscription`](/core/client-api/javascript/classes/dh.TableSubscription.html), which has:

- An `"updated"` event that fires when data is available.
- A [`close()`](/core/client-api/javascript/classes/dh.TableSubscription.html#close) method that stops the subscription.

Like viewport subscriptions, full-table subscriptions fire their events from the subscription instance, not from the `Table` instance.

```javascript
const subscription = table.createSubscription({ columns: table.columns });

subscription.addEventListener("updated", (event) => {
  const data = event.detail;
  console.log(
    `${data.added.size} added, ${data.removed.size} removed, ${data.modified.size} modified`,
  );
});
```

Since full-table subscriptions tend to be larger than viewports, their event data implements [`SubscriptionTableData`](/core/client-api/javascript/interfaces/dh.SubscriptionTableData.html). This specialized API finds only the added, modified, and removed rows. The full set of rows is still available. However, for very large tables, avoid reading every row when only a few have changed. Instead, use the `added`, `removed`, and `modified` range sets. Do not reuse values in these range sets or in the `fullIndex` range set between updates, because values may be rearranged internally.

## Related documentation

- [Use the JS API](../../how-to-guides/use-jsapi.md)
- [JS API reference (TypeDoc)](/core/client-api/javascript/modules/dh.html)
