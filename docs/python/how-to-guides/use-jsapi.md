---
title: Use the JS API
---

The [Deephaven JS API](/core/client-api/javascript/modules/dh.html) connects a browser application to a Deephaven Community Core instance. It handles the connection and authentication to the server. It runs code in a server console session. It also subscribes to table data, so your page stays current as the data updates in real time. The Deephaven web IDE is built on the same JS API.

A Node.js application can use the JS API too. The [`@deephaven/jsapi-nodejs`](https://github.com/deephaven/web-client-ui/tree/main/packages/jsapi-nodejs) package downloads the JS API from the server and loads it into Node.js.

In this guide, you'll learn how to create a basic web page that uses the JS API to create a table in Deephaven and display it. Then you'll extend the page to show a ticking table that updates live.

## Prerequisites

- A Deephaven server running locally on port `10000` with the Python console. See [Launch Deephaven from pre-built images](../getting-started/docker-install.md).
- A static file server to host the page. This guide uses Python's built-in `http.server`. JDK 18 and later include `jwebserver`, which also works.
- The Deephaven server must use [pre-shared key (PSK) authentication](./authentication/auth-psk.md). The code in this guide uses the key `very-secret-password`. Either start the server with that key, or replace `very-secret-password` in the code with your server's key.
  - To set the key, start the server with `-Dauthentication.psk=very-secret-password`.
  - In Docker, pass that option through `START_OPTS`, as [Set a pre-shared key](../getting-started/docker-install.md#set-a-pre-shared-key) shows.

## Create a simple web page

Create a folder, add an empty `index.html` file, and start a server to host it:

```sh
mkdir api-example
cd api-example
touch index.html
python3 -m http.server
```

> [!TIP]
> To use `jwebserver` instead, run `jwebserver -p 8000` from the same folder.

Open [http://localhost:8000/](http://localhost:8000/) in a browser to view `index.html` from the file server. At first, the page is blank, because `index.html` is empty.

Open `index.html` for editing, and add the following:

```html
<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <title>Deephaven JS API example</title>
  </head>

  <body>
    <h3>Table data</h3>
    <table id="simpleTable"></table>

    <script type="module">
      import dh from "http://localhost:10000/jsapi/dh-core.js";

      // Add the rest of the code here
    </script>
  </body>
</html>
```

Refresh the page, and it shows the "Table data" heading. The import statement downloads the [Deephaven JS API](/core/client-api/javascript/modules/dh.html) and makes its namespace available locally as `dh`. The inline `script` doesn't have any actual code yet.

Let's add some code to make the page do something. Add all the code in the following sections to the inline `script` in the `body` element of the page.

## Connect to the server

First, open a connection to the server with a [`CoreClient`](/core/client-api/javascript/classes/dh.CoreClient.html):

```javascript
var client = new dh.CoreClient("http://localhost:10000");
```

Next, authenticate with [`login`](/core/client-api/javascript/classes/dh.CoreClient.html#login). This example uses [pre-shared key (PSK) authentication](./authentication/auth-psk.md) with the key `very-secret-password` from the [Prerequisites](#prerequisites):

```javascript
await client.login({
  type: "io.deephaven.authentication.psk.PskAuthenticationHandler",
  token: "very-secret-password",
});
```

The login type and token depend on how your server is configured:

- On a server with [anonymous authentication](./authentication/auth-anon.md), pass `{ type: dh.CoreClient.LOGIN_TYPE_ANONYMOUS }`.
- On a server with [username/password authentication](./authentication/auth-uname-pw.md), pass `{ type: dh.CoreClient.LOGIN_TYPE_PASSWORD, username: "user", token: "password" }`.

After the client connects and authenticates, get its [`IdeConnection`](/core/client-api/javascript/classes/dh.IdeConnection.html) with [`getAsIdeConnection`](/core/client-api/javascript/classes/dh.CoreClient.html#getAsIdeConnection). An [`IdeConnection`](/core/client-api/javascript/classes/dh.IdeConnection.html) starts console sessions on the server with [`startSession`](/core/client-api/javascript/classes/dh.IdeConnection.html#startSession). The session type must match the server's console language, so start a `python` session:

```javascript
var connection = await client.getAsIdeConnection();
var ide = await connection.startSession("python");
```

## Create and retrieve a table

Run the code that creates the table with [`runCode`](/core/client-api/javascript/classes/dh.IdeSession.html#runCode). This code creates a [static table](../conceptual/table-types.md#static-tables) called `remote_table` with 10 rows and three columns: `I`, `J`, and `K`. Because it's Python code, be careful with indentation when creating the template string.

```javascript
await ide.runCode(`
from deephaven import empty_table
remote_table = empty_table(10).update_view(formulas=["I = i", "J = I * I", "K = i % 2 == 0 ? \`Hello\` : \`World\`"])
`);
```

Retrieve the table as a JS API [`Table`](/core/client-api/javascript/classes/dh.Table.html) object. [`getObject`](/core/client-api/javascript/classes/dh.IdeSession.html#getObject) takes the variable's type and name:

```javascript
var table = await ide.getObject({
  type: dh.VariableType.TABLE,
  name: "remote_table",
});
```

## Display the table

Create a table header element (`thead`) with the table columns:

```javascript
var header = document.createElement("thead");
var headerRow = document.createElement("tr");
table.columns.forEach((column) => {
  var td = document.createElement("td");
  td.innerText = column.name;
  headerRow.appendChild(td);
});
header.appendChild(headerRow);
```

A [viewport](../reference/js-api/concepts.md#viewports) is the set of rows and columns that the client asks the server to send. A viewport subscription keeps that data up to date as a [ticking table](../conceptual/table-types.md#standard-streaming-tables) changes. It stays open until you close it.

How the page uses its subscription depends on the table:

- For a static table such as `remote_table`, the page reads the data once and then closes the subscription to release its resources.
- For a ticking table, the page leaves the subscription open and rebuilds the table body each time the subscription fires its `"updated"` event. [Display a ticking table](#display-a-ticking-table) shows how.

`remote_table` is static, so the following code reads it once:

1. [`createViewportSubscription`](/core/client-api/javascript/classes/dh.Table.html#createViewportSubscription) asks the server for rows `0` through `9` of every column. [`dh.RangeSet.ofRange`](/core/client-api/javascript/classes/dh.RangeSet.html#ofRange) includes both ends, so that's all 10 rows.
2. [`getViewportData`](/core/client-api/javascript/classes/dh.TableViewportSubscription.html#getViewportData) waits for the data and returns it.
3. The loop builds one `tr` element per row.
4. [`close`](/core/client-api/javascript/classes/dh.TableViewportSubscription.html#close) ends the subscription.

```javascript
var subscription = table.createViewportSubscription({
  rows: dh.RangeSet.ofRange(0, 9),
  columns: table.columns,
});
var tbody = document.createElement("tbody");
var viewportData = await subscription.getViewportData();
var rows = viewportData.rows;
for (var i = 0; i < rows.length; i++) {
  var tr = document.createElement("tr");
  for (var j = 0; j < table.columns.length; j++) {
    var td = document.createElement("td");
    td.textContent = rows[i].get(table.columns[j]);
    tr.appendChild(td);
  }
  tbody.appendChild(tr);
}
subscription.close();
```

Retrieve the `table` element on the page and append the header and body to it:

```javascript
var tableElement = document.getElementById("simpleTable");
tableElement.appendChild(header);
tableElement.appendChild(tbody);
```

Refresh the page. After the table loads, the browser shows its 10 rows under the "Table data" heading. `I` counts from 0 to 9, `J` holds its square, and `K` alternates between `Hello` and `World`.

Congratulations! You've used the JS API to create a table in Deephaven and display it in the browser.

## Display a ticking table

The static table above never changes. The JS API's real strength is live data. A viewport subscription to a [ticking table](../conceptual/table-types.md#standard-streaming-tables) sends the browser each update to the rows it covers.

To try it, add a second heading and table element to the `body` of `index.html`, after the first table:

```html
<h3>Ticking data</h3>
<table id="tickingTable"></table>
```

Then add the following code to the end of the inline `script`. It creates a [time table](./time-table.md) that adds a row every second. [`reverse`](../reference/table-operations/sort/reverse.md) puts the newest row first, so rows `0` through `9` are always the 10 newest rows.

```javascript
await ide.runCode(`
from deephaven import time_table
ticking_table = time_table("PT1s").update_view(formulas=["X = i"]).reverse()
`);

var ticking = await ide.getObject({
  type: dh.VariableType.TABLE,
  name: "ticking_table",
});
```

Next, subscribe to the first 10 rows. This time, keep the subscription open:

1. [`createViewportSubscription`](/core/client-api/javascript/classes/dh.Table.html#createViewportSubscription) asks for rows `0` through `9`, as before.
2. [`addEventListener`](/core/client-api/javascript/classes/dh.TableViewportSubscription.html#addEventListener) listens for the subscription's `"updated"` event. The event fires when the first data arrives and again each time the server sends an update.
3. Each time, the listener rebuilds the table body from `event.detail.rows`, which holds every row now in the viewport.

```javascript
var tickingSubscription = ticking.createViewportSubscription({
  rows: dh.RangeSet.ofRange(0, 9),
  columns: ticking.columns,
});
var tickingBody = document.createElement("tbody");
document.getElementById("tickingTable").appendChild(tickingBody);

tickingSubscription.addEventListener("updated", (event) => {
  var newRows = event.detail.rows.map((row) => {
    var tr = document.createElement("tr");
    ticking.columns.forEach((column) => {
      var td = document.createElement("td");
      var value = row.get(column);
      td.textContent = column.type === "java.time.Instant"
        ? dh.i18n.DateTimeFormat.format("yyyy-MM-dd HH:mm:ss", value)
        : value;
      tr.appendChild(td);
    });
    return tr;
  });
  tickingBody.replaceChildren(...newRows);
});
```

Refresh the page. Under "Ticking data", a new row appears at the top every second. Each row shows its `Timestamp` and a counter, `X`, that grows by 1 with each new row. The JS API delivers `Timestamp` as a [`DateWrapper`](/core/client-api/javascript/classes/dh.DateWrapper.html). Its text form is the number of nanoseconds since the epoch, which is hard to read. So the listener checks each column's type. For a `java.time.Instant` column such as `Timestamp`, it calls [`dh.i18n.DateTimeFormat.format`](/core/client-api/javascript/classes/dh.i18n.DateTimeFormat.html#format) to show the value as a date and time in the browser's time zone.

The table keeps updating until you close the browser tab. To stop the updates sooner, call [`close`](/core/client-api/javascript/classes/dh.TableViewportSubscription.html#close) on `tickingSubscription`.

## Final product

The complete `index.html` file, with both tables, looks like this:

```html
<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <title>Deephaven JS API example</title>
  </head>

  <body>
    <h3>Table data</h3>
    <table id="simpleTable"></table>
    <h3>Ticking data</h3>
    <table id="tickingTable"></table>

    <script type="module">
      import dh from "http://localhost:10000/jsapi/dh-core.js";
      var client = new dh.CoreClient("http://localhost:10000");

      await client.login({
        type: "io.deephaven.authentication.psk.PskAuthenticationHandler",
        token: "very-secret-password",
      });

      var connection = await client.getAsIdeConnection();
      var ide = await connection.startSession("python");

      await ide.runCode(`
from deephaven import empty_table
remote_table = empty_table(10).update_view(formulas=["I = i", "J = I * I", "K = i % 2 == 0 ? \`Hello\` : \`World\`"])
      `);

      var table = await ide.getObject({
        type: dh.VariableType.TABLE,
        name: "remote_table",
      });

      var header = document.createElement("thead");
      var headerRow = document.createElement("tr");
      table.columns.forEach((column) => {
        var td = document.createElement("td");
        td.innerText = column.name;
        headerRow.appendChild(td);
      });
      header.appendChild(headerRow);

      var subscription = table.createViewportSubscription({
        rows: dh.RangeSet.ofRange(0, 9),
        columns: table.columns,
      });
      var tbody = document.createElement("tbody");
      var viewportData = await subscription.getViewportData();
      var rows = viewportData.rows;
      for (var i = 0; i < rows.length; i++) {
        var tr = document.createElement("tr");
        for (var j = 0; j < table.columns.length; j++) {
          var td = document.createElement("td");
          td.textContent = rows[i].get(table.columns[j]);
          tr.appendChild(td);
        }
        tbody.appendChild(tr);
      }
      subscription.close();

      var tableElement = document.getElementById("simpleTable");
      tableElement.appendChild(header);
      tableElement.appendChild(tbody);

      await ide.runCode(`
from deephaven import time_table
ticking_table = time_table("PT1s").update_view(formulas=["X = i"]).reverse()
      `);

      var ticking = await ide.getObject({
        type: dh.VariableType.TABLE,
        name: "ticking_table",
      });

      var tickingSubscription = ticking.createViewportSubscription({
        rows: dh.RangeSet.ofRange(0, 9),
        columns: ticking.columns,
      });
      var tickingBody = document.createElement("tbody");
      document.getElementById("tickingTable").appendChild(tickingBody);

      tickingSubscription.addEventListener("updated", (event) => {
        var newRows = event.detail.rows.map((row) => {
          var tr = document.createElement("tr");
          ticking.columns.forEach((column) => {
            var td = document.createElement("td");
            var value = row.get(column);
            td.textContent =
              column.type === "java.time.Instant"
                ? dh.i18n.DateTimeFormat.format("yyyy-MM-dd HH:mm:ss", value)
                : value;
            tr.appendChild(td);
          });
          return tr;
        });
        tickingBody.replaceChildren(...newRows);
      });
    </script>
  </body>
</html>
```

> [!NOTE]
> The server hosts more JS API examples at [http://localhost:10000/jsapi](http://localhost:10000/jsapi). [`table_basic.html`](http://localhost:10000/jsapi/table_basic.html) builds a page like this one, but with two differences:
>
> - It uses the deprecated [`getTable`](/core/client-api/javascript/classes/dh.IdeSession.html#getTable), [`setViewport`](/core/client-api/javascript/classes/dh.Table.html#setViewport), and [`Table.getViewportData`](/core/client-api/javascript/classes/dh.Table.html#getViewportData) calls. This guide uses their replacements.
> - It logs in with [anonymous authentication](./authentication/auth-anon.md), so it runs only on a server that allows anonymous login.

## Related documentation

- [Table types](../conceptual/table-types.md)
- [Create a time table](./time-table.md)
- [Launch Deephaven from pre-built images](../getting-started/docker-install.md)
- [Pre-shared key authentication](./authentication/auth-psk.md)
- [Anonymous authentication](./authentication/auth-anon.md)
- [Username/password authentication](./authentication/auth-uname-pw.md)
- [Create an empty table](./new-and-empty-table.md#empty_table)
- [JS API concepts](../reference/js-api/concepts.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`update_view`](../reference/table-operations/select/update-view.md)
- [`time_table`](../reference/table-operations/create/timeTable.md)
- [`reverse`](../reference/table-operations/sort/reverse.md)
- [`empty_table` Pydoc](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.empty_table)
- [`update_view` Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.update_view)
- [`time_table` Pydoc](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.time_table)
- [`reverse` Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.reverse)
- [JS API docs](/core/client-api/javascript/modules/dh.html)
