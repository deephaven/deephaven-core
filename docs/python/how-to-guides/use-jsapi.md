---
title: Use the JS API
sidebar_label: JS API
---

In this guide, you'll learn how to create a basic web page and use the JS API to create a table in Deephaven and display it. The Deephaven JS API is used to connect to a Deephaven Community Core instance from a browser or `node.js` application. It manages all your server connections, table tracking, and server communications. The Deephaven Console experience is created using our JS API.

> [!NOTE]
> This guide assumes you have Deephaven running locally with Python. You can find the completed example available at [http://localhost:10000/jsapi/table_basic.html](http://localhost:10000/jsapi/table_basic.html). There are other examples available at [http://localhost:10000/jsapi](http://localhost:10000/jsapi), but we focus specifically on creating the basic table example step-by-step. This guide uses pre-shared key (PSK) authentication. For other authentication methods, see our [authentication guides](./authentication/auth-anon.md).

## Create a simple web page

Create a new folder to work out of, add in `index.html`, and start up a server to host the file:

```sh
mkdir api-example
cd api-example
touch index.html
python -m http.server
```

> [!TIP]
> This example uses a basic Python server to host the `index.html` file, but you can substitute another server.

You should now be able to navigate to your server (located at at http://0.0.0.0:8000/ by default) to view our `index.html`. Initially, it will be a blank page - nothing is in the `index.html` yet!

Open up the `index.html` file for editing, and add the following:

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

    <script>
      (async () => {
        import dh from 'http://localhost:10000/jsapi/dh-core.js';

        // Add the rest of the code here
      })();
    </script>
  </body>
</html>
```

Now, if you refresh the page, you should see "Table data" output. The import statement downloads the Deephaven Core JS API and makes its namespace available locally as `dh`. The inline `script` doesn't have any actual code yet. Let's add some code to make the page do something. Add all the code below into the inline `script` in the `body` element of the page.

First, we need to open a connection to the server:

```javascript
var client = new dh.CoreClient("http://localhost:10000");
```

And then authenticate. This example assumes you are using the "pre-shared key" method of authentication, with key "very-secret-password":

```javascript
await client.login({
  type: "io.deephaven.authentication.psk.PskAuthenticationHandler",
  token: "very-secret-password",
});
```

This is specific to how your server is configured and deployed - for Deephaven Enterprise deployments, this will use type `io.deephaven.proto.auth.Token` and require an auth token be created for each new connection.

After a connection is established and authenticated, ask the client for the IdeConnection instance, and start a console session. For this example, we will start a `python` session (the default):

```javascript
var connection = await client.getAsIdeConnection();
var ide = await connection.startSession("python");
```

Next, we need to run the code that will create the table. This code creates a static table called `remoteTable` with 10 rows and three columns: `I`, `J`, and `K`. Because it's Python code, be careful with indentation when creating the template string.

```javascript
await ide.runCode(`
from deephaven import empty_table
remoteTable = empty_table(10).update_view(formulas = ["I=i", "J=I*I", "K=i%2==0?\`Hello\`:\`World\`"])
`);
```

Retrieve the JS Table object:

```javascript
var table = await ide.getTable("remoteTable");
```

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

Set the viewport and extract the data to build the body of the table. Since the bottom of the specified viewport is inclusive and the table is ten rows, we only need `0,9`:

```javascript
table.setViewport(0, 9);
var tbody = document.createElement("tbody");
var viewportData = await table.getViewportData();
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
```

Retrieve the table element in our document and append the table to it:

```javascript
var tableElement = document.getElementById("simpleTable");
tableElement.appendChild(header);
tableElement.appendChild(tbody);
```

Now refresh the page. After the table loads, you should see the table data output in the browser.

Congratulations! You've successfully used the JS API to create a table and read it.

## Final product

The final product of the HTML page is:

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

    <script>
      (async () => {
        import dh from 'http://localhost:10000/jsapi/dh-core.js';
        var client = new dh.CoreClient('http://localhost:10000');

        await connection.login({
          type: 'io.deephaven.authentication.psk.PskAuthenticationHandler',
          token: 'very-secret-password',
        });

        var ide = await connection.startSession('python');
        var connection = await client.getAsIdeConnection();

        await ide.runCode(`
from deephaven import empty_table
remoteTable = empty_table(10).updateView(formulas = ["I=i", "J=I*I", "K=i%2==0?\`Hello\`:\`World\`"])
      `);

        var table = await ide.getTable('remoteTable');

        var header = document.createElement('thead');
        var headerRow = document.createElement('tr');
        table.columns.forEach((column) => {
          var td = document.createElement('td');
          td.innerText = column.name;
          headerRow.appendChild(td);
        });
        header.appendChild(headerRow);

        table.setViewport(0, 9);
        var tbody = document.createElement('tbody');
        var viewportData = await table.getViewportData();
        var rows = viewportData.rows;
        for (var i = 0; i < rows.length; i++) {
          var tr = document.createElement('tr');
          for (var j = 0; j < table.columns.length; j++) {
            var td = document.createElement('td');
            td.textContent = rows[i].get(table.columns[j]);
            tr.appendChild(td);
          }
          tbody.appendChild(tr);
        }

        var tableElement = document.getElementById('simpleTable');
        tableElement.appendChild(header);
        tableElement.appendChild(tbody);
      })();
    </script>
  </body>
</html>
```

## Related documentation

- [Launch Deephaven from pre-built images](../getting-started/docker-install.md)
- [Create an empty table](./new-and-empty-table.md#empty_table)
