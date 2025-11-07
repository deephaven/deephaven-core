---
title: Excel Client
---

Deephaven's Excel Client allows you to integrate Deephaven data into your spreadsheets through an Excel add-in. You can query static or dynamic tables using custom filters that you define and use the results of your queries in other calculations. As ticking data changes, the rest of the calculations in your sheet will also update.

This guide shows you how to install the add-in, connect to Deephaven, and use the add-in to access your data.

## Install Deephaven's Excel add-in

Before installing Deephaven's Excel add-in, make sure you have satisfied the prerequisites:

- Install the latest 64-bit version of [Microsoft Excel](https://www.microsoft.com/en-us/microsoft-365/excel) with [Microsoft Office 365](https://www.microsoft.com/en-us/microsoft-365).
  - You can alternatively use Office 2021 or later.
- Ensure that you are running a 64-bit Windows operating system.

> [!NOTE]
> Please contact Deephaven support for access to the Excel add-in installer.

<!--TODO: replace with official link when repo is ready-->

Download the Deephaven Excel add-in installer when you have met the prerequisites. Close any open instances of Excel, then run the installer.

![The Deephaven Excel add-in installer](../../assets/how-to/excel/excel-installer.png)

Accept the default installation path, then click **Next**:

![The Deephaven Excel add-in installer's install path selection screen](../../assets/how-to/excel/excel-path.png)

Click **Install** to begin the installation.

> [!NOTE]
> At this stage in the installation process, you will be prompted to install [.NET Desktop Runtime](https://dotnet.microsoft.com/en-us/download/dotnet/8.0) if you do not already have it. This is a prerequisite for the Deephaven Excel add-in. Click **Install** to continue.

When you see an **Add Deephaven as Trusted Publisher** dialog box, choose:

- **Yes** to add Deephaven to your computer's list of [Trusted Publishers](https://learn.microsoft.com/en-us/microsoft-365-apps/security/trusted-publisher). This is needed so that Excel can run Deephaven code.
- **No** if you are unsure or your company's policy prohibits it. In this case, the add-in will not run. To proceed, you should consult with your company's IT administrator.

![A dialog box pops up with an option to add Deephaven to your list of Trusted Publishers](../../assets/how-to/excel/excel-trusted-pub.png)

Finally, click **Finish** to complete the installation.

## Connect to Deephaven

To connect Deephaven's Excel client with Deephaven, find "Add-ins" in Excel's menu and look for the "Deephaven" section in the ribbon that appears. Inside the Deephaven drop-down menu, click on **Connections**.

> [!NOTE]
> If the Deephaven add-in was not installed correctly or does not have the correct permissions, the add-ins menu might not appear. In this case, you may need to consult your IT administrator for assistance.

![An image showing the Excel add-ins menu through which the Deephaven add-in is accessed](../../assets/how-to/excel/excel-connections.png)

Next, in the **Connection Manager**, select **New** to create a new connection.

![The Connection Manager](../../assets/how-to/excel/new-connection.png)

<Tabs
values={[
{ label: 'Community Core', value: 'core', },
{ label: 'Enterprise Core+', value: 'core+', },
]
}>

<TabItem value='core'>

Select **Community Core**, choose a Connection ID, and enter your Connection String. The Connection ID is a short string of your choosing that you can use to reference this connection from your Excel formulas. The Connection String specifies the host address and port of the Community Core server you are connecting to. The host address might be a numeric IP address like `10.128.0.21` or a name like `myserver.example.com`. When you start Deephaven, the port number is specified in the configuration and defaults to `10000`. In our configuration, the connection string is `10.128.0.21:10000`; yours will likely be different.

Before setting the Connection ID and Connection String, click **Test Creds** to ensure the connection is valid. Finally, click **Set Credentials** to save the connection.

![The Credentials Editor pop-up](../../assets/how-to/excel/connection.png)

</TabItem>
<TabItem value='core+'>

Select **Enterprise Core+** and choose a Connection ID. The Connection ID is a short string of your choosing that you can use to reference this connection from your Excel formulas.

Next, enter the JSON URL for your Enterprise Core+ server, which is `<your_server_address>:<port>/iris/connection.json`. `<your_server_address>` and `<port>` have the same values as you would use to access the Deephaven web console; in other words, if you access your console at `https://mydeephavenserver.example.com:8000`, your JSON URL would be `https://mydeephavenserver.example.com:8000/iris/connection.json`.

Enter the UserId and Password that you use to log onto your Enterprise Core+ server. If you have permissions, you can enter another user's name in the **OperateAs** field to operate as that user.

Before setting the credentials, click **Test Creds** to ensure the connection is valid. Finally, click **Set Credentials** to save the connection.

![The Credentials Editor, with the 'set credentials' button highlighted](../../assets/how-to/excel/connect-plus2.png)

</TabItem>
</Tabs>

That's it! You're connected to Deephaven, and you can now use the Deephaven Excel client to access your data.

## Usage

You can use two functions to access data in Deephaven from Excel: [`DEEPHAVEN_SNAPSHOT`](#deephaven_snapshot) and [`DEEPHAVEN_SUBSCRIBE`](#deephaven_subscribe). Both methods have the same syntax; they differ in that [`DEEPHAVEN_SNAPSHOT`](#deephaven_snapshot) retrieves a single snapshot of the data, while [`DEEPHAVEN_SUBSCRIBE`](#deephaven_subscribe) subscribes to the data and updates it in real time. Each method takes the following parameters:

- `TABLE_DESCRIPTOR`: A string that points the Deephaven Excel client to the Deephaven connection and table you want to access.
  - If you are using a Community Core connection, the format is `"<ConnectionID>:<TableName>"`. For example, `"con1:t1"` to access table `t1` in connection `con1`.
  - If you are using an Enterprise Core+ connection, the format is `"<ConnectionID>:<PersistentQueryName>/<TableName>"`. For example, `"con1+:pq1/t1"` to access table `t1` in persistent query `pq1` in connection `con1+`.
- `FILTER_STRING` (optional): A conditional expression interpreted by the Deephaven server to filter the data that Excel receives. For example, ``"Symbol = `AAPL` && Quantity > 50"``.
- `WANT_HEADERS` (optional): A Boolean value that determines whether Excel includes the headers (column names) from the Deephaven table. If `TRUE`, Excel will include the headers. The default is `FALSE`.

> [!NOTE]
> Many examples in this guide connect to a Deephaven Community Core instance. The `TABLE_DESCRIPTOR` argument in the `DEEPHAVEN_SNAPSHOT` and `DEEPHAVEN_SUBSCRIBE` functions is the only difference between using the Excel client with Community Core and Enterprise Core+; if you are following along with a Core+ connection, simply replace the `TABLE_DESCRIPTOR` argument with the appropriate Enterprise Core+ format.

The examples in this guide use the Deephaven server-side Python API. Groovy also works, so use the language you are most comfortable with.

### `DEEPHAVEN_SNAPSHOT`

To retrieve a snapshot of a table from Deephaven, use `DEEPHAVEN_SNAPSHOT`. This function fetches data from a Deephaven table, optionally filtered by a filter string, at a specific point in time. It works on both static and ticking tables.

<Tabs
values={[
{ label: 'Community Core', value: 'core', },
{ label: 'Enterprise Core+', value: 'core+', },
]
}>

<TabItem value='core'>

First, create a static Deephaven table in the Deephaven console:

```python order=static_table
from deephaven import empty_table

static_table = empty_table(10).update(formulas=["X = randomInt(0,10)"])
```

Next, from your Excel sheet, use `=DEEPHAVEN_SNAPSHOT("con1:static_table",,FALSE)` to retrieve a snapshot of the table `static_table` from connection `con1`:

![A Deephaven table updating in Excel](../../assets/how-to/excel/excel-snap.gif)

</TabItem>
<TabItem value='core+'>

The Deephaven Excel client can fetch tables from any Persistent Query you can access. For this example, [create a new Persistent Query](/enterprise/docs/interfaces/web/query-monitor) called `test_pq` and save a script that generates a simple table.

```python order=static_table
from deephaven import empty_table

static_table = empty_table(10).update(formulas=["X = randomInt(0,10)"])
```

Next, from your Excel sheet, use `=DEEPHAVEN_SNAPSHOT("con1+:test_pq/static_table",,FALSE)` to retrieve a snapshot of the table `static_table` from connection `con1+`:

![The user subscribing to a static Deephaven table from Excel](../../assets/how-to/excel/excel-snap-plus.gif)

> [!NOTE]
> The remainder of this section uses the Community Core connection format. If using an Enterprise Core+ connection, replace the `TABLE_DESCRIPTOR` argument with the appropriate Enterprise Core+ format.

</TabItem>
</Tabs>

You can add a filter to the snapshot by including a filter string: `=DEEPHAVEN_SNAPSHOT("con1:static_table", "X > 5", FALSE)`:

![Applying a filter to a Deephaven table](../../assets/how-to/excel/excel-filter-string.gif)

Change the `WANT_HEADERS` parameter to `TRUE` to include the headers in the snapshot. Here, use `=DEEPHAVEN_SNAPSHOT("con1:static_table",,TRUE)`:

![A Deephaven table in Excel, with headers](../../assets/how-to/excel/excel-inc-header.gif)

### `DEEPHAVEN_SUBSCRIBE`

To subscribe to updates from ticking tables, use `DEEPHAVEN_SUBSCRIBE`. This function fetches the data from a ticking Deephaven table and updates as the source table changes.

<Tabs
values={[
{ label: 'Community Core', value: 'core', },
{ label: 'Enterprise Core+', value: 'core+', },
]
}>

<TabItem value='core'>

First, create a ticking table in the Deephaven console:

```python ticking-table order=null
from deephaven import time_table
import random


def rand_exchange() -> str:
    return random.choice(["kraken", "gemini", "bitstamp", "binance", "coinbase-pro"])


crypto_table = (
    time_table("PT1S")
    .update(
        [
            "Exchange = rand_exchange()",
            "Price = randomDouble(2000,4000)",
            "Size = randomDouble(0,4)",
        ]
    )
    .tail(10)
)
```

![The ticking `crypto_table` updating in the Deephaven console](../../assets/how-to/excel/crypto-table.gif)

Next, fetch the table with `=DEEPHAVEN_SUBSCRIBE("con1:crypto_table",,FALSE)`:

![The ticking 'crypto_table' updating live in Excel](../../assets/how-to/excel/subscribe-crypto.gif)

</TabItem>
<TabItem value='core+'>

For this example, [create a new Persistent Query](/enterprise/docs/interfaces/web/query-monitor) called `test_pq` and save a script that generates a ticking table:

```python ticking-table order=null
from deephaven import time_table
import random


def rand_exchange() -> str:
    return random.choice(["kraken", "gemini", "bitstamp", "binance", "coinbase-pro"])


crypto_table = (
    time_table("PT1S")
    .update(
        [
            "Exchange = rand_exchange()",
            "Price = randomDouble(2000,4000)",
            "Size = randomDouble(0,4)",
        ]
    )
    .tail(10)
)
```

!['crypto_table' updating in Deephaven](../../assets/how-to/excel/crypto-table.gif)

Next, fetch the table with `=DEEPHAVEN_SUBSCRIBE("con1+:test_pq/crypto_table",,FALSE)`:

![Fetching the table from the Persistent Query](../../assets/how-to/excel/subscribe-crypto-plus.gif)

> [!NOTE]
> The remainder of this section uses the Community Core connection format. If using an Enterprise Core+ connection, replace the `TABLE_DESCRIPTOR` argument with the appropriate Enterprise Core+ format.

</TabItem>
</Tabs>

Cells that depend on the data in our new ticking rows will also update when the data changes. For example, you can use relative references in the formula `=D3 * E3` to multiply the `Price` and `Size` columns for individual rows:

![A cell that is dependent on D3 and E3 via relative reference updates in time with the upstream cells](../../assets/how-to/excel/mult-relative-ref.gif)

> [!NOTE]
> For ticking data, you should expect Excel to update your sheet every few seconds.

Having a fixed number of formulas in your Excel sheet is a limitation because you may not know in advance how many rows your table has. For example, add a filter string to the `DEEPHAVEN_SUBSCRIBE` formula above - ``=DEEPHAVEN_SUBSCRIBE("con1:crypto_table","Exchange = `kraken`", FALSE)``. This filters the table only to show rows where the `Exchange` column is "kraken", and the table size changes as it ticks:

![The filtered table ticking in Excel](../../assets/how-to/excel/filter-kraken-v2.gif)

To ensure that your `Price * Size` column matches the size of the source table, use an `INDEX` formula: `=INDEX(B3#,,2) * INDEX(B3#,,3)`. The `INDEX` formula is discussed in more detail [below](#extract-a-column-from-a-dynamic-array).

![`INDEX` can be used to ensure a result column has the same size as the source table](../../assets/how-to/excel/index-formula.gif)

### Cell references vs. dynamic array references

Cell references like the formula `=B3` copy the value of the referenced cell and update when the referenced cell changes. Dynamic array references like `=B3#` copy the entire array of values from the referenced cell and update when the referenced cell changes. For example:

![A cell reference (`=B3#`) can copy an entire array of values, which updates alongside the referenced cell](../../assets/how-to/excel/cell-v-dynamic-array.gif)

> [!NOTE]
> If you change your table's definition on the server, your `DEEPHAVEN_SNAPSHOT` and `DEEPHAVEN_SUBSCRIBE` functions may still reference data from the original table. To address this, press the **Reconnect** button in the Connection Manager.

### Extract a column from a dynamic array

Say you want to take column 3 of the ticking table above and use it elsewhere in your spreadsheet. You can use the Excel formula `INDEX` as in `=INDEX(B3#,,3)` to extract column 3 from the dynamic array at `B3#`:

![Column 3 extracted from the dynamic array at `B3#`, ticking in Excel](../../assets/how-to/excel/index-1.gif)

Extract column 4 by using `=INDEX(B3#,,4)`:

![Column 4 extracted from the dynamic array at `B3#`, ticking in Excel](../../assets/how-to/excel/index-2.gif)

### Multiply one column by another

Now, you can multiply one column by another by using dynamic array references, such as `=G3# * H3#`:

![Multiplying one column by another using dynamic array references in Excel](../../assets/how-to/excel/mult-cols.gif)

### Inline Excel INDEX commands

You don't have to extract columns somewhere on the sheet before you can use them in a formula. Instead, you can refer to them inside a formula directly. For example, we can get the same result as the formula in the previous section with `=INDEX(B3#,,3) * INDEX(B3#,,4)`:

![Usng inline Excel INDEX commands to multiply columns directly in a formula](../../assets/how-to/excel/inline-index.gif)

### Filter dynamic arrays with `FILTER`

You can use a filter expression on one dynamic array to control what rows are selected from another dynamic array. We will demonstrate this in several steps.
First, use the `INDEX` command to extract the `Exchange` column from the B3# table and store it at G3#. Use the formula `=INDEX(B3#,,2)`:

![The `INDEX` command is used to extract the `Exchange` column from the B3# table and store it at G3#](../../assets/how-to/excel/excel-filter2.gif)

Next, Excel looks at the filter `G3# = "kraken"` to determine which rows of G3# match "kraken", then pulls in the corresponding rows from B3# to form the result. The full expression is `=FILTER(B3#,G3#="kraken")`:

![A ticking table in Excel that filters the table at B3# by the rows in G3# that match "kraken"](../../assets/how-to/excel/excel-filter-2.gif)

To make this filtering operation more compact, you can inline the expression that defines the filter source rather than having it occupy space on your sheet. This allows us to combine the previous two formulas into one: `=FILTER(B3#,,INDEX(B3#,,2)="kraken")`.

![The whole above table can be filtered from the source table with the inline formula `=FILTER(B3#,,INDEX(B3#,,2)="kraken")`](../../assets/how-to/excel/filter-whole-table.gif)

### Filter server-side

You can also filter tables on the server side by using a filter string.

If you are working with a large table, you may prefer to filter the table on the server side before it reaches the client. Do this by using `DEEPHAVEN_SUBSCRIBE` with a filter string, such as ``=DEEPHAVEN_SUBSCRIBE("con1:crypto_table","Exchange = `kraken`",FALSE)``:

![Filter server-side by passing a filter string such as "Exchange = `kraken`" to the second argument of `DEEPHAVEN_SUBSCRIBE`](../../assets/how-to/excel/excel-server-side2.gif)

You can also make the `DEEPHAVEN_SUBSCRIBE` formula dependent on a cell value. For example, `=DEEPHAVEN_SUBSCRIBE("con1:crypto_table", F2, FALSE)`, with F2 containing the formula ``="Exchange = `" & C2 & "`"``. This way, you can change the filter string by changing the value in cell C2:

<LoopedVideo src='../../assets/how-to/excel/cell-dependent2.mp4' />

## Related Documentation

- [`empty_table`](../../how-to-guides/new-and-empty-table.md#empty_table)
- [`time_table`](../../how-to-guides/time-table.md)
- [`update`](../../reference/table-operations/select/update.md)
- [`tail`](../../reference/table-operations/filter/tail.md)
