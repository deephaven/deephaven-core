---
title: Filter without writing queries
---

This guide shows you how to filter tables and plots directly in the user interface, without writing queries. To combine filters precisely, or to keep a filter in code that you can re-run or share, use [filter query methods](../use-filters.md) instead. Here, we focus on the shortcuts built directly into the IDE.

Like the filters you write in queries, these filters run on the server. Deephaven tables can [tick](../../conceptual/table-update-model.md), which means their rows change in real time. On a ticking table, a UI filter keeps applying as new rows arrive.

Filter features, and the settings that control them, are in four menus. Open the **Table Options** menu from the hamburger icon at the top right of a table. The **Controls** menu is in the IDE's [top bar](./navigating-the-ui.md#top-bar). Each menu offers these features:

- Column header context menu (right-click a column header):

  - [Quick Filters](#quick-filters)
  - [Advanced Filters](#advanced-filters)

- Table cell context menu (right-click a cell):

  - [Filter by Value](#filter-by-values)

- **Table Options** menu:

  - [Quick Filters](#quick-filters)
  - [Search Bar](#search-bar)
  - [Advanced Settings](#advanced-settings)

- **Controls** menu:

  - [Input Filter](#input-filters)
  - [Dropdown Filter](#dropdown-filter)
  - [Linker](#linker)
  - [Filter Sets](#filter-sets)
  - [Clear All Filters](#clear-all-filters)

Input Filters, Dropdown Filters, and the Linker also filter plots. See [Filter plots](#filter-plots).

Let's look at each option in turn. The screenshots use tables of cryptocurrency trade data, with columns such as `Exchange` and `Instrument`.

## Quick filters

The **Quick Filters** feature adds a filter row directly below the column headers. Type a value in the filter field under any column header to filter the table by that column.

You can add **Quick Filters** from a table's **Table Options** menu, or by right-clicking a column header:

![Access Quick Filters by right-clicking a column header](../../assets/how-to/quick-filter.gif)

> [!TIP]
> You can also open or close the **Quick Filters** bar using the keyboard shortcut <kbd>Ctrl+F</kbd> or <kbd>⌘F</kbd>.

### What you can type

A plain value keeps the rows that equal it. In a text column, matching ignores case, and you can type an operator before the value:

- `kraken` keeps rows whose value is `kraken`.
- `!=kraken` keeps rows whose value is not `kraken`.
- `~btc` keeps rows whose value contains `btc`, and `!~btc` keeps rows whose value does not contain it.
- `btc*` keeps rows whose value starts with `btc`, and `*usd` keeps rows whose value ends with `usd`.
- `null` keeps rows with no value, and `!=null` keeps rows that have one.

In a number column, you can type a comparison or a special value:

- `>100`, `>=100`, `<100`, and `<=100` compare each value with `100`.
- `=100` and `!=100` keep rows that equal or don't equal `100`.
- `null`, `nan`, `inf`, and `-inf` match null, NaN, positive infinite, and negative infinite values. Put `!=` in front to exclude them instead.

To combine conditions in one field, join them with `&&` (and) or `||` (or). For example, `>100 && <200` keeps values greater than 100 and less than 200. Deephaven evaluates `&&` before `||`.

An [Input Filter](#input-filters) value uses the same syntax.

## Advanced filters

The **Advanced Filters** dialog combines conditions and value selection to filter one column. Open it by right-clicking a column header, or by clicking a column's funnel icon in the [**Quick Filters**](#quick-filters) bar:

![The Advanced Filters dialog](../../assets/how-to/advanced-filters.png)

The dialog applies each change to the table as you make it. Click **Done** to close the dialog.

### Sorting options

The first two buttons at the top of the **Advanced Filters** dialog sort the entire table by the selected column in descending or ascending order. The third button maximizes the dialog:

![Sorting options at the top right of the Advanced Filters dialog](../../assets/how-to/advanced-filter2.png)

### Enter value(s)

The upper section of the dialog provides options for filtering the data in the selected column. These options depend on the data type:

![Advanced Filters options](../../assets/how-to/advanced-filters2.gif)

After you enter a first value, **AND** and **OR** buttons appear below the value field. Click one to add a second condition to the dialog:

![Once one filter is selected, an option to add more filters appears](../../assets/how-to/advanced-filters3.gif)

The example above adds a second filter with the **OR** option, so the table keeps only the rows whose `Instrument` value contains `BTC` or `ETH`.

### Select value(s)

The lower section of the **Advanced Filters** dialog filters the table by specific values that you select. Search for a value in the **Find** field, or scroll through the list to select or deselect values. The **Find** field's placeholder shows the column name, such as **Find Instrument...**:

!["Select values" options in the Advanced Filters dialog](../../assets/how-to/advanced-filters4.png)

Date-time columns have no **Find** field.

### How the two sections combine

The upper and lower sections of the **Advanced Filters** dialog filter the same column, and Deephaven combines them with AND: a row must pass both sections to appear.

### Removing filters

- Click the trash can icon to the right of a filter value in the **Advanced Filters** dialog to remove that filter.
- Click **Reset** in the **Advanced Filters** dialog to remove all Advanced Filters on the selected column.
- Right-click the column header and choose **Clear Column Filter** to remove all filters on that column, or **Clear Table Filters** (<kbd>Ctrl+Shift+E</kbd> or <kbd>⇧⌘E</kbd>) to remove all of the table's filters:

![Options to remove filters in the column header context menu](../../assets/how-to/advanced-filters5.png)

## Filter by value(s)

Right-clicking a table cell shows the **Filter by Value** feature:

![The Filter by Value feature](../../assets/how-to/filter-by-value.png)

The options available depend on the column's data type. For a string column, for example, you can keep only the rows whose text:

- Is exactly the cell's value.
- Is not exactly the cell's value.
- Contains the cell's value.
- Does not contain the cell's value.
- Starts with the cell's value.
- Ends with the cell's value.

After you apply the first filter, the context menu offers options to add more filters or clear them.

You can also select multiple values to filter by:

![Filtering by multiple values](../../assets/how-to/filter_by_values.gif)

## Search bar

When you set the **Search Bar** option to **On** in a table's **Table Options** menu, Deephaven adds a search bar to that table:

![Toggle the search bar on/off in the Table Options menu](../../assets/how-to/searchbar.png)

By default, the search covers all columns. To choose which columns the search covers, click the **Configure Columns** button at the right of the search bar:

![The search bar's settings dialog](../../assets/how-to/searchbar2.png)

The search also covers vector and array columns. A row matches when any element of the array matches the search text.

> [!TIP]
> You can also open or close the search bar using the keyboard shortcut <kbd>Ctrl+Shift+F</kbd> or <kbd>⇧⌘F</kbd>.

## Input filters

An Input Filter applies a filter to every table in your dashboard that has a column with the same name and type as the filter column. To add one, choose **Input Filter** from the **Controls** menu:

![The Input Filter](../../assets/how-to/input-filter.gif)

Choose a **Filter Column** from the dropdown list and click **Save**. Then enter a value. Deephaven applies the filter to your dashboard as soon as you enter it.

In this example, we filter all the open tables to one exchange:

![A user filters open tables to the kraken exchange, then to gemini](../../assets/how-to/input-exchange.gif)

When you close the Input Filter panel, it stops filtering, but tables keep the filter it already applied. To remove that filter, clear the Input Filter's value before you close the panel, or use [Clear All Filters](#clear-all-filters).

By default, an Input Filter adds to a table's other filters. See [Advanced settings](#advanced-settings).

## Dropdown filter

The Dropdown Filter control creates a dropdown list of values. Pick a value from the list to change the filter applied to your tables.

Choose **Dropdown Filter** from the **Controls** menu. In the dialog that opens, choose:

- A source column. Under **Source Column**, click **Select a column**, then click a column in any table. That column supplies the values in the dropdown list, and its table becomes the source table.
- A filter column. Under **Filter Column**, choose the column to filter. The list shows the columns whose type matches the source column.

Click **Save**. The panel then shows the dropdown list. When you pick a value from it, Deephaven filters every other table in the dashboard that has a column with the filter column's name and type. Deephaven does not filter the source table itself.

<LoopedVideo src='../../assets/how-to/dropdown-filter.mp4' />

The dropdown list shows every row of the source column, including duplicates, so the source column should hold unique values. You can create a table of unique values in one of two ways:

- Apply [`selectDistinct`](../../reference/table-operations/select/select-distinct.md) in a table's query.
- Use [**Select Distinct Values**](./formatting-tables.md#select-distinct-values) in a table's **Table Options** menu.

> [!NOTE]
> Dropdown lists work best with a small number of options. The source table can have at most 256 rows. A larger source table shows an error message.

The Dropdown Filter stays linked to its source table:

- Sorting or filtering the source table updates the values in the dropdown list.
- Deleting the source column or closing the source table unlinks the Dropdown Filter.

To edit the Dropdown Filter's configuration, click the gear icon at the top right of its panel.

By default, a Dropdown Filter adds to a table's other filters. See [Advanced settings](#advanced-settings).

## Linker

The Linker filters one or more tables based on the row you double-click in another table. First, link the tables: one table is the source, and one or more tables are targets. When you double-click a row in the source table, Deephaven filters the linked columns in the target tables by the values in that row.

> [!TIP]
> You can also open the Linker using the keyboard shortcut <kbd>Ctrl+L</kbd> or <kbd>⌘L</kbd>.

When you select the **Linker** tool from the **Controls** menu, the following instructions appear:

![The Linker panel](../../assets/how-to/ui/linker1.png)

Click a column in the source table, then move the arrow cursor to the column you want to filter in the target table and click it:

![Move the arrow cursor from the source column to the target column](../../assets/how-to/ui/linker2.png)

Each link appears as an arrow between the two columns. Select a link to show its menu, then expand the menu to choose the comparison operator the link uses:

![The expanded filter options menu](../../assets/how-to/ui/linker5.png)

Click **Done** to save your changes.

To delete a link, select it and click the trash can icon, or Alt+click the link. **Clear All** deletes every link.

Now that the tables are linked, double-clicking a row in the source table filters the target table:

![Double-click a row in the source table to filter the target table](../../assets/how-to/ui/linker.gif)

To filter several target tables from one source, repeat these steps for each target:

![Multiple tables are linked at once](../../assets/how-to/ui/linker3.png)

Deephaven saves your links. When you open the Linker tool again, it shows them so you can edit or delete them.

By default, a link filter adds to the target table's other filters. See [Advanced settings](#advanced-settings).

## Filter sets

A Filter Set is a named record of the filters applied across the dashboard at the moment you capture it. You can reapply the set later. A set records each table's [Quick Filters](#quick-filters) and [Advanced Filters](#advanced-filters), plus the values of any [Input Filter](#input-filters) and [Dropdown Filter](#dropdown-filter) controls. It does not record filters on plots.

To capture a set:

1. From the **Controls** menu, choose **Filter Sets**.
2. Click **Capture filter set**.
3. Enter a name for your set.
4. Choose **Restore only filters**, or choose **Restore full table state** to also restore other table settings such as sorts.
5. Click the save icon next to the name.
6. Click **Save**.

The panel shows a dropdown list of all available sets:

<LoopedVideo src='../../assets/how-to/filtersets.mp4' />

Select a set name to apply the saved filters. To apply the selected set again, click the **Apply Filter Set** button next to the list.

A set applies only to panels that are still open. It skips tables you have closed since you captured it.

To edit your sets, click the gear icon at the top right of the **Filter Sets** panel. In the editor that opens, you can update, rename, or delete the highlighted Filter Set. Select **Capture new filter set** to create a new Filter Set.

## Clear all filters

The **Clear All Filters** option in the **Controls** menu removes every filter applied through the UI in the current dashboard at once. This includes Quick Filters, Advanced Filters, search bar text, Input Filter and Dropdown Filter values, and link filters. Your links stay in place, and filters in your query code are not affected:

![Clear all filters via the Controls menu](../../assets/how-to/clearall.png)

> [!TIP]
> You can also clear all filters using the keyboard shortcut <kbd>Ctrl+E</kbd> or <kbd>⌘E</kbd>.

## Filter plots

[Input Filters](#input-filters), [Dropdown Filters](#dropdown-filter), and the [Linker](#linker) also filter these plots:

- Plots built from a [`SelectableDataSetOneClick`](../../reference/query-language/types/SelectableDataSetOneClick.md), which [`oneClick`](../../reference/plot/oneClick.md) creates.
- [Chart Builder](./chart-builder.md) plots set to **Sync State**. These follow every filter on the table they were built from, including the ones these controls apply.

## Advanced settings

Controls and the [Linker](#linker) apply their own kinds of filters:

- A control filter is a filter applied by a control, such as an [Input Filter](#input-filters) or a [Dropdown Filter](#dropdown-filter).
- A link filter is a filter applied to a table by the Linker.

By default, both kinds add to a table's other filters. On the column they filter, they replace only some existing filters:

- A control filter replaces any [Quick Filter](#quick-filters) or [Advanced Filter](#advanced-filters) on that column.
- A link filter replaces any Quick Filter on that column. An Advanced Filter on that column stays.

Two settings under **Advanced Settings** in a table's **Table Options** menu change this. Each one makes one kind of filter replace all of the table's existing filters instead:

![The Advanced Settings options menu](../../assets/how-to/ui/advanced_settings.png)

- **Clear current table filters before applying new filters from a control**: Set this option to **On** to let control filters replace all of the table's filters.
- **Clear current table filters before applying new filters from an incoming link filter**: Set this option to **On** to let link filters replace all of the table's filters.

## Related documentation

- [Filter table data](../use-filters.md)
- [Chart Builder](./chart-builder.md)
- [Use table tools in the UI](./formatting-tables.md)
- [Navigate the GUI](./navigating-the-ui.md)
- [`oneClick`](../../reference/plot/oneClick.md)
- [`SelectableDataSetOneClick`](../../reference/query-language/types/SelectableDataSetOneClick.md)
- [`selectDistinct`](../../reference/table-operations/select/select-distinct.md)
- [`oneClick` Javadoc](/core/javadoc/io/deephaven/plot/filters/Selectables.html)
- [`SelectableDataSetOneClick` Javadoc](/core/javadoc/io/deephaven/plot/filters/SelectableDataSetOneClick.html)
- [`selectDistinct` Javadoc](/core/javadoc/io/deephaven/api/TableOperations.html#selectDistinct(java.lang.String...))
