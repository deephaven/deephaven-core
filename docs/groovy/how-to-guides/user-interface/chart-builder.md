---
title: Chart Builder
---

Deephaven makes it easy to visualize your data. You can plot data with code by using the [built-in plotting API](../plotting/api-plotting.md). You can also plot data directly in the [Deephaven IDE](./navigating-the-ui.md) user interface. This guide focuses on the Chart Builder feature, which creates line, bar, scatter, and pie charts from a table without writing any code. Charts you build from a [ticking table](../../conceptual/table-types.md) update as new data arrives.

## Open the Chart Builder

To open the Chart Builder, click the **Table Options** icon at the right of a table (circled in the screenshot below). Then select **Chart Builder** from the [**Table Options** menu](./formatting-tables.md#table-options-menu). [Tree tables](../tree-table.md) and [rollup tables](../rollup-table.md) don't offer the Chart Builder.

![The mouse hovers over the hamburger icon (Table Options) at the top right of a table](../../assets/how-to/plots/plots1.png)

![The Table Options menu expands to show available options](../../assets/how-to/plots/plots2.png)

The menu then shows the Chart Builder form, where you configure a chart of the table's data. Click **Back** to return to the menu.

![The Chart Builder screen](../../assets/how-to/plots/plots3.png)

## Configure and create a chart

You can choose from four different chart types:

- [Line](#line-bar-and-scatter)
- [Bar](#line-bar-and-scatter)
- [Scatter](#line-bar-and-scatter)
- [Pie](#pie)

Each chart type has its own fields. Click in a field to see the table's column names.

- **Line**, **Bar**, and **Scatter** charts take an **X-Axis** column and one or more **Series** columns. The **X-Axis** column sets each point's horizontal position. Each **Series** column sets the values plotted against it, and appears as its own line, set of bars, or set of points.
- **Pie** charts take a **Labels** column, which names the slices, and a **Values** column, which sets each slice's size.

Each field starts with one of the table's columns already selected, so you only need to change the fields you want to be different.

To plot another column in a line, bar, or scatter chart, click **Add Series**. The new **Series** field starts with the table's first column selected, so change it to the column you want.

To remove a series, click the trash can icon to the right of its **Series** field. The icon appears only when the chart has more than one series.

To start over, click **Reset**. The Chart Builder returns to a line chart with its default columns and **Sync State**.

The Chart Builder plots the table as it currently appears, with any active [filters](./filters.md), [sorts](./formatting-tables.md#sort-by), and [custom columns](./formatting-tables.md#custom-columns) applied. If the table has no [filters](./filters.md), it plots all of the data.

The table you open the Chart Builder from is the _parent table_. Below the column fields, choose whether the chart follows later [filters](./filters.md) and other changes you make to the parent table:

- **Sync State**: When selected, the chart updates to match any [filters](./filters.md) or other user modifications made to the parent table.
- **Freeze State**: When selected, the chart ignores [filters](./filters.md) and other user modifications made to the parent table after you create it. If the parent table is a [ticking table](../../conceptual/table-types.md), the chart still updates when new data arrives.

Once you make your selections, click **Create**. The chart opens as a new panel in your [workspace](./navigating-the-ui.md) with a title built from the column names, such as **Avg_Temp by Year**. Each time you click **Create**, the Chart Builder opens another new panel.

> [!CAUTION]
> You can't edit an existing chart configuration. To change a chart, create a new one in the Chart Builder.

## Examples

The next sections show an example of each chart type, using two tables of Seattle weather data:

- `hiLoByYear` has one row for each year from 2000 through 2020. Its `Avg_Temp`, `Lo_Temp`, and `Hi_Temp` columns hold the average, lowest, and highest of Seattle's daily mean temperatures for that year. The line, bar, and scatter examples use this table.
- `tempBands` counts the days in 2020 that fell into each temperature band. The pie example uses this table.

Run this script if you'd like to follow along:

```groovy test-set=1 order=hiLoByYear,tempBands
import static io.deephaven.api.agg.Aggregation.AggAvg
import static io.deephaven.api.agg.Aggregation.AggMin
import static io.deephaven.api.agg.Aggregation.AggMax
import static io.deephaven.csv.CsvTools.readCsv

seattleWeather = readCsv("https://media.githubusercontent.com/media/deephaven/examples/main/GSOD/csv/seattle.csv")

hiLoByYear = seattleWeather.view("Year = year(ObservationDate, timeZone(`UTC`))", "TemperatureF")
    .where("Year >= 2000 && Year <= 2020")
    .aggBy([
        AggAvg("Avg_Temp = TemperatureF"),
        AggMin("Lo_Temp = TemperatureF"),
        AggMax("Hi_Temp = TemperatureF")
    ], "Year")
    .formatColumns("Year = Decimal(`#`)")

tempBands = seattleWeather.view("Year = year(ObservationDate, timeZone(`UTC`))", "TemperatureF")
    .where("Year == 2020")
    .update("Band = TemperatureF < 45 ? `Cold` : TemperatureF < 65 ? `Mild` : `Warm`")
    .countBy("Days", "Band")
```

### Line, bar, and scatter

Open the Chart Builder from the `hiLoByYear` table to chart the average temperature for each year. Select **Line**, then click in the **X-Axis** field to see the table's column names:

![The Chart Builder for the hiLoByYear table, with the line chart type selected](../../assets/how-to/plots/plots5.png)

Select `Year` for the **X-Axis** and `Avg_Temp` for the **Series**. Click **Create** to generate a single-series chart.

![A line plot](../../assets/how-to/plots/plots6.png)

Let's make a multi-series bar chart. This time, select **Bar**, set the **X-Axis** to `Year`, and select `Lo_Temp` for the first **Series** field. Then click **Add Series**, select `Hi_Temp`, and click **Create**:

![A user creates a multi-series bar chart with the Chart Builder](../../assets/how-to/plots/plots4.gif)

![A multi-series bar chart](../../assets/how-to/plots/plots7.png)

To make a scatter chart, select **Scatter** and set the **X-Axis** and **Series** fields the same way as in the line chart.

### Pie

To chart the `tempBands` table, open the Chart Builder from that table, select **Pie**, select `Band` for **Labels** and `Days` for **Values**, and click **Create**:

![The pie chart options in the Chart Builder](../../assets/how-to/plots/plots10.png)

![A pie chart](../../assets/how-to/plots/plots11.png)

## Interact with charts

The following features work on any chart made with the Chart Builder or the [built-in plotting API](../plotting/api-plotting.md).

### Plot hover menu

Hovering your cursor over a chart brings up the following menu:

![A menu with buttons to download the plot as a PNG, zoom, pan, zoom in and out, autoscale, and reset axes](../../assets/how-to/plots/plots12.png)

- **Download plot as a png**: Save the plot locally as a PNG file.
- **Zoom**: Drag across the part of the chart you want to magnify. Double-click the chart to return it to its original view.
- **Pan**: Click and drag the chart to move other areas of it into view. Double-click the chart to return it to its original view.
- **Zoom in**: Zoom in on the chart.
- **Zoom out**: Zoom out on the chart.
- **Autoscale**: Rescale the axes so that all of the data fits in view.
- **Reset axes**: Return the axes to their original ranges.

For a large chart, Deephaven may downsample the data, plotting fewer points than the table holds so that the chart stays responsive. The menu then adds buttons for this:

- **Downsampling status**: Click to turn downsampling off or back on.
- **Downsampling failed** or **Error**: A warning button that appears when downsampling fails or the chart reports an error. Click it to see the error message.

While downsampling is in progress, or while a warning is showing, the menu stays visible without hovering.

### Tooltips

If you hover your cursor over the data shown in a chart, a tooltip shows the exact value of the data under the cursor.

![A tooltip displays when the user hovers over the plot data](../../assets/how-to/plots/plots13.png)

### Toggle series visibility

When a chart plots multiple series, you might want to temporarily hide one or more of them. A multi-series chart made with the [built-in plotting API](../plotting/api-plotting.md) shows a legend. Click a series name in the legend to hide that series. Click it again to show it.

Charts made with the Chart Builder don't show a legend.

![The user toggles which series are visible by clicking on their names in the legend](../../assets/how-to/plots/plots14.gif)

## Related documentation

- [Deephaven's table types](../../conceptual/table-types.md)
- [Real-time plots](../../getting-started/crash-course/plots.md)
- [Built-in plotting API](../plotting/api-plotting.md)
- [Navigate the GUI](./navigating-the-ui.md)
- [Filter without writing queries](./filters.md)
- [Use table tools in the UI](./formatting-tables.md)
- [Create a hierarchical tree table](../tree-table.md)
- [Create a hierarchical rollup table programmatically](../rollup-table.md)
- [Read CSV or other delimited files](../data-import-export/csv-import.md)
- [Multi-aggregation](../combined-aggregations.md)
- [`readCsv`](../../reference/data-import-export/CSV/readCsv.md)
- [`view`](../../reference/table-operations/select/view.md)
- [`year`](../../reference/time/datetime/year.md)
- [`timeZone`](../../reference/time/datetime/timeZone.md)
- [`where`](../../reference/table-operations/filter/where.md)
- [`update`](../../reference/table-operations/select/update.md)
- [`countBy`](../../reference/table-operations/group-and-aggregate/countBy.md)
- [`aggBy`](../../reference/table-operations/group-and-aggregate/aggBy.md)
- [`AggAvg`](../../reference/table-operations/group-and-aggregate/AggAvg.md)
- [`AggMin`](../../reference/table-operations/group-and-aggregate/AggMin.md)
- [`AggMax`](../../reference/table-operations/group-and-aggregate/AggMax.md)
- [`formatColumns`](../../reference/table-operations/format/format-columns.md)
- [`Table` Javadoc](/core/javadoc/io/deephaven/engine/table/Table.html)
- [`Aggregation` Javadoc](/core/javadoc/io/deephaven/api/agg/Aggregation.html)
