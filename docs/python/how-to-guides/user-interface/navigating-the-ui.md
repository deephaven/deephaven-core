---
title: Navigate the GUI
---

The Deephaven graphical user interface (GUI) runs in your web browser. It lets you explore, filter, sort, and plot your tables without writing more code. Real-time tables update on screen as new data arrives. This guide shows where to find the GUI's main panels, menus, and tools. Let's take a tour.

The screenshot below outlines each item in a different color. The legend after it follows the screen layout: the top bar, the console pane, the right-hand pane, and the viewing area. Each entry gives the item's outline color and location.

![A screenshot of the GUI, with color-coded outlines around each item in the legend below](../../assets/how-to/ui/gui-tour-py.png)

## Legend

### Top bar

- **Controls menu** (rust outline, top right): Add and remove [filters](./filters.md), create widgets such as [Markdown widgets](./markdown-widget.md), and open the [Linker](./filters.md#linker) tool.
- **Panels menu** (dark blue outline, right of **Controls**): Search for and open the tables, plots, and widgets in your session, and import, export, or reset your layout.
- **Settings menu** (teal outline, gear icon at the far right, tooltip **User Settings**): Change your settings, such as the [time zone](../set-date-time-format.md), number formats, how tables display empty and null strings, [table density](./set-table-density.md), and the [theme](../custom-themes.md). Your browser stores these settings.

### Console pane

- **Console** (red outline, first tab on the left): Type and run code here. You do most of your work in this panel.
- **Log** (purple outline, tab next to **Console**): View recent [server log](../logs.md) output. The panel shows the most recent log messages when you open it, then new messages as they arrive.
- **Console Options menu** (magenta outline, ⋮ icon at the right end of the console toolbar, tooltip **More Actions...**): Change the console's behavior or [upload a table from a file](./upload-table-from-file.md). Behavior options include whether panels open automatically and whether they close when the connection drops.

### Right-hand pane

- **File Explorer** (light blue outline, a tab in the right-hand pane): Create, open, copy, rename, move, and delete the notebooks and folders stored on your Deephaven server. See [Create and organize notebooks](./file-explorer.md).
- **Command History** (yellow outline, the other tab in the right-hand pane): View a [history of the commands](../logs.md#command-history) you've run in the console.

### Viewing area

The viewing area, also called the workspace, sits below the console and right-hand panes in the default layout. It shows the [notebooks](./file-explorer.md), tables, plots, and widgets you open.

You can rearrange any panel, not just the ones in the viewing area. Drag a panel's tab to move it. Drop it beside another panel to split the space, or on another panel's row of tabs to stack the two.

- **Panel tabs** (lavender outline, the row of tabs at the top of the viewing area): Switch between the [notebooks](./file-explorer.md), tables, plots, and other widgets open in the viewing area.
- **Cell context menu** (pink outline, around the table cells): Right-click a cell in a table to open the cell context menu, which has options to copy data, [filter](./filters.md), and more.
- **Column header menu** (medium blue outline, around a column header): Right-click a column header to open the [column header menu](./formatting-tables.md#column-header-menu), which has options to [freeze](./formatting-tables.md#freeze-column), [filter](./filters.md), [sort](./formatting-tables.md#sort-by), and more.
- **Table Options menu** (dark green outline, hamburger icon at the top right of a table, tooltip **Table Options**): Click the hamburger icon to open the [Table Options menu](./formatting-tables.md#table-options-menu). Use it to [organize columns](./formatting-tables.md#organize-columns), apply [conditional formatting](./formatting-tables.md#conditional-formatting), or [aggregate columns](./formatting-tables.md#aggregate-columns). The menu also lets you [download a CSV](./formatting-tables.md#download-csv) of the table data, among other tools.
- **Plot hover menu** (orange outline, across the top of a plot): Hover over a Deephaven plot to reveal the [plot hover menu](./chart-builder.md#plot-hover-menu), which has options to save the plot as an image, zoom in and out, and more.

## Related documentation

- [Filter without writing queries](./filters.md)
- [Create and manage Markdown widgets](./markdown-widget.md)
- [Set date-time format](../set-date-time-format.md)
- [Set table density](./set-table-density.md)
- [Create custom themes](../custom-themes.md)
- [Upload a table from a file](./upload-table-from-file.md)
- [Create and organize notebooks](./file-explorer.md)
- [Retrieve logs](../logs.md)
- [Use table tools in the UI](./formatting-tables.md)
- [Chart Builder](./chart-builder.md)
