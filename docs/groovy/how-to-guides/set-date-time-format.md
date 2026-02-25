---
title: Set date-time format
---

By default, [date-time](../reference/query-language/types/date-time.md) columns are displayed in the long format. If the [date-time](../reference/query-language/types/date-time.md) column is explicitly named `Timestamp`, the column will be displayed as `hh:mm:ss.fff`.

From the UI, you can adjust how to display the [date-time](../reference/query-language/types/date-time.md) value for all or selected tables in your session. Note that these settings do not change the underlying value.

## Settings menu

You can change the default time zone and [date-time](../reference/query-language/types/date-time.md) formats for all the tables in your session from the **Settings** menu.

Open the **Settings** menu at the top-right of the UI:

![The Settings menu icon](../assets/how-to/date-time/date-time9.png)

The presents the following options:

![The Settings menu](../assets/how-to/date-time/date-time8.png)

### Time Zone

This sets the default time zone used in all tables in your session. Corresponding timestamps in the tables are adjusted accordingly.

### DateTime

This sets the default format when [date-time](../reference/query-language/types/date-time.md) values are presented in table columns. Two additional options allow you to:

- include the time zone in the [date-time](../reference/query-language/types/date-time.md) value.
- add the "T" separator between the date and time portion of the [date-time](../reference/query-language/types/date-time.md) value.

### Default formatting rule

This creates a default rule to be applied to all tables in your Deephaven instance. You can create rules for various columns and types. In this example, we create a rule for all columns with the name `Timestamp` and the type [date-time](../reference/query-language/types/date-time.md):

<LoopedVideo src='../assets/how-to/date-time/date-time13.mp4' />

## Column header menu

You can change the [date-time](../reference/query-language/types/date-time.md) format for an individual table by clicking its column header:

![The column header menu, with date-time options open](../assets/how-to/date-time/date-time11.png)

## Related documentation

- [Time in Deephaven](../conceptual/time-in-deephaven.md)
- [date-time](../reference/query-language/types/date-time.md)
