---
title: Web Defaults
sidebar_label: Web Defaults
---

You can configure default settings for the Web UI using the Deephaven configuration file. See [Create a Deephaven configuration file](./config-file.md) for details.

To configure a default setting:

1. Set the value of a config property in the configuration file.
1. Add the property name to `client.configuration.list`. This is a comma-separated list of property names that the Web UI will recognize and use for configuration.
   You can find the default list in the [dh-defaults.prop file (line 68)](https://github.com/deephaven/deephaven-core/blob/main/props%2Fconfigs%2Fsrc%2Fmain%2Fresources%2Fdh-defaults.prop#L68).

For example:

```properties
# Add `timeZone` and `dateTimeFormat` properties to the list.
client.configuration.list=timeZone,dateTimeFormat,java.version,deephaven.version,barrage.version,groovy.version,python.version,http.session.durationMs,file.separator,web.storage.layout.directory,web.storage.notebook.directory,web.webgl,web.webgl.editable,web.flattenViewports

# Define default values for the properties
timeZone=America/Chicago
dateTimeFormat=yyyy-MM-dd HH:mm:ss
```

Configuration properties correspond to UI fields in the Settings UI.

![UI Settings Panel](../../assets/how-to/ui/ui-settings-panel.png)

## Default format and time zone properties

| Label     | Field                         | Server prop                |
| --------- | ----------------------------- | -------------------------- |
| Time zone | Select                        | `timeZone`                 |
| DateTime  | Select                        | `dateTimeFormat`           |
|           | Show time zone in dates       | `showTimeZone`             |
|           | Show 'T' separator            | `showTSeparator`           |
| Decimal   | Input                         | `decimalFormat`            |
| Integer   | Input                         | `integerFormat`            |
|           | Show truncated numbers as ### | `truncateNumbersWithPound` |
| String    | Show empty strings as empty   | `showEmptyStrings`         |
|           | Show null strings as null     | `showNullStrings`          |
| Rollup    | Show extra "group" column     | `showExtraGroupColumn`     |

## Advanced properties

| Field        | Server prop (value) | Server prop (editable) |
| ------------ | ------------------- | ---------------------- |
| Enable WebGL | `web.webgl`         | `web.webgl.editable`   |

## Related documentation

- [Set date-time format](../../how-to-guides/set-date-time-format.md)
- [Create a Deephaven configuration file](./config-file.md)
