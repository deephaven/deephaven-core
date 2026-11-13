---
title: Web defaults
---

As a server administrator, you can set default values for many of the options in the web UI [**Settings** menu](../user-interface/navigating-the-ui.md), such as the time zone and the date-time format. Users can still change these settings for themselves. When a user changes a setting in the **Settings** menu, their browser stores the new value, and it overrides the server default.

The only setting you can stop users from changing is **Enable WebGL**, as described in [Advanced properties](#advanced-properties).

The **Settings** menu groups its options into sections. This page covers the **Default Format & Time zone**, **Editor**, and **Advanced** sections, the only sections whose defaults a server can set. The screenshot shows the **Default Format & Time zone** section expanded:

![The Settings menu, with the Default Format & Time zone section open](../../assets/how-to/ui/ui-settings-panel.png)

## Set a default

You set defaults in the Deephaven configuration file. See [Create a Deephaven configuration file](./config-file.md) for details.

To configure a default setting:

1. Set the value of one or more of the properties listed in the tables below in the configuration file.
1. Add your property names to `client.configuration.list`, the comma-separated list of configuration properties that the server sends to the web UI. Setting this property replaces its default value, so copy the default list from [`dh-defaults.prop`](https://github.com/deephaven/deephaven-core/blob/main/props/configs/src/main/resources/dh-defaults.prop) and append your names to it.
1. Restart the server. The server reads the configuration file only at startup.
1. Check the new default in a browser where that setting hasn't been changed. A user who already changed a setting in their browser keeps their own value.

For example, this configuration file sets defaults for the time zone and the date-time format:

```properties
includefiles=dh-defaults.prop

# Keep the Groovy console. dh-defaults.prop sets the console type to python.
deephaven.console.type=groovy

# Redefine the list: the defaults from dh-defaults.prop plus timeZone and dateTimeFormat.
client.configuration.list=java.version,deephaven.version,barrage.version,groovy.version,python.version,http.session.durationMs,file.separator,web.storage.layout.directory,web.storage.notebook.directory,web.webgl,web.webgl.editable,web.flattenViewports,timeZone,dateTimeFormat

# Define default values for the properties
timeZone=America/Chicago
dateTimeFormat=yyyy-MM-dd HH:mm:ss
```

Keep the `deephaven.console.type=groovy` line. Without it, the server tries to start a Python console instead of a Groovy one. On the `server-slim` Docker image, which doesn't include Python, the server then fails to start. That image sets the Groovy console type only in its own `deephaven.prop`, and your file replaces that file, as described in [Use a configuration file](./docker-application.md#use-a-configuration-file).

## Default format and time zone properties

These properties set the defaults for the **Default Format & Time zone** options in the [**Settings** menu](../user-interface/navigating-the-ui.md). To use one, set its value in your configuration file and add its name to `client.configuration.list`, as described in [Set a default](#set-a-default).

| Setting                       | Property                   | Value                                                                                                                                       |
| ----------------------------- | -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| Time zone                     | `timeZone`                 | A time zone ID from the menu's list, such as `America/Chicago`, `America/New_York`, `Europe/London`, or `UTC`                               |
| DateTime                      | `dateTimeFormat`           | A date-time format, such as one of the menu's choices: `yyyy-MM-dd HH:mm:ss`, `yyyy-MM-dd HH:mm:ss.SSS`, or `yyyy-MM-dd HH:mm:ss.SSSSSSSSS` |
| Show time zone in dates       | `showTimeZone`             | `true` or `false`                                                                                                                           |
| Show 'T' separator            | `showTSeparator`           | `true` or `false`                                                                                                                           |
| Decimal                       | `decimalFormat`            | A number format, such as `###,##0.0000`                                                                                                     |
| Integer                       | `integerFormat`            | A number format, such as `###,##0`                                                                                                          |
| Show truncated numbers as ### | `truncateNumbersWithPound` | `true` or `false`                                                                                                                           |
| Show empty strings as empty   | `showEmptyStrings`         | `true` or `false`                                                                                                                           |
| Show null strings as null     | `showNullStrings`          | `true` or `false`                                                                                                                           |
| Show extra "group" column     | `showExtraGroupColumn`     | `true` or `false`                                                                                                                           |

## Editor properties

These properties set the defaults for the **Editor** options in the [**Settings** menu](../user-interface/navigating-the-ui.md), which control the notebook editor. To use one, set its value in your configuration file and add its name to `client.configuration.list`, as described in [Set a default](#set-a-default).

**Format on Save** and the two Ruff settings apply only to Python notebooks, because the web UI formats and lints only Python code. **Enable Minimap** applies to every notebook.

| Setting                   | Property                                            | Value                                                                                |
| ------------------------- | --------------------------------------------------- | ------------------------------------------------------------------------------------ |
| Enable Minimap            | `web.user.notebookSettings.isMinimapEnabled`        | `true` or `false`                                                                    |
| Format on Save            | `web.user.notebookSettings.formatOnSave`            | `true` or `false`                                                                    |
| Enable Ruff Python Linter | `web.user.notebookSettings.python.linter.isEnabled` | `true` or `false`                                                                    |
| Customize Ruff Settings   | `web.user.notebookSettings.python.linter.config`    | A JSON object of [Ruff settings](https://docs.astral.sh/ruff/settings/), on one line |

The `web.user.notebookSettings.python.linter.config` value replaces the UI's built-in Ruff settings rather than merging with them, so include every setting you want. If the value isn't valid JSON, the UI ignores it and uses its built-in settings.

## Advanced properties

These properties control the **Enable WebGL** option in the **Advanced** section of the [**Settings** menu](../user-interface/navigating-the-ui.md). The default `client.configuration.list` already includes both, so you only need to set their values. Setting `web.webgl.editable` to `false` stops users from changing the **Enable WebGL** option, but it doesn't override a WebGL setting that a user saved in their browser before you set it.

| Property             | Effect                                                             | Value             |
| -------------------- | ------------------------------------------------------------------ | ----------------- |
| `web.webgl`          | Default for the **Enable WebGL** option                            | `true` or `false` |
| `web.webgl.editable` | Whether users can change **Enable WebGL** in the **Settings** menu | `true` or `false` |

## Related documentation

- [Set date-time format](../set-date-time-format.md)
- [Create a Deephaven configuration file](./config-file.md)
- [Configuration properties reference](./configuration-properties.md#web-ui-configuration)
- [Configure the Deephaven Docker application](./docker-application.md)
- [Navigate the GUI](../user-interface/navigating-the-ui.md)
