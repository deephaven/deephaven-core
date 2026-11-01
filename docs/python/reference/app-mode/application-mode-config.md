---
title: Application Mode configuration
---

[Application Mode](../../how-to-guides/application-mode.md) starts applications when the Deephaven server starts. Each `*.app` file in the application directory defines one application.

The `deephaven.application.dir` property sets the _application directory_. When that property is unset, the server uses the `app.d` directory inside the [Deephaven configuration directory](../../how-to-guides/configuration/configure-production-application.md#deephaven-server-bootstrap-configuration), if it exists.

Each `*.app` file can have any name that ends in `.app`. The file must be a regular file. It must not be a symbolic link, and it must be in the _application directory_.

The server loads `.app` files in lexicographic order by file name. It skips any file that is not a readable `.app` regular file, without reporting an error. A readable `.app` file with an invalid definition, such as one with no `type`, stops the server from starting.

The following file defines a [script application](#script-applications) that runs `helloWorld.py` on a Python server. The [Common properties](#common-properties) and [Script applications](#script-applications) sections describe each property.

```
type=script
scriptType=python
enabled=true
id=hello.world
name=Hello World!
file_0=helloWorld.py
```

## Common properties

<ParamTable>
<Param name="type" type="String">

Required. The kind of application. Valid values:

- `script` runs the scripts listed in `file_<n>`.
- `static` creates the application from the class named in `class`. See [Class-based applications](#class-based-applications).
- `dynamic` creates the application from the class named in `class`. See [Class-based applications](#class-based-applications).
- `qst` is reserved and not implemented, so an application of this type never runs.

</Param>
<Param name="enabled" type="Boolean">

Optional. Whether the server loads this application.

- `true` (default) loads the application. The value is not case-sensitive.
- Any other value, such as `false` or `yes`, skips the application. The server still reads and validates a skipped application's definition, so a disabled file with an invalid definition still stops the server from starting.

</Param>
</ParamTable>

## Script applications

These properties apply when `type` is `script`.

<ParamTable>
<Param name="scriptType" type="String">

Required. The language of the script, `python` or `groovy`. For an enabled application, it must match the language of the server (set with `-Ddeephaven.console.type`), or the server fails to start. The value is not case-sensitive.

</Param>
<Param name="id" type="String">

Required. A unique identifier for the application. API clients use it to look up the application's fields.

A field is a named value that the application publishes for clients to fetch by name.

No two enabled applications on a server can share an `id`. The `id` `scope` is reserved, so an application with that `id` cannot publish fields.

</Param>
<Param name="name" type="String">

Required. The application's display name.

</Param>
<Param name="file_<n>" type="String">

Required. The path to one script to run, where `<n>` is a string of ASCII digits, such as `file_0` and `file_1`. Keys whose suffix contains other characters, such as `file_+1` or `file_a`, are ignored. A number above 2147483647 stops the server from starting. Specify at least one. Relative paths resolve against the application directory.

Scripts run in ascending order of `<n>`, not in the order they appear in the file. The server ignores keys whose suffix is not an integer, such as `file_a`.

</Param>
</ParamTable>

## Class-based applications

This property applies when `type` is `static` or `dynamic`.

<ParamTable>
<Param name="class" type="String">

Required. The fully qualified name of a class that has a no-argument constructor. The class implements one of these interfaces:

- For `static` applications, [`io.deephaven.appmode.Application.Factory`](/core/javadoc/io/deephaven/appmode/Application.Factory.html). The application declares all of its exported variables when the server starts.
- For `dynamic` applications, [`io.deephaven.appmode.ApplicationState.Factory`](/core/javadoc/io/deephaven/appmode/ApplicationState.Factory.html). The application can add or remove exported variables as its state changes.

</Param>
</ParamTable>

## Related documentation

- [How to use Application Mode video](https://youtu.be/GNm1k0WiRMQ)
- [Application Mode](../../how-to-guides/application-mode.md)
- [How to use Application Mode libraries](../../how-to-guides/application-mode-libraries.md)
- [How to use Application Mode scripts](../../how-to-guides/application-mode-script.md)
- [`deephaven.appmode` Pydoc](/core/pydoc/code/deephaven.appmode.html)
- [`Application.Factory` Javadoc](/core/javadoc/io/deephaven/appmode/Application.Factory.html)
- [`ApplicationState.Factory` Javadoc](/core/javadoc/io/deephaven/appmode/ApplicationState.Factory.html)
