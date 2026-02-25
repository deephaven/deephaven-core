---
title: Application Mode configuration
---

The `*.app` file defines the configuration for Application Mode. This file can be named anything as long as it ends with `.app` and is stored in the directory pointed to by `-Ddeephaven.application.dir`. See the example below.

```
type=script
scriptType=python
enabled=true
id=hello.world
name=Hello World!
file_0=helloWorld.py
```

<ParamTable>
<Param name="type" type="String">

The type of script to run. This can be one of `script`, `static`, `dynamic`, or `qst`.

</Param>
<Param name="scriptType" type="String">

The language of the script. One of `groovy` or `python`.

</Param>
<Param name="enabled" type="Boolean">

Boolean value to indicate whether or not to run the scripts.

- `true` (default) causes the scripts to run.
- `false` skips the scripts.

</Param>
<Param name="id" type="String">

An identifier used when API clients connect to this server Application Mode instance.

</Param>
<Param name="name" type="String">

A description that is displayed to users when mentioning this application.

</Param>
<Param name="file_<n>" type="String">

A list of scripts to run.

</Param>
</ParamTable>

## Related documentation

- [How to use Application Mode video](https://youtu.be/GNm1k0WiRMQ)
- [How to use Application Mode libraries](../../how-to-guides/application-mode-libraries.md)
- [How to use Application Mode scripts](../../how-to-guides/application-mode-script.md)
- [Initialize server state with Application Mode](../../how-to-guides/application-mode.md)
