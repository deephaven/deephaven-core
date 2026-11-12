---
title: Is it possible to disable the Deephaven console?
---

Yes. Set the configuration property `deephaven.console.disable` to `true`. With Docker, pass it as a JVM argument through `START_OPTS` in your `docker-compose.yml`:

```yaml
environment:
  - START_OPTS=-Xmx4g -Ddeephaven.console.disable=true
```

With other installs, add `-Ddeephaven.console.disable=true` to the JVM arguments or set it in a [configuration file](../../how-to-guides/configuration/config-file.md).

This stops API clients, including the web IDE, from starting console (REPL) sessions. The server keeps running and still serves tables, for example from [application mode](../../how-to-guides/application-mode.md). The default is `false`.

## Related documentation

- [Configure the Deephaven console service](../../how-to-guides/configuration/console-service.md)
- [Create a Deephaven configuration file](../../how-to-guides/configuration/config-file.md)
- [Application mode](../../how-to-guides/application-mode.md)

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
