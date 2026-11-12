---
title: Where can I find the password for the Deephaven IDE?
---

<em>I started a Deephaven server. I want to connect to the IDE via my web browser, but it's asking me for a password I didn't set. Where can I find the password?</em>

<p></p>

The password the IDE asks for is Deephaven's pre-shared key (PSK). If you didn't set one, Deephaven generated a random key at startup and printed it in the server log in a line that starts `Superuser access through pre-shared key is enabled`. The log also includes a URL that ends in `/?psk=<key>`, which logs you in directly.

With Docker, view the log with `docker compose logs`.

To avoid this in the future, [set the key yourself](../../how-to-guides/authentication/auth-psk.md#setting-your-own-key) or use [anonymous authentication](../../how-to-guides/authentication/auth-anon.md).

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
