---
title: Can I reset the Python kernel without restarting the Deephaven Docker container?
---

<em>My query has been running for a long time and appears to be stuck. Is there a way for me to reset the Python kernel without restarting the Deephaven Docker container?</em>

<p></p>

No. The Python interpreter runs inside the Deephaven server process, so the only way to reset it is to restart the server. Deephaven doesn't currently support cancelling a running console command, so a stuck query also requires a server restart.

If you run Deephaven in Docker, restart the Deephaven container (for example, `docker compose restart`). If you installed Deephaven with pip, stop and restart the server process.

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
