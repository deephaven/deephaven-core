---
title: Connecting and authenticating
---

# Connecting and authenticating

A {@link dh.CoreClient} connects to a server URL and {@link dh.CoreClient.login | logs in}.
{@link dh.CoreClient.getAsIdeConnection | getAsIdeConnection} then gives an {@link dh.IdeConnection}, which fetches
tables and {@link dh.IdeConnection.startSession | starts sessions} for running code on the server.
