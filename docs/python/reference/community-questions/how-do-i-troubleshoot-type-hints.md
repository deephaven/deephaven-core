---
title: How do I troubleshoot type hints?
sidebar_label: How do I troubleshoot type hints?
---

<em>My table has a column that's the wrong type. I checked my typecasting and type hints, and it should produce the correct type. What's going on?</em>

<p></p>

There are multiple things that can cause a column to be of the wrong type. If a column should be an integer type but instead contains decimal numbers, there could be a `NaN` value poisoning it. [Here](/blog/2022/08/03/gremlins-in-data#nans-in-pandas-dataframes) is an example where this happens. If a column should be numeric, but is of string type, it has a non-numeric value somewhere. A good way to find exactly where these things happen is to [convert the column to a pandas DataFrame](../../how-to-guides/use-pandas.md) and search for the offending value.

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
