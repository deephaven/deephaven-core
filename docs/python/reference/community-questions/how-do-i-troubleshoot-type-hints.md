---
title: How do I troubleshoot type hints?
---

<em>My table has a column that's the wrong type. I checked my typecasting and type hints, and it should produce the correct type. What's going on?</em>

<p></p>

Several things can give a column the wrong type:

- **Missing or unrecognized return type hint** — If a Python function called in a query string has no return type hint, or a return hint that Deephaven doesn't recognize, the result column holds `PyObject` values instead of the intended type. Check that the function has a recognized return type hint, such as `-> int` or `-> float`. See [Type hints](../../how-to-guides/python-functions.md#type-hints).
- **Integer column shows decimals** — If the data came from a pandas DataFrame and an integer column contains decimal numbers, a `NaN` in the source data may have coerced the DataFrame column to float before Deephaven converted it. The [Gremlins in data blog post](/blog/2022/08/03/gremlins-in-data#nans-in-pandas-dataframes) shows an example. Deephaven integer columns use null sentinels rather than `NaN`, so this coercion happens on the pandas side.
- **Numeric column shows strings** — If a column that Deephaven inferred from a CSV file or a DataFrame should be numeric but holds strings, the source has a non-numeric value somewhere.

A good way to find where these problems occur is to [convert the column to a pandas DataFrame](../../how-to-guides/use-pandas.md) and search for the offending value.

See also: [Why does my function with type hints throw an error on null values?](./why-do-my-python-type-hints-error.md)

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
