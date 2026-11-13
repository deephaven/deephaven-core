---
title: "Create interactive, real-time browser dashboards with deephaven.ui"
---

<div className="comment-title">

A framework for building real-time data-focused apps in Python

</div>

[`deephaven.ui`](/core/ui/docs/) is a Deephaven [plugin](./install-use-plugins.md) for building real-time, data-focused apps in Python using a [React](https://react.dev/)-like component model.

<Pullquote> Create dashboards entirely in Python — no front-end engineering, JavaScript, or CSS required. </Pullquote>

Many other Python dashboarding frameworks handle real-time data by refreshing it on a timer. Deephaven components and plots instead update as the streaming data they display changes, so there's no polling or manual refresh to write.

Here is a [`deephaven.ui`](/core/ui/docs/) dashboard backed by a mock streaming dataset, to give you an idea of what you can create.

<LoopedVideo src='../assets/how-to/deephaven-ui/dashboard.mp4' />

## Features

- **Python-native**: You build user interfaces from components defined entirely in Python.
- **Table-aware**: You can use Deephaven tables, including tables that update in real time, as a data source for components.
- **Reactive**: You change a component's state, and it re-renders. Tables and plots inside it update on their own as their data changes.
- **Declarative**: You describe what the user interface (UI) shows for the current data and user input, not the steps to update it.
- **Composable**: You combine and reuse components to build complex interfaces.
- **Extensive**: You choose from a wide range of components, from simple text fields to complex tables and plots.

## React concepts

[`deephaven.ui`](/core/ui/docs/) uses a React-like approach to building dashboards. If you're new to React, these concepts help you follow the examples:

- **Components**: React applications are made of components. A component is a part of the UI with its own logic and visual appearance. A component can be as small as a button or as large as an entire page, and you can nest components inside other components.
- **Hooks**: [Hooks](/core/ui/docs/hooks/overview/) are functions that let components use state and other [`deephaven.ui`](/core/ui/docs/) features. Their names begin with `use_`. You can use built-in hooks such as [`use_state`](/core/ui/docs/hooks/use_state/), or write your own hooks to reuse logic across components.
- **State**: State is the data a component remembers between calls of its function. Hooks such as [`use_state`](/core/ui/docs/hooks/use_state/) create it. Whenever a component's state changes, the component re-renders. Its function runs again, and the UI updates to show the new output.
- **Hook rules**: Hooks have rules about where you call them:
  - Call hooks only at the top level of a component or of another hook, before any early `return`.
  - Never call a hook inside a condition, a loop, or a nested function. If you need one there, move that code into a new component and call the hook at its top level.

See the [React documentation](https://react.dev/learn) for a comprehensive overview of these concepts.

## Getting started

The `ghcr.io/deephaven/server` [Docker image](../getting-started/docker-install.md#choose-a-deployment) includes [`deephaven.ui`](/core/ui/docs/). To start a server with [Docker](../getting-started/docker-install.md), run:

```bash skip-test
docker run --rm --name deephaven-ui -p 10000:10000 --pull=always ghcr.io/deephaven/server:latest
```

To open the [IDE](./user-interface/navigating-the-ui.md), find the link in the Docker logs:

![Docker log output showing the URL to open the Deephaven IDE](../assets/how-to/deephaven-ui/docker.png)

Other installations need the `deephaven-plugin-ui` package. [Install and use plugins](./install-use-plugins.md#install-with-pip) covers both of these cases:

- **pip**: If you run Deephaven from [pip](../getting-started/pip-install.md), install `deephaven-plugin-ui` in the same Python environment as Deephaven, then restart the server.
- **Other Docker images**: If you use a [Docker image](../getting-started/docker-install.md#choose-a-deployment) that doesn't include it, such as `server-nltk` or `server-all-ai`, [build a custom image](./install-and-use-python-packages.md#permanently-add-packages-to-a-custom-docker-image) that adds the package.

## Create a component with state

Now you can start creating components! In the [IDE](./user-interface/navigating-the-ui.md), paste the following code into the console and run it. The example:

- Defines a counter component with the [`@ui.component`](/core/ui/docs/describing/your_first_component/) decorator.
- Keeps the count with [`use_state`](/core/ui/docs/hooks/use_state/), so the component re-renders each time you click the button.
- Creates two instances of the component, `c1` and `c2`. Each one keeps its own count and opens in its own panel.

```python order=null
from deephaven import ui


@ui.component
def ui_counter():
    count, set_count = ui.use_state(0)
    return ui.button(f"Pressed {count} times", on_press=lambda: set_count(count + 1))


c1 = ui_counter()
c2 = ui_counter()
```

![Two counter buttons, each showing its own click count](../assets/tutorials/crash-course/dh-ui/button_state.gif)

## Display a ticking table in a dashboard

Components can also display Deephaven tables, including ticking tables, which update in real time as new rows arrive. The following example:

- Uses [`time_table`](../reference/table-operations/create/timeTable.md) to create a table that adds a new row every second, with a column `X` that numbers the rows.
- Places that table in a [`ui.panel`](/core/ui/docs/components/panel/).
- Uses [`ui.row`](/core/ui/docs/components/dashboard/) to arrange the table panel beside a counter panel in a [`ui.dashboard`](/core/ui/docs/components/dashboard/).

The table panel updates as each new row arrives:

```python ticking-table order=null
from deephaven import time_table
from deephaven import ui


@ui.component
def ui_counter():
    count, set_count = ui.use_state(0)
    return ui.button(f"Pressed {count} times", on_press=lambda: set_count(count + 1))


ticking = time_table("PT1s").update("X = ii")

live_dash = ui.dashboard(
    ui.row(
        ui.panel(ticking, title="Ticking table"),
        ui.panel(ui_counter(), title="Counter"),
    )
)
```

![A dashboard with a ticking table panel beside a counter panel](../assets/how-to/deephaven-ui/live_dash.gif)

See [`ui.dashboard`](/core/ui/docs/components/dashboard/) for rows, columns, and other layout options.

## Next steps

The [`deephaven.ui` chapter of the Crash Course](../getting-started/crash-course/deephaven-ui.md) walks through more components, state, [table-backed components](../getting-started/crash-course/deephaven-ui.md#table-backed-components), and [dashboards](../getting-started/crash-course/deephaven-ui.md#dashboards) step by step. For comprehensive examples and the full component reference, see the [`deephaven.ui` documentation](/core/ui/docs/).

## Related documentation

- [How Deephaven works: A mental model and patterns of use](../conceptual/deephaven-mental-model.md)
- [Crash Course: `deephaven.ui`](../getting-started/crash-course/deephaven-ui.md)
- [Install and run with Docker](../getting-started/docker-install.md)
- [Install and run Deephaven with pip](../getting-started/pip-install.md)
- [Install and use Python packages](./install-and-use-python-packages.md)
- [Install and use plugins](./install-use-plugins.md)
- [Navigate the GUI](./user-interface/navigating-the-ui.md)
- [`time_table`](../reference/table-operations/create/timeTable.md)
- [`deephaven.ui` documentation](/core/ui/docs/)
- [Hooks overview](/core/ui/docs/hooks/overview/)
- [`ui.button`](/core/ui/docs/components/button/)
- [`ui.component`](/core/ui/docs/describing/your_first_component/)
- [`ui.dashboard`](/core/ui/docs/components/dashboard/)
- [`ui.panel`](/core/ui/docs/components/panel/)
- [`use_state`](/core/ui/docs/hooks/use_state/)
- [`time_table` Pydoc](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.time_table)
