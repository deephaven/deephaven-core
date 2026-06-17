---
title: "Create interactive, real-time browser dashboards with deephaven.ui"
sidebar_label: "deephaven.ui"
---

<div className="comment-title">

A framework for building real-time data-focused apps in Python

</div>

`deephaven.ui` is a plugin for Deephaven that combines a reactive UI framework and a library of pre-built real-time data-focused components for creating data apps. It uses a React-like approach to building components and rendering them in the UI, allowing for the creation of reactive components that can be reused and composed together, as well as react to user input from the UI.

<Pullquote> Create dashboards entirely in Python -- no front-end engineering Javascript or CSS required. </Pullquote>

Many other dashboarding frameworks in Python can be made to work with real-time data by periodically refreshing data. However, components and plots in Deephaven are driven by streaming data _continuously_.

## Features

- **Components**: Create user interfaces from components defined entirely with Python.
- **Live dataframe aware**: Components are live dataframe aware and can use Deephaven tables as a data source.
- **Reactive**: UI components automatically update when the underlying Python data changes.
- **Declarative**: Describe the UI as a function of the data and let the framework handle the rest.
- **Composable**: Combine and reuse components to build complex interfaces.
- **Wide range of components**: From simple text fields to complex tables and plots, the library has a wide range of components to build your app.

## React concepts

`deephaven.ui` uses a React-like approach to building dashboards. The following concepts might be useful for users unfamiliar with React when following along with examples:

- **Components**: React applications are built using components. A component is a part of the user interface (UI) with its own logic and visual appearance. They can range in size from a small button to an entire page. Components can be nested within other components.
- **Hooks**: Functions that begin with `use_` are called **hooks**. They allow you to write reusable pieces of code that can be used within different components. Hooks have stricter usage rules than other functions; they can only be called at the top level of your components or within other hooks. One such built-in hook is `use_state`. If you need to use `use_state` within a condition or a loop, extract a new component first. You can also create your own hooks.
- **State**: State is a built-in object that stores and manages data that can change over time within your component. Whenever a component's state changes, the component re-renders.

See the [React documentation](https://react.dev/learn) for a comprehensive overview of these concepts.

## Getting Started

You can run the example Docker container with the following command:

```skip-test
docker run --rm --name deephaven-ui -p 10000:10000 --pull=always ghcr.io/deephaven/server:latest
```

You'll need to find the link to open the UI in the Docker logs:
![docker](../assets/how-to/deephaven-ui/docker.png)

Now, you can start using and creating components!

Here is a relatively complex [`deephaven.ui`](https://github.com/deephaven/deephaven-plugins/blob/main/plugins/ui/docs/README.md) dashboard with a mock streaming dataset underneath it to give you an idea of what you can create.

<LoopedVideo src='../assets/how-to/deephaven-ui/dashboard.mp4' />

For comprehensive examples and more detailed documentation, see the [`deephaven.ui` README](https://github.com/deephaven/deephaven-plugins/blob/main/plugins/ui/docs/README.md).

## Related documentation

- [`deephaven.ui` README](https://github.com/deephaven/deephaven-plugins/blob/main/plugins/ui/docs/README.md)
- [Install and use plugins](./install-use-plugins.md)
- [Navigate the GUI](./user-interface/navigating-the-ui.md)
