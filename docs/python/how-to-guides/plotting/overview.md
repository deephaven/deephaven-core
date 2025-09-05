---
title: How to plot real-time data
sidebar-label: Plotting Overview
---

This section explores various methods for visualizing data in Deephaven, catering to real-time and static scenarios. Deephaven comes with support for five libraries for plotting. [Deephaven Express](/core/plotly/docs/), built on top of Plotly Express, offers a seamless solution for effortlessly plotting real-time data directly from Deephaven tables. We support three additional methods for those working with static or snapshotted data: Matplotlib, Seaborn, and Plotly. Each method provides a unique set of features and capabilities, allowing users to choose the most suitable approach for their specific visualization needs.

Our how-to guides delve into the supported methods, showcasing their advantages and use cases. Deephaven has you covered whether you require dynamic visualizations with real-time data streaming or static visualizations for historical analysis. Each method offers flexibility, interactivity, and high-quality visuals, ensuring you can effectively explore and communicate insights from your data.

## Plotting Library Support

| Library                                     | Real-Time     | Static        | Ease of Use   | Deephaven Tables | Pip Install                                                  |
| ------------------------------------------- | ------------- | ------------- | ------------- | ---------------- | ------------------------------------------------------------ |
| **[Deephaven Express](/core/plotly/docs/)** | **Excellent** | **Excellent** | **Excellent** | **Native**       | `pip install deephaven-plugin-plotly-express`\*              |
| **[Seaborn](./matplot-seaborn.md)**         | Snapshots     | Good          | Excellent     | via Pandas       | `pip install deephaven-plugin-matplotlib matplotlib seaborn` |
| **[Matplotlib](./matplot-seaborn.md)**      | Snapshots     | Good          | Okay          | via Pandas       | `pip install deephaven-plugin-matplotlib matplotlib`         |
| **[Plotly](https://plotly.com/python/)**    | Snapshots     | Good          | Good          | via Pandas       | Installed with Deephaven Express                             |
| **[Legacy Figure API](./api-plotting.md)**  | **Excellent** | Good          | Good          | **Native**       | **Built in**                                                 |

<small>

\*Deephaven Express is built into the Deephaven `server` docker image. Deephaven Express is the recommended choice for most plotting tasks. See the [examples section](/core/plotly/docs#plot-types) for a showcase of its versatile usage and advantages.

</small>

### Snapshot limitations

One limitation of plotting with snapshots is the need to take a full snapshot of the data at regular intervals. This approach requires storing the entire dataset in memory, leading to increased memory usage, especially for large datasets. Additionally, since the snapshots are taken periodically, the visualizations may not always reflect real-time changes, resulting in slower updates and potentially missing dynamic patterns in the data.

In contrast, Deephaven Express is specifically designed to leverage Deephaven table updates natively. By directly connecting to real-time Deephaven tables from the client, Deephaven Express can dynamically visualize data as it updates in real time, eliminating the need for periodic snapshots and ensuring more efficient memory usage and faster updates. This allows users to gain immediate insights into dynamic data changes and maintain interactive and responsive visualizations without the overhead of taking snapshots.
