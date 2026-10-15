---
title: deephaven.ui
---

Now that you're more familiar with the table API and visualizing data, it's time to consider how to present your work. The [`deephaven.ui`](/core/ui/docs/) package provides a rich set of tools for building interactive user experiences that show off the tables, plots, and other widgets your queries created.

## What is `deephaven.ui`?

[`deephaven.ui`](/core/ui/docs/) is a [React](https://react.dev/)-like framework for building user experiences in Python. You don't need any front-end development, JavaScript, or CSS experience to get started. Key features include:

- **Components**: You build user interfaces from components defined entirely in Python.
- **Live table integration**: Components use Deephaven tables as a data source, including [ticking tables](../../conceptual/table-types.md#standard-streaming-tables) that update as new data arrives.
- **Reactive UI updates**: Components update automatically when a user interacts with them or when their source data changes.
- **Declarative UI framework**: You describe the UI as a function of the data, and the framework renders it.
- **Composability**: You combine and reuse components to build complex interfaces.
- **Wide range of components**: The library includes everything from simple text fields to complex tables and plots.

`deephaven.ui` exposes many [React Spectrum](https://react-spectrum.adobe.com/react-spectrum/index.html) components to Python. React Spectrum is Adobe's implementation of the [Spectrum design system](https://spectrum.adobe.com/). Component names and arguments generally match React Spectrum's, except that Python uses `snake_case` instead of `camelCase`. For example, `onPress` becomes `on_press`, and `defaultValue` becomes `default_value`.

> [!NOTE]
> `deephaven.ui` and [Deephaven Express](/core/plotly/docs/) are plugins, not part of the core `deephaven` Python package. The `ghcr.io/deephaven/server` Docker image used in this crash course includes both. If you installed Deephaven with pip, install them with `pip install deephaven-plugin-ui deephaven-plugin-plotly-express`. See [Install and use plugins](../../how-to-guides/install-use-plugins.md) for details.

## Components

At the core of [`deephaven.ui`](/core/ui/docs/) are components, which are reusable building blocks for your user interface. Take, for instance, a simple [heading](/core/ui/docs/components/heading/):

```python test-set=1 order=null
from deephaven import ui

hello_world = ui.heading("Hello World")
```

![Hello world](../../assets/tutorials/crash-course/dh-ui/hello_world.png)

This component isn't very exciting. What about a component that handles events, such as a [button](/core/ui/docs/components/button/) click?

```python test-set=1 order=null
button = ui.button(
    "Click me!", on_press=lambda e: print(f"The button was clicked: {e}")
)
```

![Button](../../assets/tutorials/crash-course/dh-ui/click_me.png)

![Button click](../../assets/tutorials/crash-course/dh-ui/button_clicked.png)

In this example, the `on_press` handler takes the press event as an argument. If you don't need the event, the handler can take no arguments, as in the examples that follow.

### Custom components

With `deephaven.ui`, you can decorate functions with [`@ui.component`](/core/ui/docs/describing/your_first_component/) to define your own components that combine several built-in components and have custom behavior. The following example is a simple component that builds off the button example.

```python test-set=1 order=null
@ui.component
def ui_foo_bar():
    return [
        ui.heading("Click button below"),
        ui.button("Click Me!", on_press=lambda: print("Button was clicked!")),
    ]


foo_bar = ui_foo_bar()
```

![Better button](../../assets/tutorials/crash-course/dh-ui/click_me_2.png)

![Better button click](../../assets/tutorials/crash-course/dh-ui/button_clicked_2.png)

### State and hooks

To make components more useful, `deephaven.ui` offers [hooks](/core/ui/docs/hooks/overview/). A hook is a function whose name starts with `use_`, and you call it inside a component.

The [`use_state`](/core/ui/docs/hooks/use_state/) hook manages state, a value the component keeps from one call of the component function to the next:

- [`use_state`](/core/ui/docs/hooks/use_state/) returns the current value and a function that sets a new one.
- When the state changes, `deephaven.ui` re-renders the component. It calls the component function again, so the UI shows the new value.

For instance, you can count how many times someone clicks a button. Each instance of a component has its own state. Click `c1` and `c2` separately to see that each one keeps its own count:

```python test-set=1 order=null
@ui.component
def ui_counter():
    count, set_count = ui.use_state(0)
    return ui.button(f"Pressed {count} times", on_press=lambda: set_count(count + 1))


c1 = ui_counter()
c2 = ui_counter()
```

![Buttons with state](../../assets/tutorials/crash-course/dh-ui/button_state.gif)

### More components

Headings and buttons are only two of the many components available in `deephaven.ui`, and you can combine any of them with custom components and state. The following code shows off a few more components, including a [picker](/core/ui/docs/components/picker/), a [combo box](/core/ui/docs/components/combo_box/), and a [slider](/core/ui/docs/components/slider/). It also uses [`ui.flex`](/core/ui/docs/components/flex/), which lays out its children in a row, or in a column with `direction="column"`:

```python order=null
from deephaven import ui


@ui.component
def ui_color_picker():
    return ui.picker("Red", "Yellow", "Blue", "Green")


@ui.component
def ui_combo_box_basic():
    option, set_option = ui.use_state("")

    return ui.combo_box(
        ui.item("red panda"),
        ui.item("cat"),
        ui.item("dog"),
        ui.item("aardvark"),
        ui.item("kangaroo"),
        ui.item("snake"),
        ui.item("ant"),
        label="Favorite Animal",
        selected_key=option,
        on_change=set_option,
    )


pick_a_color = ui_color_picker()

my_combo_box_basic = ui_combo_box_basic()

my_slider_label_example = ui.flex(
    ui.slider(label="Cookies to buy", default_value=25),
    ui.slider(label="Donuts to buy", label_position="side", default_value=25),
    ui.slider(label="Cakes to buy", show_value_label=False, default_value=25),
    direction="column",
    gap="size-500",
)
```

![Picker](../../assets/tutorials/crash-course/dh-ui/picker.png)

![Combo box](../../assets/tutorials/crash-course/dh-ui/combo_box.png)

![Sliders](../../assets/tutorials/crash-course/dh-ui/sliders.gif)

For a full list of available components, see the [Components overview](/core/ui/docs/components/overview/).

### Table-backed components

Things get a lot more interesting when you use Deephaven tables as the data source for components. When a source table is [ticking](../../conceptual/table-types.md#standard-streaming-tables), components built from it update as it ticks. The examples in this section use a static CSV file to keep things simple, and the [Dashboards](#dashboards) section shows components fed by ticking data.

You can pass a table to a component to display its data. Take the picker example above. What if you want to choose from the available values in a column of a table?

```python order=null
from deephaven import read_csv
from deephaven import ui

iris = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/refs/heads/main/Iris/csv/iris.csv"
)


@ui.component
def species_panel():
    return ui.picker(iris.select_distinct("Class"))


species_picker_panel = species_panel()
```

![Picker sourced from table](../../assets/tutorials/crash-course/dh-ui/picker_from_iris.png)

You can take this a step further and use the selected value to [filter](../../reference/table-operations/filter/where.md) a table. The following example:

- Stores the selected species in component state, starting with `Iris-setosa`.
- [Filters](../../reference/table-operations/filter/where.md) the table to that species.
- Plots a [density heatmap](/core/plotly/docs/density_heatmap/) from [Deephaven Express](/core/plotly/docs/), which shows how many observations fall in each sepal length and sepal width bin.
- Returns the picker and heatmap together in a [`ui.panel`](/core/ui/docs/components/panel/), a titled container that the [Dashboards](#dashboards) section builds on.

The example also uses the [`use_memo`](/core/ui/docs/hooks/use_memo/) hook, which caches the result of a function and calls it again only when one of the listed dependencies changes. Because the component re-renders every time its state changes, the example uses `use_memo` three times:

- To read the CSV file once, not on every re-render.
- To compute the [distinct](../../reference/table-operations/select/select-distinct.md) species once.
- To [filter](../../reference/table-operations/filter/where.md) the table again only when the selected species changes.

```python order=null
from deephaven.plot import express as dx
from deephaven import read_csv
from deephaven import ui


@ui.component
def species_explorer():
    species, set_species = ui.use_state("Iris-setosa")
    iris = ui.use_memo(
        lambda: read_csv(
            "https://media.githubusercontent.com/media/deephaven/examples/refs/heads/main/Iris/csv/iris.csv"
        ),
        [],
    )
    species_table = ui.use_memo(lambda: iris.select_distinct("Class"), [iris])
    species_picker = ui.picker(
        species_table,
        on_change=set_species,
        selected_key=species,
        label="Current Species",
    )

    filtered_table = ui.use_memo(
        lambda: iris.where(f"Class = `{species}`"), [iris, species]
    )

    heatmap = dx.density_heatmap(filtered_table, x="SepalLengthCM", y="SepalWidthCM")

    return ui.panel(
        ui.flex(species_picker, heatmap, direction="column"),
        title="Investigate Species",
    )


species_explorer_panel = species_explorer()
```

![Species heatmap from picker](../../assets/tutorials/crash-course/dh-ui/species_picker_panel.gif)

## Dashboards

A [dashboard](/core/ui/docs/components/dashboard/) arranges panels into a layout, so you can present data and visualizations in a single view. You build the layout from these elements:

- [`ui.panel`](/core/ui/docs/components/panel/) is a titled container for a component, like the panel the species explorer above returns.
- `ui.row` arranges panels side by side, and `ui.column` arranges them top to bottom. Rows and columns nest inside each other.
- `ui.stack` groups panels as tabs.

The following example creates a 2x2 dashboard of placeholder panels to show how you build a dashboard's grid layout in code.

```python order=null
from deephaven import ui

my_dash = ui.dashboard(
    ui.row(
        ui.column(ui.panel("A"), ui.panel("C")),
        ui.column(ui.panel("B"), ui.panel("D")),
    )
)
```

![Basic dashboard](../../assets/tutorials/crash-course/dh-ui/dashboard_basic.png)

A 2x2 grid doesn't suit many applications, but you can customize the layout to fit your needs. What if you need a header and footer around a row of panels of different widths? You size layout elements with percentages relative to their siblings:

- A row takes an optional `height`.
- A column takes an optional `width`.
- A stack takes either one.
- A panel doesn't take a size, so to size a single panel, wrap it in a `ui.stack`.

The following example puts a header and footer above and below a row of three panels, and makes the middle one wider:

```python order=null
from deephaven import ui

dash_custom_layout = ui.dashboard(
    ui.column(
        ui.panel("Header", title="Header"),
        ui.row(
            ui.panel("Left Sidebar", title="Left Sidebar"),
            ui.stack(ui.panel("Main Content", title="Main Content"), width=70),
            ui.panel("Right Sidebar", title="Right Sidebar"),
        ),
        ui.panel("Footer", title="Footer"),
    )
)
```

![Custom dashboard layout](../../assets/tutorials/crash-course/dh-ui/dashboard_level2.png)

Dashboards become much more useful when their panels hold real data. The next example uses `dx.data.iris`, a [ticking](../../conceptual/table-types.md#standard-streaming-tables) version of the Iris dataset that ships with [Deephaven Express](/core/plotly/docs/). It adds simulated rows every second, so the table and plots in the dashboard update as new rows arrive. Its column names differ from those in the CSV file used above. For example, it has `SepalLength` instead of `SepalLengthCM`.

The dashboard holds a table, a [scatter plot](/core/plotly/docs/scatter/), two [histograms](/core/plotly/docs/histogram/), and a [Markdown](/core/ui/docs/components/markdown/) panel describing the dataset. The example wraps the table in [`ui.table`](/core/ui/docs/components/table/), which adds display options such as row order and which columns appear first or are hidden:

<!-- TODO: Unskip this test once the snapshots bug is fixed (related to dh-express) -->

```python skip-test
from deephaven.plot import express as dx
from deephaven import ui

iris = dx.data.iris()

ui_iris = ui.table(
    iris,
    reverse=True,
    front_columns=["Timestamp", "Species"],
    hidden_columns=["PetalLength", "PetalWidth", "SpeciesID"],
    density="compact",
)

scatter_by_species = dx.scatter(iris, x="SepalLength", y="SepalWidth", by="Species")

sepal_text = ui.text("SepalLength vs. SepalWidth By Species Panel")

sepal_flex = ui.flex(ui_iris, scatter_by_species)

sepal_flex_column = ui.flex(sepal_text, sepal_flex, direction="column")

sepal_length_hist = dx.histogram(iris, x="SepalLength", by="Species")
sepal_width_hist = dx.histogram(iris, x="SepalWidth", by="Species")

about_markdown = ui.markdown(
    r"""
### Iris Dashboard

Explore the Iris dataset with **deephaven.ui**

- The data powering this dashboard is the Iris dataset, with simulated rows added every second.
- Charts are from Deephaven Express.
- Other components are from **deephaven.ui**.

See the full [deephaven.ui documentation](https://deephaven.io/core/ui/docs/).
"""
)

iris_dashboard = ui.dashboard(
    ui.column(
        ui.row(
            ui.stack(ui.panel(about_markdown, title="About this dashboard"), width=30),
            ui.panel(sepal_flex_column, title="Sepal flex column"),
        ),
        ui.row(
            ui.panel(sepal_length_hist, title="Sepal Length Histogram"),
            ui.panel(sepal_width_hist, title="Sepal Width Histogram"),
        ),
    )
)
```

![Iris dashboard](../../assets/tutorials/crash-course/dh-ui/iris_dashboard.gif)

This crash course page only scratches the surface of what you can do with `deephaven.ui`. For more components, hooks, and examples, see the [`deephaven.ui` documentation](/core/ui/docs/).
