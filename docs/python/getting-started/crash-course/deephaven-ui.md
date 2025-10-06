---
title: deephaven.ui
sidebar_label: deephaven.ui
---

Now that you're more familiar with the table API and visualizing data, it's time to consider how to present your work. The [`deephaven.ui`](/core/ui/docs/) package provides a rich set of tools for building interactive user experiences that show off the tables, plots, and other widgets your queries created.

## What is deephaven.ui?

[`deephaven.ui`](/core/ui/docs/) is a [react](https://react.dev/)-like framework for building user experiences in Python. No front-end development, JavaScript, or CSS experience is required to hit the ground running. Key features include:

- **Components**: Create user interfaces from components defined entirely with Python.
- **Live dataframe integration**: Components are live dataframe aware and can use Deephaven tables as a data source.
- **Reactive UI updates**: UI components automatically update when the underlying data changes.
- **Declarative UI framework**: Describe the UI as a function of the data and let the framework handle the rest.
- **Composability**: Combine and re-use components to build complex interfaces.
- **Wide range of components**: From simple text fields to complex tables and plots, the library has a wide range of components to build your app.

## Components

At the core of `deephaven.ui` are components, which are reusable building blocks for your user interface. Take, for instance, a simple heading:

```python test-set=1 order=null
from deephaven import ui

hello_world = ui.heading("Hello World")
```

![Hello world](../../assets/tutorials/crash-course/dh-ui/hello_world.png)

This component isn't very exciting. What about a component that handles events, such as a button click?

```python test-set=1 order=null
button = ui.button(
    "Click me!", on_press=lambda e: print(f"The button was clicked: {e}")
)
```

![Button](../../assets/tutorials/crash-course/dh-ui/click_me.png)

![Button click](../../assets/tutorials/crash-course/dh-ui/button_clicked.png)

The example above is more useful than the first, but a real user experience doesn't print raw data to a console. With `deephaven.ui`, you can decorate functions with `@ui.component` to define components with custom behavior. The following example is a simple component that builds off the button example.

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

Simple enough. But once again, the example above isn't all that useful for any real-world scenarios. `deephaven.ui` also offers the [`use_state`](/core/ui/docs/hooks/use_state/) hook, which allows you to manage state within components. For instance, you can track how many times a button has been clicked. Each component has its own state, as shown by clicking on `c1` and `c2` separately:

```python test-set=1 order=null
@ui.component
def ui_counter():
    count, set_count = ui.use_state(0)
    return ui.button(f"Pressed {count} times", on_press=lambda: set_count(count + 1))


c1 = ui_counter()
c2 = ui_counter()
```

![Buttons with state](../../assets/tutorials/crash-course/dh-ui/button_state.gif)

Buttons and text boxes are only two of the many components available in `deephaven.ui`. The following code shows off a few more components including a [picker](/core/ui/docs/components/picker/), [slider](/core/ui/docs/components/slider/), and [combo box](/core/ui/docs/components/combo_box/):

```python order=null
from deephaven import ui


@ui.component
def color_picker():
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


pick_a_color = color_picker()

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

![Sliders](../../assets/tutorials/crash-course/dh-ui/sliders.gif)

![Combo box](../../assets/tutorials/crash-course/dh-ui/combo_box.png)

For a full list of available components, see the [Components overview](/core/ui/docs/components/overview/).

### Table-backed components

Things get a lot more interesting when you source components from Deephaven tables. Components tick in lock-step with the underlying data if it's streaming.

Components can be passed tables as input to display data. Take the picker example above. What if you want to choose from the available values in a column of a table?

```python order=null
from deephaven import empty_table
from deephaven import read_csv
from deephaven import ui

iris = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/refs/heads/main/Iris/csv/iris.csv"
)


@ui.component
def species_panel():
    return ui.picker(iris.view("Class").select_distinct())


species_picker_panel = species_panel()
```

![Picker sourced from table](../../assets/tutorials/crash-course/dh-ui/picker_from_iris.png)

You can take this a step further and utilize the selected value to produce a filtered table and generate a [Deephaven Plotly Express](/core/plotly/docs/) [heatmap](/core/plotly/docs/density_heatmap/) showing the joint density of sepal length and sepal width for the selected species.

What if you want to utilize state from a component? The following example utilizes state from the component to show the joint density of sepal length and sepal width for the selected species:

```python order=null
from deephaven.plot import express as dx
from deephaven import empty_table
from deephaven import read_csv
from deephaven import ui


@ui.component
def species_panel():
    species, set_species = ui.use_state()
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


species_picker_panel = species_panel()
```

![Species heatmap from picker](../../assets/tutorials/crash-course/dh-ui/species_picker_panel.gif)

## Dashboards

Dashboards are collections of components arranged in a grid layout. These grids are arranged into rows and columns, allowing you to create simple and complex layouts depending on your application's needs. They are a great way to present data and visualizations in a single view.

The following example creates a simple 2x2 dashboards with components that don't have much use. This shows how the grid layout of a dashboard is created programmatically.

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

Dashboard layouts can be customized to fit your needs. A simple 2x2 grid won't suffice for many applications. What if you need more rows, where each row has a different layout? The following example shows a more complex dashboard layout:

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

Obviously, dashboards are much more interesting when they contain components with real data, plots, widgets, and information.

Dashboards combine multiple components into a single user experience. Dashboards are built with rows and columns of previously created components. Below is a full example creating a dashboard that displays a picker for Iris species, a heatmap, and some other custom components:

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

- The data powering this dashboard is simulated Iris data.
- Charts are from Deephaven Plotly Express.
- Other components are from **deephaven.ui**.

See the full documentation [here](/core/ui/docs/).
"""
)

iris_dashboard = ui.dashboard(
    ui.column(
        ui.row(
            ui.stack(ui.panel(about_markdown, title="About this dashboard"), width=30),
            ui.panel(sepal_flex_column, title="Sepal flex column"),
        ),
        ui.row(
            ui.panel(sepal_length_hist, "Sepal Length Histogram"),
            ui.panel(sepal_width_hist, "Sepal Width Histogram"),
        ),
    )
)
```

![Iris dashboard](../../assets/tutorials/crash-course/dh-ui/iris_dashboard.gif)

This crash course page only scratches the surface of what you can do with `deephaven.ui`. For the full `deephaven.ui` documentation with detailed information on everything possible, see [deephaven.ui](/core/ui/docs/).
