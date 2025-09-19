---
title: Create custom themes
sidebar_label: Custom themes
---

This guide will show you how to create a custom theme for the Deephaven UI. Themes work by providing a set of [CSS variables](https://developer.mozilla.org/en-US/docs/Web/CSS/Using_CSS_custom_properties) that override either the default dark or light theme. This document creates a custom theme that emulates the look of the [Financial Times](https://www.ft.com/) website.

![An example theme that emulates the look of the Financial Times website](../assets/how-to/example-theme.png)

## Install the plugin

In order to design and preview your theme, you will need a running Deephaven server, and to clone and install the [Deephaven plugins](https://github.com/deephaven/deephaven-plugins/tree/main) and [Deephaven web-client-ui](https://github.com/deephaven/web-client-ui) repositories.

1. A Deephaven server.

   - Start a deephaven-core server on port 10000. You can follow the instructions here in the [Quickstart](../tutorials/quickstart.md).

2. A plugin server. This server will proxy your plugins, which allows you to develop your theme without needing to reinstall the plugin every time you make a change.

   - Clone the [deephaven-plugins project](https://github.com/deephaven/deephaven-plugins/tree/main).
   - Open a separate terminal from where you are running deephaven-core. From the deephaven-plugins project repository root directory, run:

   ```
   npm install
   npm start
   ```

   - Once the plugin is installed:
     - Copy the `example-theme` directory to a new directory and rename it to your plugin name.
     - Rename the `example-theme` references in the `./<your-theme>/src/js/package.json` and `./<your-theme>/src/js/src/index.ts` to your plugin name in your theme folder.
     - Add your plugin's name to the `manifest.json` file in the repository root directory to enable your plugin in development previews.
     - You should also update the `name` key in the `./<your-theme>/src/js/src/index.ts` file with a unique name that will appear in the theme selector in the Deephaven UI.

3. A development version of the web-client-ui server.
   - Clone the [deephaven/web-client-ui](https://github.com/deephaven/web-client-ui) repository.
   - Add the following line to the file `./packages/code-studio/.env.development.local` in web-client-ui (note: you will need to create this file if it does not already exist):
     ```
     VITE_JS_PLUGINS_DEV_PORT=4100
     ```
   - Open a new terminal, and from the web-client-ui project repository root directory, run:
     ```
     npm install
     npm run start
     ```

Visit http://localhost:4000 to preview your theme in the Deephaven UI. You can change the theme in the **Settings** menu:

![The **pick a color scheme** option in the **Settings** menu](../assets/how-to/themes/pick_a_color.png)

Alternatively, visit http://localhost:4000/ide/styleguide for a preview of all the components in the web-client-ui library. You can change the theme by clicking on the theme selector in the top right corner of the style guide:

![The style guide displays all the components of the theme: headings, menus, etc](../assets/how-to/themes/style_guide_selector.png)

> [!NOTE]
> If you change your styles, you will need to refresh the page to see the changes.

## Creating your theme

The general steps for creating a theme are as follows:

1. [Inherit a base theme (either `dark` or `light`)](#inherit-a-base-theme).
2. [Create your color palette (gray, red, orange, etc)](#use-color-generators-and-define-your-palette)
3. [Override any semantic colors with colors from your palette as desired (accent, positive, negative, etc)](#use-your-palette-for-semantic-colors)
4. [Override component-specific colors with semantic colors or colors from your palette as desired (grid colors, plot, etc.)](#use-your-palette-for-component-specific-semantic-colors)

### Inherit a base theme

To create your theme, first decide whether your theme is light or dark. For example, if your desired background color is black or purple, inherit the `dark`. If it's white or yellow, inherit `light`. Inheriting from either `dark` or `light` significantly reduces the amount of work required to create a custom theme. You may choose to override just one variable or all of them.

In the `./<your-theme>/src/js/src/index.ts` file, set the `baseTheme` to either `dark` or `light`:

```typescript
export const plugin: ThemePlugin = {
  name: 'example-theme',
  type: 'ThemePlugin',
  themes: {
    ...
    baseTheme: 'light', // The base theme to extend, either 'light' or 'dark'
    ...
  },
};
```

Next, you'll want to override colors and other variables to create your custom theme in the `./<your-theme>/src/js/src/theme.css` file. This file already includes variables for the entire theme, and you only need to override the variables you want to change.

<details>
<summary>See the entire example-theme .css file.</summary>

```css
/* override the inherited base theme by scoping variables to :root */
:root {
  /*
  ============ PALETTE ===========
  GRAY 50, 75, 100, 200, 300, 400, 500, 600, 700, 800, 900

  RED, ORANGE, YELLOW, CHARTREUSE, CELERY, GREEN, SEAFOAM,
  CYAN, BLUE, INDIGO, PURPLE, FUSCIA AND MAGENTA
  100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200, 1300, 1400

  Use any srgb color space value (hex, rgb, hsl, etc) to define a color.

  See for reference: 
  github repo deephaven/web-client-ui
  /packages/components/src/theme/theme-light/theme-light-palette.css
  */

  /* Create a custom gray palette for background colors */
  --dh-color-gray-50: hsl(30, 100%, 99.22%);
  --dh-color-gray-75: hsl(30, 100%, 97.65%);
  --dh-color-gray-100: hsl(27.69, 100%, 94.9%);
  --dh-color-gray-200: hsl(29.19, 78.72%, 90.78%);
  --dh-color-gray-300: hsl(28.33, 58.06%, 87.84%);
  --dh-color-gray-400: hsl(28, 18.99%, 69.02%);
  --dh-color-gray-500: hsl(27.5, 10.34%, 54.51%);
  --dh-color-gray-600: hsl(26.67, 8.57%, 41.18%);
  --dh-color-gray-700: hsl(25, 9.38%, 25.1%);
  --dh-color-gray-800: hsl(34.29, 10.77%, 12.75%);
  --dh-color-gray-900: hsl(0, 0%, 0%);

  /* Create a custom seafoam palette */
  --dh-color-seafoam-100: hsl(185, 30%, 92.16%);
  --dh-color-seafoam-200: hsl(186, 31.25%, 87.45%);
  --dh-color-seafoam-300: hsl(186.67, 31.03%, 82.94%);
  --dh-color-seafoam-400: hsl(187.14, 32.81%, 74.9%);
  --dh-color-seafoam-500: hsl(186.43, 32.18%, 65.88%);
  --dh-color-seafoam-600: hsl(186.86, 31.82%, 56.86%);
  --dh-color-seafoam-700: hsl(186.43, 34.43%, 47.84%);
  --dh-color-seafoam-800: hsl(186.19, 49.74%, 38.24%);
  --dh-color-seafoam-900: hsl(185.17, 84.06%, 27.06%);
  --dh-color-seafoam-1000: hsl(185.61, 100%, 20.98%);
  --dh-color-seafoam-1100: hsl(186.21, 100%, 17.06%);
  --dh-color-seafoam-1200: hsl(187.06, 100%, 13.33%);
  --dh-color-seafoam-1300: hsl(187.2, 100%, 9.8%);
  --dh-color-seafoam-1400: hsl(188.33, 100%, 7.06%);

  /* Create a custom green palette */
  --dh-color-green-100: hsl(102.35, 39.53%, 91.57%);
  --dh-color-green-200: hsl(102, 41.67%, 85.88%);
  --dh-color-green-300: hsl(101.43, 41.18%, 80%);
  --dh-color-green-400: hsl(100.33, 40.94%, 70.78%);
  --dh-color-green-500: hsl(99.49, 39.7%, 60.98%);
  --dh-color-green-600: hsl(98.13, 38.4%, 50.98%);
  --dh-color-green-700: hsl(96.22, 53.11%, 40.98%);
  --dh-color-green-800: hsl(91.34, 90.54%, 29.02%);
  --dh-color-green-900: hsl(93.72, 100%, 23.73%);
  --dh-color-green-1000: hsl(96.6, 100%, 19.61%);
  --dh-color-green-1100: hsl(99.26, 100%, 15.88%);
  --dh-color-green-1200: hsl(101.9, 100%, 12.35%);
  --dh-color-green-1300: hsl(105.65, 100%, 9.02%);
  --dh-color-green-1400: hsl(109.09, 100%, 6.47%);

  /* Create a custom red palette */
  --dh-color-red-100: hsl(10, 100%, 95.29%);
  --dh-color-red-200: hsl(8.78, 100%, 91.96%);
  --dh-color-red-300: hsl(9.15, 100%, 88.43%);
  --dh-color-red-400: hsl(8.47, 100%, 83.33%);
  --dh-color-red-500: hsl(8.28, 100%, 77.25%);
  --dh-color-red-600: hsl(7.03, 96.03%, 70.39%);
  --dh-color-red-700: hsl(5.85, 87.23%, 63.14%);
  --dh-color-red-800: hsl(4.67, 77.59%, 54.51%);
  --dh-color-red-900: hsl(2.14, 86.73%, 44.31%);
  --dh-color-red-1000: hsl(0, 100%, 35.29%);
  --dh-color-red-1100: hsl(0, 100%, 28.63%);
  --dh-color-red-1200: hsl(0, 100%, 22.75%);
  --dh-color-red-1300: hsl(0, 100%, 17.25%);
  --dh-color-red-1400: hsl(0, 100%, 12.94%);

  /*
  ============ SEMANTIC VARIABLES ===========
  Assign things like accent colors, apply how background colors
  are mapped, positive/negative colors, etc. 
  
  See: 
  /packages/components/src/theme/theme-light/theme-light-semantic.css
  */

  /*
  Assign the accent colors to use seafoam. We could have assigned colors 
  directly to accent-XXX but by first setting our brand color to the 
  nearest palette color it allows it to also be exposed as a color option 
  in color pickers within the UI.
  */

  --dh-color-accent-100: var(--dh-color-seafoam-100);
  --dh-color-accent-200: var(--dh-color-seafoam-200);
  --dh-color-accent-300: var(--dh-color-seafoam-300);
  --dh-color-accent-400: var(--dh-color-seafoam-400);
  --dh-color-accent-500: var(--dh-color-seafoam-500);
  --dh-color-accent-600: var(--dh-color-seafoam-600);
  --dh-color-accent-700: var(--dh-color-seafoam-700);
  --dh-color-accent-800: var(--dh-color-seafoam-800);
  --dh-color-accent-900: var(--dh-color-seafoam-900);
  --dh-color-accent-1000: var(--dh-color-seafoam-1000);
  --dh-color-accent-1100: var(--dh-color-seafoam-1100);
  --dh-color-accent-1200: var(--dh-color-seafoam-1200);
  --dh-color-accent-1300: var(--dh-color-seafoam-1300);
  --dh-color-accent-1400: var(--dh-color-seafoam-1400);

  /*
  ============ COMPONENT SPECIFIC SEMANTIC VARIABLES ===========
  Override specific colors to components like grid, plots, code editor, etc.

  See: 
  /packages/components/src/theme/theme-light/theme-dark-components.css
  /packages/components/src/theme/theme-light/theme-dark-semantic-grid.css
  /packages/components/src/theme/theme-light/theme-dark-semantic-charts.css
  /packages/components/src/theme/theme-light/theme-dark-editor.css
  */

  --dh-color-grid-header-bg: var(--dh-color-gray-100);
  --dh-color-grid-row-0-bg: var(--dh-color-gray-200);
  --dh-color-grid-row-1-bg: var(--dh-color-gray-100);
  --dh-color-grid-date: var(--dh-color-black);

  /* Chart colorway is special in that it contains a space separated 
  list of colors to assign in order for each series in a chart. 
  Consider including at least 10 colors. */
  --dh-color-chart-colorway: var(--dh-color-accent-bg) var(
      --dh-color-visual-green
    )
    var(--dh-color-visual-yellow) var(--dh-color-visual-purple) var(
      --dh-color-visual-orange
    ) var(--dh-color-visual-red) var(--dh-color-visual-chartreuse) var(
      --dh-color-visual-fuchsia
    )
    var(--dh-color-visual-blue) var(--dh-color-visual-magenta) var(--dh-color-white);
}

/*
============ CUSTOM CLASS OVERRIDES ===========
Exampe of overriding specific class color variables for one-off customizations.
Use sparingly as these are not guaranteed to be stable across releases.
*/

/* target a specific class in the DOM and override the variables or properties */
.console-input-wrapper {
  --console-input-bg: var(--dh-color-gray-75);
  padding: 4px;
}
```

</details>

The web-client-ui repository includes a list of all the variables you can override in the [Default Dark](https://github.com/deephaven/web-client-ui/tree/main/packages/components/src/theme/theme-dark) or [Default Light](https://github.com/deephaven/web-client-ui/tree/main/packages/components/src/theme/theme-light), organized into files by usage. You can also inspect elements in the browser to discover the class or variable names you may need to override. For example, to create the example theme above, we only need to override a few colors, mostly from the palette files listed above.

> [!CAUTION]
> Use selector-based styling sparingly, as these are not guaranteed to be stable across releases. Most themes will start by overriding a few colors and then add additional variables as needed.

In the sections below, we'll show you how we modified the .css file to create the example theme. We'll start by overriding the background colors, accent colors, positive and negative colors, and then a few additional colors specific to grids and plots.

The web-client-ui theme provider converts your colors to HEX internally via JavaScript for display in some non-traditional dom-elements (canvas-based grids, monaco code editors, svg/webgl plots).

We also mix in an alpha channel to create transparent colors for you via [color-mix](https://developer.mozilla.org/en-US/docs/Web/CSS/color_value/color-mix) in srgb space.

### Use color generators and define your palette

The Deephaven design system is based on the [Adobe spectrum](https://spectrum.adobe.com/page/color-palette/) color system. It is set up to use a palette consisting of:

- 11 shades of a "gray" palette used for background colors
- 13 shades of each of the 12 colors in the "color" palette: Red, Orange, Yellow, Chartreuse, Celery, Green, Seafoam, Cyan, Blue, Indigo, Purple, Fuschia and Magenta

You can create color palettes with shades for each color using tools like Adobe's [Leonardo](https://leonardocolor.io/theme.html) color tool (recommended), or [Coolors](https://coolors.co/gradient-palette/fae7d5-24211d?number=11). You may already have an existing brand guide at your company that gives you full palettes. Leonardo is a great tool for creating color ramps, and has an "Export as CSS" feature to get the HEX values for each color in the ramp.

> [!NOTE]
> Colors must be in the [sRGB color space](https://en.wikipedia.org/wiki/SRGB). Here we use HSL values, but you can use any supported color format. These include #HEX, rgb(), rgba(), hsl(), hwb(), color(srgb) or a named color. Other color spaces such as LAB, OKLAB, P3, etc, are not supported.

The background colors are defined using a set of 11 colors, ranging from light to dark for light themes, and inverted from dark to light for dark themes. The 50, 75, and 100 colors are closer together, and 100-900 are a more equal distribution. The other colors are more evenly distributed across lightness. You can see this reflected in the image of our full color palette below.

Our example theme used a background color inspired by the Financial Times site to create a palette using Leonardo. The example theme uses the following palette for the "gray" background colors:

```css
// Background colors, labeled as gray but may be any suitable background color
--dh-color-gray-50: hsl(30, 100%, 99.22%);
--dh-color-gray-75: hsl(30, 100%, 97.65%);
--dh-color-gray-100: hsl(27.69, 100%, 94.9%);
--dh-color-gray-200: hsl(29.19, 78.72%, 90.78%);
--dh-color-gray-300: hsl(28.33, 58.06%, 87.84%);
--dh-color-gray-400: hsl(28, 18.99%, 69.02%);
--dh-color-gray-500: hsl(27.5, 10.34%, 54.51%);
--dh-color-gray-600: hsl(26.67, 8.57%, 41.18%);
--dh-color-gray-700: hsl(25, 9.38%, 25.1%);
--dh-color-gray-800: hsl(34.29, 10.77%, 12.75%);
--dh-color-gray-900: hsl(0, 0%, 0%);
```

We also override a "red" palette, a "green" palette and a "seafoam" palette based on the colors inspired by the Financial Times site. You will inherit any color from your chosen base theme you do not define.

Here is the full palette we used for our example theme:

![A color palette example for the custom theme](../assets/how-to/example-theme-palette.jpg)

### Use your palette for semantic colors

Depending on how much you want to customize, you may choose to stop after just setting the palette and inherit the rest of the theme from the default theme. Or you may choose to override additional variables to customize the theme further. For example, we also override the `accent` variables used for things like buttons from `blue` to `seafoam`. `positive` and `negative` already default to `red` and `green` -- updating `red` and `green` palettes will change these variables. They could also be changed independently if you prefer a different color associated with positive or negative values or actions. Refer to the files labeled as "semantic" in the default theme for exposed variables.

```css
--dh-color-accent-100: var(--dh-color-seafoam-100);
--dh-color-accent-200: var(--dh-color-seafoam-200);
--dh-color-accent-300: var(--dh-color-seafoam-300);
--dh-color-accent-400: var(--dh-color-seafoam-400);
--dh-color-accent-500: var(--dh-color-seafoam-500);
--dh-color-accent-600: var(--dh-color-seafoam-600);
--dh-color-accent-700: var(--dh-color-seafoam-700);
--dh-color-accent-800: var(--dh-color-seafoam-800);
--dh-color-accent-900: var(--dh-color-seafoam-900);
--dh-color-accent-1000: var(--dh-color-seafoam-1000);
--dh-color-accent-1100: var(--dh-color-seafoam-1100);
--dh-color-accent-1200: var(--dh-color-seafoam-1200);
--dh-color-accent-1300: var(--dh-color-seafoam-1300);
--dh-color-accent-1400: var(--dh-color-seafoam-1400);
```

### Use your palette for component-specific semantic colors

You may find that after overriding the palette and semantic colors, you want even further customization within specific components. In our example, we also override the default grid header color, row stripes, and date colors, as well as the plot colorway.

```css
--dh-color-grid-header-bg: var(--dh-color-gray-100);
--dh-color-grid-row-0-bg: var(--dh-color-gray-200);
--dh-color-grid-row-1-bg: var(--dh-color-gray-100);
```

## Additional customization

You may also override specific components using CSS selectors and setting your own variables and properties. This is not recommended as it may break in future updates to the web-client-ui library. If you find yourself needing to do this, please consider opening an [issue](https://github.com/deephaven/web-client-ui/issues) to request additional variables to be exposed in the default themes, or contact us in the [Deephaven Community Slack](/slack) #web-client-ui channel.
