---
title: Create custom themes
---

This guide shows you how to create a custom theme for the Deephaven web UI. Themes work by providing a set of [CSS variables](https://developer.mozilla.org/en-US/docs/Web/CSS/Using_CSS_custom_properties) that override either the default dark or light theme.

As a worked example, this guide walks through a theme that emulates the look of the [Financial Times](https://www.ft.com/) website. You start from its files and change them into your own theme. You package a theme as a [JS plugin](./create-js-plugins.md), which extends the Deephaven web UI, and install it on your server.

![An example theme that emulates the look of the Financial Times website](../assets/how-to/example-theme.png)

> [!TIP]
> If you want ready-made themes rather than your own, install the [theme pack](https://github.com/deephaven/deephaven-plugins/tree/main/plugins/theme-pack) plugin by packaging its `@deephaven/js-plugin-theme-pack` npm package, as shown in [Install the theme pack](./install-use-plugins.md#example-install-the-theme-pack). It includes the Financial Times theme this guide walks through, along with several others.

## Development setup

To design and preview your theme, you run three things side by side: a Deephaven server, a plugin development server that serves your theme plugin, and a development build of the Deephaven web UI. This setup is for development only. When your theme is complete, follow [Build and deploy your theme](#build-and-deploy-your-theme) to install it on your server.

You also need [Node.js](https://nodejs.org/) and npm at the versions each repository requires. Both repositories refuse to install on other versions. Each lists its Node.js version in its `.nvmrc` file, which [`nvm use`](https://github.com/nvm-sh/nvm) reads.

Set up the three pieces in order. In step 2, you create your theme plugin by copying the [theme pack](https://github.com/deephaven/deephaven-plugins/tree/main/plugins/theme-pack):

1. A Deephaven server.
   - Start a Deephaven server on port 10000. Follow the [Quickstart](../getting-started/quickstart.md).

2. Your theme plugin and a plugin development server. The development server builds your plugins whenever you save a change and serves them to the web UI, so you can iterate quickly without reinstalling the plugin after every change. The plugin development server listens on port 4100.
   - Clone the [deephaven-plugins project](https://github.com/deephaven/deephaven-plugins/tree/main).
   - Copy the `plugins/theme-pack` directory to a new directory under `plugins/`, named for your plugin, such as `plugins/my-theme`.
   - In `plugins/<your-theme>/src/js/package.json` and `plugins/<your-theme>/src/js/src/index.ts`, rename the `theme-pack` references to your plugin name.
   - In `package.json`, also change the package name from the `@deephaven` scope to your own, such as `@my-org/my-theme`.
   - In `plugins/<your-theme>/src/js/src/index.ts`, keep the `FT Theme` entry in the `themes` list. It loads `theme.css`, the Financial Times example theme this guide walks through. The copied file imports the stylesheet as `ftStyleContent`. The [minimal version in Inherit a base theme](#inherit-a-base-theme) calls it `styleContent`. Either name works.
   - Delete the other themes' entries and imports from `index.ts`, and delete their `.css` files.
   - Set the `name` key of the remaining entry to a unique name. This name appears in the theme selector in the Deephaven web UI.
   - Add an entry for your plugin to the `plugins/manifest.json` file to enable your plugin in development previews. Copy the `theme-pack` entry and change its `name` to your plugin name.
   - Open a terminal other than the one running your Deephaven server. From the root directory of your deephaven-plugins clone, run:

     ```bash
     npm install
     npm start
     ```

     `npm start` rebuilds every plugin in the repository when you save. To rebuild only your theme plugin, pass its package name with `--scope`, for example `npm start -- --scope @my-org/my-theme`.

3. A development build of the Deephaven web UI, built from the web-client-ui repository.
   - Clone the [deephaven/web-client-ui](https://github.com/deephaven/web-client-ui) repository.
   - In web-client-ui, create the file `./packages/code-studio/.env.development.local` if it doesn't already exist, and add the following line so the web UI loads plugins from the plugin development server on port 4100:

     ```text
     VITE_JS_PLUGINS_DEV_PORT=4100
     ```

   - Open a new terminal. From the root directory of your web-client-ui clone, run:

     ```bash
     npm install
     npm run start
     ```

The web UI development build runs at `http://localhost:4000/ide/` and connects to the Deephaven server on port 10000. Visit it and, if prompted, log in with the [pre-shared key](./authentication/auth-psk.md) printed in the server log. To preview your theme, open the **Settings** menu and select it from the **Pick a color scheme** list in the **Theme** section:

![The Pick a color scheme list in the Settings menu, with FT Theme selected](../assets/how-to/themes/pick_a_color.png)

Alternatively, visit `http://localhost:4000/ide/styleguide` for a preview of all the components in the web-client-ui library. To change the theme, click the theme selector in the top-right corner of the style guide:

![The theme selector in the top-right corner of the style guide](../assets/how-to/themes/style_guide_selector.png)

> [!NOTE]
> If you change your styles, refresh the page to see the changes.

## Theme file overview

You define your theme's styles in `plugins/<your-theme>/src/js/src/theme.css`. The file you copied contains the Financial Times example theme.

The file sets CSS variables on `:root`. They override the same variables in the base theme, the default dark or light theme that your theme inherits. [Inherit a base theme](#inherit-a-base-theme) shows how to choose it.

Override only the variables you want to change. Every other variable keeps its base-theme value. Most themes start by overriding a few colors and add more variables as needed.

<details>
<summary>See the entire example <code>theme.css</code> file.</summary>

```css
/* override the inherited base theme by scoping variables to :root */
:root {
  /*
  ============ PALETTE ===========
  GRAY 50, 75, 100, 200, 300, 400, 500, 600, 700, 800, 900

  RED, ORANGE, YELLOW, CHARTREUSE, CELERY, GREEN, SEAFOAM,
  CYAN, BLUE, INDIGO, PURPLE, FUCHSIA AND MAGENTA
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
  /packages/components/src/theme/theme-light/theme-light-components.css
  /packages/components/src/theme/theme-light/theme-light-semantic-grid.css
  /packages/components/src/theme/theme-light/theme-light-semantic-chart.css
  /packages/components/src/theme/theme-light/theme-light-semantic-editor.css
  */

  --dh-color-grid-header-bg: var(--dh-color-gray-100);
  --dh-color-grid-row-0-bg: var(--dh-color-gray-200);
  --dh-color-grid-row-1-bg: var(--dh-color-gray-100);
  --dh-color-grid-date: var(--dh-color-black);

  /* Chart colorway is special in that it contains a space separated 
  list of colors to assign in order for each series in a chart. 
  Consider including at least 10 colors. */
  --dh-color-chart-colorway: var(--dh-color-accent-bg)
    var(--dh-color-visual-green) var(--dh-color-visual-yellow)
    var(--dh-color-visual-purple) var(--dh-color-visual-orange)
    var(--dh-color-visual-red) var(--dh-color-visual-chartreuse)
    var(--dh-color-visual-fuchsia) var(--dh-color-visual-blue)
    var(--dh-color-visual-magenta) var(--dh-color-white);
}

/*
============ CUSTOM CLASS OVERRIDES ===========
Example of overriding specific class color variables for one-off customizations.
Use sparingly as these are not guaranteed to be stable across releases.
*/

/* target a specific class in the DOM and override the variables or properties */
.console-input-wrapper {
  --console-input-bg: var(--dh-color-gray-75);
  padding: 4px;
}
```

</details>

The web-client-ui repository lists the theme variables you can override in the [Default Dark](https://github.com/deephaven/web-client-ui/tree/main/packages/components/src/theme/theme-dark) and [Default Light](https://github.com/deephaven/web-client-ui/tree/main/packages/components/src/theme/theme-light) themes, organized into files by usage. You can also inspect elements in the browser to find the variable a component uses. To override a component's class directly, see [Override individual components with CSS selectors](#override-individual-components-with-css-selectors), which also explains why we don't recommend this approach.

## Create your theme

To create a theme:

1. [Inherit a base theme](#inherit-a-base-theme) (`dark` or `light`).
2. [Use color generators and define your palette](#use-color-generators-and-define-your-palette) (gray, red, orange, etc.).
3. [Use your palette for semantic colors](#use-your-palette-for-semantic-colors) (accent, positive, negative, etc.).
4. [Use your palette for component-specific semantic colors](#use-your-palette-for-component-specific-semantic-colors) (grid, plot, etc.).

The sections below show how we built the example theme. It inherits the `light` base theme in `index.ts`. Then, in `theme.css`, it defines `gray`, `red`, `green`, and `seafoam` palettes, points the accent colors at `seafoam`, and overrides a few grid and plot colors.

### Inherit a base theme

To create your theme, first decide whether your theme is light or dark. For example, if your desired background color is black or purple, inherit `dark`. If it's white or yellow, inherit `light`. Inheriting from either `dark` or `light` significantly reduces the amount of work required to create a custom theme.

In the `plugins/<your-theme>/src/js/src/index.ts` file, set the `baseTheme` of your theme to either `dark` or `light`. The theme's `name` is the name that appears in the theme selector, and `styleContent` holds the contents of your `theme.css` file:

```typescript
import type { ThemePlugin } from "@deephaven/plugin";
import styleContent from "./theme.css?inline";

const plugin: ThemePlugin = {
  name: "my-theme", // The plugin name, matching your plugin's folder name
  type: "ThemePlugin",
  themes: [
    {
      name: "My Theme", // The name shown in the theme selector
      baseTheme: "light", // The base theme to extend, either 'light' or 'dark'
      styleContent, // The contents of your theme.css file
    },
  ],
};

export default plugin;
```

### Use color generators and define your palette

The Deephaven design system is based on the [Adobe Spectrum](https://opensource.adobe.com/spectrum-design-data/tokens/color-palette/) color system. It uses a palette that consists of:

- 11 shades (50, 75, and 100-900) of a `gray` palette used for background colors
- 14 shades (100-1400) of each of 13 hues: `red`, `orange`, `yellow`, `chartreuse`, `celery`, `green`, `seafoam`, `cyan`, `blue`, `indigo`, `purple`, `fuchsia`, and `magenta`

The shades of every palette, `gray` and the 13 hues alike, range from light to dark for light themes and from dark to light for dark themes.

The `gray` shades aren't evenly spaced. In the light example theme, shades 50, 75, and 100 are close together in lightness, and shades 100 through 900 are spaced further apart. In the default dark palette, shades 50 through 400 are close together, and the large steps start at 500. Compare your shades with your base theme's palette file, [`theme-light-palette.css`](https://github.com/deephaven/web-client-ui/blob/main/packages/components/src/theme/theme-light/theme-light-palette.css) or [`theme-dark-palette.css`](https://github.com/deephaven/web-client-ui/blob/main/packages/components/src/theme/theme-dark/theme-dark-palette.css). The full palette of the example theme shows this spacing:

![A color palette example for the custom theme](../assets/how-to/example-theme-palette.jpg)

We recommend generating a palette with Adobe's [Leonardo](https://leonardocolor.io/theme.html) color tool. Leonardo is a great tool for building color ramps, the ordered series of shades for one color. Its **Export as CSS** feature gives the hex value of each shade. [Coolors](https://coolors.co/gradient-palette/fae7d5-24211d?number=11) also works. If your company has a brand guide with full palettes, you can use those instead.

> [!NOTE]
> Colors must be in the [sRGB color space](https://en.wikipedia.org/wiki/SRGB). Here we use HSL values, but you can use any supported color format. Supported formats include `#hex`, `rgb()`, `rgba()`, `hsl()`, `hwb()`, `color(srgb ...)`, and named colors. Other color spaces, such as LAB, OKLAB, and P3, are not supported.
>
> The web UI converts your colors to hex, because some components read colors in JavaScript rather than CSS:
>
> - Canvas-based grids.
> - Monaco code editors.
> - SVG and WebGL plots.
>
> The conversion handles only sRGB colors, which is why other color spaces aren't supported. The default themes also derive transparent variants of your palette colors with [`color-mix`](https://developer.mozilla.org/en-US/docs/Web/CSS/color_value/color-mix) in the sRGB color space.

To build the example theme, we picked a background color inspired by the Financial Times site and generated shades from it in Leonardo. The palette is named `gray`, but it can be any background color. The resulting `gray` background palette, which goes inside the `:root` block of `theme.css`, is:

```css
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
```

We also override the `red`, `green`, and `seafoam` palettes with colors inspired by the Financial Times site. Their values are in the [example `theme.css` file](#theme-file-overview).

### Use your palette for semantic colors

Semantic colors are variables named for the role a color plays rather than its hue. For example, `accent` sets the color of buttons and other highlighted controls, and `positive` and `negative` set the colors of gains and losses. Each semantic variable points at a palette color, so changing the palette changes every semantic color built on it.

Depending on how much you want to customize, you can stop after setting the palette and inherit the rest of the theme from the base theme. You can also override more variables to customize the theme further. For example, this guide's theme changes the `accent` variables from `blue` to `seafoam`. Like the palette, these declarations go inside the `:root` block:

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

`positive` defaults to `green`, and `negative` defaults to `red`. Updating the `green` and `red` palettes changes these variables. You can also change them independently if you want a different color for positive or negative values or actions.

For the full list of semantic variables, see the files with `semantic` in their names, such as `theme-light-semantic.css`, in the [Default Dark](https://github.com/deephaven/web-client-ui/tree/main/packages/components/src/theme/theme-dark) and [Default Light](https://github.com/deephaven/web-client-ui/tree/main/packages/components/src/theme/theme-light) themes.

### Use your palette for component-specific semantic colors

Component-specific semantic colors are variables that apply to a single component, such as the grid header background or the chart colorway. The colorway is the ordered list of colors a chart assigns to its series. Override these variables when the palette and semantic colors don't give you the look you want in a specific component.

The example theme overrides these component variables:

- The grid header and row stripe colors. Only the even-row stripe differs from the light theme's default.
- The grid's date color.
- The chart colorway.

These declarations also go inside the `:root` block. The overrides refer to variables that the base theme already defines, so you don't define them yourself. `--dh-color-black` and `--dh-color-white` map to shades of the `gray` palette. The semantic files linked in [Use your palette for semantic colors](#use-your-palette-for-semantic-colors) define `--dh-color-accent-bg` and the `--dh-color-visual-*` data-visualization colors.

```css
--dh-color-grid-header-bg: var(--dh-color-gray-100);
--dh-color-grid-row-0-bg: var(--dh-color-gray-200);
--dh-color-grid-row-1-bg: var(--dh-color-gray-100);
--dh-color-grid-date: var(--dh-color-black);

/* Chart colorway is special in that it contains a space separated 
list of colors to assign in order for each series in a chart. 
Consider including at least 10 colors. */
--dh-color-chart-colorway: var(--dh-color-accent-bg)
  var(--dh-color-visual-green) var(--dh-color-visual-yellow)
  var(--dh-color-visual-purple) var(--dh-color-visual-orange)
  var(--dh-color-visual-red) var(--dh-color-visual-chartreuse)
  var(--dh-color-visual-fuchsia) var(--dh-color-visual-blue)
  var(--dh-color-visual-magenta) var(--dh-color-white);
```

## Override individual components with CSS selectors

You can also override specific components with CSS selectors that set your own variables and properties, as the `.console-input-wrapper` rule at the end of the [example `theme.css` file](#theme-file-overview) does.

> [!CAUTION]
> We don't recommend overriding components this way, because class names and DOM structure can change in future web-client-ui releases. If you need a variable that the default themes don't expose, open a [web-client-ui GitHub issue](https://github.com/deephaven/web-client-ui/issues) or ask in the `#web-client-ui` channel of the [Deephaven Community Slack](/slack).

## Build and deploy your theme

The development servers only preview your theme. To use it on a Deephaven server, build the plugin and install it on that server:

1. Build the plugin. From the `plugins/<your-theme>/src/js` directory of your deephaven-plugins clone, run:

   ```bash
   npm run build
   ```

2. Install the plugin on your server as a JS-only plugin. The server registers plugins from Python packages only when Python is its script language. On a Groovy server, the Python packaging in the `theme-pack` directory you copied doesn't apply.

   To package the plugin, use a Dockerfile build stage based on the `ghcr.io/deephaven/web-plugin-packager` image:
   1. Copy your built `plugins/<your-theme>/src/js` directory into the stage at an absolute path, for example `COPY my-theme/src/js /my-theme`.
   2. Run the packaging script on that directory, for example `RUN ./pack-plugins.sh /my-theme`. Pass an absolute path, because the script runs `npm pack` from a temporary subdirectory, where a relative path such as `./my-theme` doesn't resolve.
   3. Copy the `js-plugins` directory that the script generates to the `js-plugins` directory in your server's configuration directory (`<configDir>/js-plugins/`).

   For details, see [Configure JS plugins](./configuration/js-plugins.md).

   For other ways to register a JS plugin, such as [pointing the server at the plugin's npm package directory](./create-js-plugins.md#register-an-npm-package-directory-during-development) during development, see [Create a JavaScript plugin](./create-js-plugins.md).

3. Restart the server, then select your theme in the **Settings** menu.

## Related documentation

- [Navigate the GUI](./user-interface/navigating-the-ui.md)
- [Install and use plugins](./install-use-plugins.md)
- [Create a JavaScript plugin](./create-js-plugins.md)
- [Configure JS plugins](./configuration/js-plugins.md)
- [Embed tables and charts in an IFrame](./use-iframes.md)
