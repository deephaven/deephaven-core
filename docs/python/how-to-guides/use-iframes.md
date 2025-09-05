---
title: Display tables in an Inline Frame
sidebar_label: IFrames
---

In this guide, you'll learn how to create a basic web page with embedded tables and charts from Deephaven using Inline Frames (IFrames). In addition to the Deephaven Web UI, the Deephaven server also provides endpoints for fetching individual tables or charts. Add the following script using [Application Mode](./application-mode.md) to run through the examples below:

```python ticking-table order=null
from deephaven import time_table
from deephaven.plot import Figure

# Create a ticking table with x and y values showing a sin wave
sin_table = time_table("PT1s").update(["x=i", "y=Math.sin(x)"])

# Create a plot displaying the sin_table data
sin_chart = Figure().plot_xy(series_name="Sin wave", t=sin_table, x="x", y="y").show()
```

## Display tables in an IFrame

Assuming your server is running at `http://localhost:10000/ide/`, the URL for retrieving a specific table is:

```
http://localhost:10000/iframe/widget/?name=TABLE_NAME
```

For example, to show the `sin_table` from the code above, the IFrame URL is `http://localhost:10000/iframe/widget/?name=sin_table`.

Here is a basic HTML page embedding an IFrame table:

```html
<html>
  <body>
    <h1>Sin Table</h1>
    <iframe
      src="http://localhost:10000/iframe/widget/?name=sin_table"
      width="800"
      height="500"
    ></iframe>
  </body>
</html>
```

![The `sin_table` from the code above, embedded in an IFrame](../assets/how-to/iframe-table-basic.gif)

## Display charts in an IFrame

Assuming your server is running at `http://localhost:10000/ide/`, the URL for retrieving a specific chart is:

```
http://localhost:10000/iframe/widget/?name=CHART_NAME
```

For example, to show the `sin_chart` from the code above, the IFrame URL is `http://localhost:10000/iframe/widget/?name=sin_chart`.

Here is a basic HTML page embedding an IFrame chart:

```html
<html>
  <body>
    <h1>Sin Chart</h1>
    <iframe
      src="http://localhost:10000/iframe/widget/?name=sin_chart"
      width="800"
      height="500"
    ></iframe>
  </body>
</html>
```

![The `sin_chart` from the code above embedded in an IFrame](../assets/how-to/iframe-chart-basic.gif)

## Authentication

When embedding in IFrames, you may want to provide authentication details from the parent window. This can be done by providing the `authProvider=parent` query parameter on the IFrame URL, and then responding to the authentication request sent by the child window.

<details>
<summary> Expand for the full HTML. </summary>

```html
<html>
  <head>
    <script>
      /**
       * Listen for events on the window, sent from the IFrame
       */
      window.addEventListener(
        "message",
        function (e) {
          console.log("message received:  ", e.data);
          const { data, source } = e;
          const { id, message } = data;

          /** Only look for the login request */
          if (message === "io.deephaven.message.LoginOptions.request") {
            /**
             * Specify the authentication type and any other parameters for logging in.
             */
            source.postMessage(
              {
                /** Need to respond with the same message ID */
                id,

                /**
                 * payload is the login options for `client.login`.
                 *   type: Authentication type to use
                 *   token: Authentication token or password
                 *   username?: Username to authenticate against
                 */
                payload: {
                  /**
                   * The authentication handler to authenticate with.
                   * See https://github.com/deephaven/deephaven-core/tree/main/authentication
                   */
                  type: "io.deephaven.authentication.psk.PskAuthenticationHandler",

                  /**
                   * Replace this token with the token configured for the server.
                   * Should be kept secret.
                   */
                  token: "hello",
                },
              },
              "*"
            );
          }
        },
        false
      );
    </script>
  <body>
    <h1>Sin Table</h1>

    <!--
      The IFrame for our table.
      In addition to the `name` parameter we previously specified, we also specify `authProvider=parent`.
      That triggers the embedded IFrame to wait for login credentials from the parent window.
     -->
    <iframe
      id="tableIframe"
      src="http://localhost:10000/iframe/widget/?name=sin_table&authProvider=parent"
      width="800"
      height="500"
    ></iframe>

    <h1>Sin Chart</h1>
    <!--
      The IFrame for our chart. We also specify `authProvider=parent` here, and this parent window will authenticate both IFrames.
    -->
    <iframe
      src="http://localhost:10000/iframe/widget/?name=sin_chart&authProvider=parent"
      width="800"
      height="500"
    ></iframe>
  </body>
</html>
```

</details>

![The embedded IFrame waits for login credentials from the parent window before generating the above table and chart](../assets/how-to/iframe-authentication.gif)

## Set theme colors

When embedding in IFrames, Deephaven can request color theme settings from the parent window using `postMessage` APIs. Provide the `theme=external-theme` query parameter on the IFrame URL, and then respond to the theme request sent by the child window.

```html
<iframe
  src="http://localhost:10000/iframe/widget/?name=simple_ticking&theme=external-theme"
></iframe>
```

You can optionally provide the `preloadTransparentTheme=true` query parameter to allow the parent window to show while the IFrame is still loading.

```html
<iframe
  src="http://localhost:10000/iframe/widget/?name=simple_ticking&theme=external-theme&preloadTransparentTheme=true"
></iframe>
```

<details>
<summary> Expand for the full HTML. </summary>

```html
<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <link
      rel="icon"
      type="image/png"
      href="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAYAAABzenr0AAAABmJLR0QA/wD/AP+gvaeTAAAA9klEQVRYhe3WMUoDQRSH8e+tkw0bdhtj7wXSmUSIVboUVjYeIIU5hOIVBG8SUppSUBAstDSQFKkEWbOQBLPseoR5KWQt3tQf//nBNCO9y8UbihO14/dGP2n5OqFcjtv1gWYTwAHeUYAiL740bYkk2ssBgn3ivzgGMIABKgc4bVhkxbwUPnxdnGXfq+7VULNZc/mDGvAz275OTsI7X7fpDY9zDuaazV0eXFT+BAYwgAEMIOeP20wTXtdunjrhy6k3LFmko9aZZvMwaq6d1INYE0fBLgT8H04haT7frzSb8A+ewAAGMEDlAAdyqwk/OUqBqTcU0n0Av+DQNksxGrVgAAAAAElFTkSuQmCC"
    />
    <title>Deephaven Iframe Tester</title>
    <style>
      html,
      body {
        background-color: midnightblue;
        margin: 0;
        font-family: sans-serif;
      }
      body::before {
        color: white;
        font-size: 2rem;
        position: absolute;
        top: 0;
        display: flex;
        align-items: center;
        justify-content: center;
        content: 'Parent Container';
        width: 100%;
        height: 100%;
      }
      iframe {
        display: block;
        position: relative;
        width: 100vw;
        height: 100vh;
        border: none;
      }
    </style>
  </head>
  <body>
    <iframe
      src="http://localhost:10000/iframe/widget/?name=simple_ticking&theme=external-theme&preloadTransparentTheme=true"
    ></iframe>

    <script>
      // Get the origin of the Deephaven server
      const dhServerOrigin = new URL(document.querySelector('iframe').src)
        .origin;

      /** Handle postMessage events from the child iframe */
      window.addEventListener(
        'message',
        function onMessage({ data, origin, source }) {
          // Ignore messages that are not from our Deephaven server
          if (origin !== dhServerOrigin) {
            return;
          }

          // Respond to theme request from Deephaven in the iframe
          if (
            data.message ===
            'io.deephaven.message.ThemeModel.requestExternalTheme'
          ) {
            // Base theme key determines whether base colors are for dark or light theme
            const baseThemeKey =
              Math.random() > 0.5 ? 'default-dark' : 'default-light';

            // Set a custom background color based on the base theme key
            const bgColor =
              baseThemeKey === 'default-dark' ? 'midnightblue' : 'aliceblue';

            source.postMessage(
              {
                id: data.id,
                payload: {
                  name: 'Iframe External Theme',
                  baseThemeKey,
                  // Override --dh-color-xxx theme variables
                  cssVars: {
                    '--dh-color-grid-header-bg': bgColor,
                    '--dh-color-grid-bg': `color-mix(in srgb, ${bgColor} 70%, black 50%)`,
                  },
                },
              },
              origin
            );
          }
        }
      );
    </script>
  </body>
</html>
```

</details>

The parent window can also explicitly set the theme by sending a `requestSetTheme` message to the child window.

<details>
<summary> Expand for the full HTML. </summary>

```html
<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <link
      rel="icon"
      type="image/png"
      href="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAYAAABzenr0AAAABmJLR0QA/wD/AP+gvaeTAAAA9klEQVRYhe3WMUoDQRSH8e+tkw0bdhtj7wXSmUSIVboUVjYeIIU5hOIVBG8SUppSUBAstDSQFKkEWbOQBLPseoR5KWQt3tQf//nBNCO9y8UbihO14/dGP2n5OqFcjtv1gWYTwAHeUYAiL740bYkk2ssBgn3ivzgGMIABKgc4bVhkxbwUPnxdnGXfq+7VULNZc/mDGvAz275OTsI7X7fpDY9zDuaazV0eXFT+BAYwgAEMIOeP20wTXtdunjrhy6k3LFmko9aZZvMwaq6d1INYE0fBLgT8H04haT7frzSb8A+ewAAGMEDlAAdyqwk/OUqBqTcU0n0Av+DQNksxGrVgAAAAAElFTkSuQmCC"
    />
    <title>Deephaven Iframe Tester</title>
    <style>
      html,
      body {
        background-color: midnightblue;
        margin: 0;
        font-family: sans-serif;
      }
      body::before {
        color: white;
        font-size: 2rem;
        position: absolute;
        top: 0;
        display: flex;
        align-items: center;
        justify-content: center;
        content: 'Click "Toggle Theme" to set the theme';
        width: 100%;
        height: 100%;
      }
      iframe {
        display: block;
        position: relative;
        width: 100vw;
        height: 100vh;
        border: none;
      }
      #btnUpdateTheme {
        display: flex;
        align-items: center;
        gap: 0.5rem;
        position: absolute;
        bottom: 0;
        padding: 0.5rem 1rem 0.5rem 0.5rem;
        &:before {
          content: '';
          background: no-repeat left center
            url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAYAAABzenr0AAAABmJLR0QA/wD/AP+gvaeTAAAA9klEQVRYhe3WMUoDQRSH8e+tkw0bdhtj7wXSmUSIVboUVjYeIIU5hOIVBG8SUppSUBAstDSQFKkEWbOQBLPseoR5KWQt3tQf//nBNCO9y8UbihO14/dGP2n5OqFcjtv1gWYTwAHeUYAiL740bYkk2ssBgn3ivzgGMIABKgc4bVhkxbwUPnxdnGXfq+7VULNZc/mDGvAz275OTsI7X7fpDY9zDuaazV0eXFT+BAYwgAEMIOeP20wTXtdunjrhy6k3LFmko9aZZvMwaq6d1INYE0fBLgT8H04haT7frzSb8A+ewAAGMEDlAAdyqwk/OUqBqTcU0n0Av+DQNksxGrVgAAAAAElFTkSuQmCC);
          background-size: cover;
          width: 20px;
          height: 20px;
        }
      }
    </style>
  </head>
  <body>
    <iframe
      src="http://localhost:10000/iframe/widget/?name=simple_ticking&theme=external-theme&preloadTransparentTheme=true"
    ></iframe>
    <button id="btnUpdateTheme" onclick="onUpdateTheme()">Toggle Theme</button>

    <script>
      // Get the origin of the Deephaven server
      const dhServerOrigin = new URL(document.querySelector('iframe').src)
        .origin;

      let baseThemeKey = 'default-dark';

      /** Handle button click */
      function onUpdateTheme() {
        const iframeEl = document.querySelector('iframe');
        const childWindow = iframeEl.contentWindow;

        // Base theme key determines whether base colors are for dark or light theme
        baseThemeKey =
          baseThemeKey === 'default-dark' ? 'default-light' : 'default-dark';

        // Set a custom background color based on the base theme key
        const bgColor =
          baseThemeKey === 'default-dark' ? 'midnightblue' : 'aliceblue';

        // Initiate setting the theme from the parent window
        childWindow?.postMessage(
          {
            message: 'io.deephaven.message.ThemeModel.requestSetTheme',
            payload: {
              name: 'Iframe External Theme',
              baseThemeKey,
              cssVars: {
                '--dh-color-grid-header-bg': bgColor,
                '--dh-color-grid-bg': `color-mix(in srgb, ${bgColor} 70%, black 50%)`,
              },
            },
          },
          dhServerOrigin
        );
      }
    </script>
  </body>
</html>
```

</details>

![IFrame Color Theme](../assets/how-to/iframe-color-theme.gif)

## Related documentation

- [Application Mode](./application-mode.md)
