---
title: Embed tables and charts in an IFrame
---

In addition to the Deephaven web UI, the Deephaven server serves pages that each display a single table, chart, or other widget. In this guide, you'll learn how to embed them in your own web page using inline frames (IFrames), authenticate from your page, and set the IFrame's color theme.

Run the following script in the Deephaven IDE console, or load it at startup with [Application Mode](./application-mode.md), to create the objects used in the examples below. It creates `sinTable`, a [time table](./time-table.md) that adds a row every second, and `sinChart`, a line chart of that table made with [`plot`](../reference/plot/plot.md):

```groovy ticking-table order=null
// Create a ticking table with X and Y values that trace a sine wave
sinTable = timeTable("PT1s").update("X = i", "Y = Math.sin(X)")

// Create a plot displaying the sinTable data
sinChart = plot("Y", sinTable, "X", "Y").chartTitle("Sine wave").show()
```

## Display a table or chart in an IFrame

If your server runs at `http://localhost:10000`, the URL for a specific table, chart, or other widget is:

```
http://localhost:10000/iframe/widget/?name=OBJECT_NAME
```

Replace `OBJECT_NAME` with the variable name of the object. For example, to show the `sinTable` created by the setup script at the top of this page, the IFrame URL is `http://localhost:10000/iframe/widget/?name=sinTable`. To show the `sinChart`, the IFrame URL is `http://localhost:10000/iframe/widget/?name=sinChart`.

Here is a basic HTML page that embeds the table and the chart, each in its own IFrame:

```html
<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Deephaven IFrame Tester</title>
  </head>
  <body>
    <h1>Sine table</h1>
    <iframe
      src="http://localhost:10000/iframe/widget/?name=sinTable"
      width="800"
      height="500"
    ></iframe>

    <h1>Sine chart</h1>
    <iframe
      src="http://localhost:10000/iframe/widget/?name=sinChart"
      width="800"
      height="500"
    ></iframe>
  </body>
</html>
```

![A ticking table embedded in an IFrame](../assets/how-to/iframe-table-basic.gif)

![A ticking chart embedded in an IFrame](../assets/how-to/iframe-chart-basic.gif)

Let's watch it tick: the embedded table and chart stay live, so each new row that `sinTable` adds every second appears in your page just as it does in the Deephaven IDE.

If the server requires authentication, each IFrame can show its own Deephaven login screen. To log in from your page instead, see [Authenticate from the parent window](#authenticate-from-the-parent-window).

## Authenticate from the parent window

Your page can supply the Deephaven login credentials itself, so the embedded table or chart opens without a separate login inside the IFrame. To turn this on, add the `authProvider=parent` query parameter to the IFrame URL.

In the steps below, the parent window is your page that contains the IFrame, and the child window is the Deephaven page inside the IFrame:

1. The child window uses [`postMessage`](https://developer.mozilla.org/en-US/docs/Web/API/Window/postMessage) to send the parent window an `io.deephaven.message.LoginOptions.request` message.
2. Your page responds with the same message `id` and a `payload` that holds the login options, such as the authentication `type` and `token`.
3. The child window logs in to Deephaven with those options and shows the table or chart.

The following page authenticates both IFrames with [pre-shared key authentication](./authentication/auth-psk.md). For another authentication type, change the `type` and `token` in the payload, and add a `username` if the handler requires one.

The response contains the server's secret key, so the page accepts requests only from the Deephaven server's origin and sends its response only to that origin. Anyone who can load your page can read a key written into its source, so hard-code the key only for local testing. In production, have your page fetch the token from your own backend after it signs the user in.

<details>
<summary> Expand for the full HTML. </summary>

```html
<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Deephaven IFrame Tester</title>
    <script>
      /**
       * Listen for events on the window, sent from the IFrame
       */
      window.addEventListener(
        'message',
        function (e) {
          const { data, origin, source } = e;

          /** Get the origin of the Deephaven server from the IFrame URL */
          const dhServerOrigin = new URL(
            document.getElementById('tableIframe').src
          ).origin;

          /** Ignore messages that are not from the Deephaven server */
          if (origin !== dhServerOrigin) {
            return;
          }

          console.log('message received:', data);
          const { id, message } = data;

          /** Only look for the login request */
          if (message === 'io.deephaven.message.LoginOptions.request') {
            /**
             * Specify the authentication type and any other parameters for logging in.
             */
            source.postMessage(
              {
                /** Need to respond with the same message ID */
                id,

                /**
                 * payload is the login options the Deephaven page uses to log in.
                 *   type: Authentication type to use
                 *   token: Authentication token or password
                 *   username?: Username to authenticate against
                 */
                payload: {
                  /**
                   * The authentication handler to authenticate with.
                   * See https://github.com/deephaven/deephaven-core/tree/main/authentication
                   */
                  type: 'io.deephaven.authentication.psk.PskAuthenticationHandler',

                  /**
                   * Replace this token with the token configured for the server.
                   * Anyone who can view this page can read it; use a hard-coded key only for local testing.
                   */
                  token: 'hello',
                },
              },
              /** Send the token only to the Deephaven server */
              dhServerOrigin
            );
          }
        },
        false
      );
    </script>
  </head>
  <body>
    <h1>Sine table</h1>

    <!--
      The IFrame for our table.
      In addition to the `name` parameter we previously specified, we also specify `authProvider=parent`.
      That triggers the embedded IFrame to wait for login credentials from the parent window.
    -->
    <iframe
      id="tableIframe"
      src="http://localhost:10000/iframe/widget/?name=sinTable&authProvider=parent"
      width="800"
      height="500"
    ></iframe>

    <h1>Sine chart</h1>
    <!--
      The IFrame for our chart. We also specify `authProvider=parent` here, and this parent window authenticates both IFrames.
    -->
    <iframe
      src="http://localhost:10000/iframe/widget/?name=sinChart&authProvider=parent"
      width="800"
      height="500"
    ></iframe>
  </body>
</html>
```

</details>

![The embedded IFrames wait for login credentials from the parent window before showing the table and chart](../assets/how-to/iframe-authentication.gif)

## Set theme colors

Your page can also set the color theme of the Deephaven page inside an IFrame with [`postMessage`](https://developer.mozilla.org/en-US/docs/Web/API/Window/postMessage). To turn this on, add the `theme=external-theme` query parameter to the IFrame URL. The Deephaven page then asks your page for a theme when it loads, and your page must answer. After that, your page can send a new theme at any time.

The following IFrame URL includes `theme=external-theme` and the optional `preloadTransparentTheme=true` parameter, which lets the parent window show through while the IFrame is still loading:

```html
<iframe
  src="http://localhost:10000/iframe/widget/?name=sinTable&theme=external-theme&preloadTransparentTheme=true"
></iframe>
```

Both the response to the theme request and any theme you send later carry the same theme settings, which have three parts:

- `name` (required): a name for the theme.
- `baseThemeKey` (optional): the base theme to start from, either `default-dark` or `default-light`. Defaults to `default-dark`.
- `cssVars` (required): overrides for `--dh-color-*` [theme variables](./custom-themes.md). To use the base theme unchanged, pass an empty object (`{}`). Deephaven ignores any entry that isn't a `--dh-color-*` variable set to a valid CSS color.

### Respond to the theme request

The Deephaven page inside the IFrame sends an `io.deephaven.message.ThemeModel.requestExternalTheme` message when it loads. Respond to it with the theme settings and the same message `id`.

The following page responds to the theme request with the dark base theme and custom grid colors:

<details>
<summary> Expand for the full HTML. </summary>

```html
<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Deephaven IFrame Tester</title>
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
      src="http://localhost:10000/iframe/widget/?name=sinTable&theme=external-theme&preloadTransparentTheme=true"
    ></iframe>

    <script>
      // Get the origin of the Deephaven server
      const dhServerOrigin = new URL(document.querySelector('iframe').src)
        .origin;

      /** Handle postMessage events from the child IFrame */
      window.addEventListener(
        'message',
        function onMessage({ data, origin, source }) {
          // Ignore messages that are not from our Deephaven server
          if (origin !== dhServerOrigin) {
            return;
          }

          // Respond to theme request from Deephaven in the IFrame
          if (
            data.message ===
            'io.deephaven.message.ThemeModel.requestExternalTheme'
          ) {
            // Base theme key determines whether base colors are for dark or light theme.
            // The parent can choose either 'default-dark' or 'default-light'.
            const baseThemeKey = 'default-dark';

            // Set a custom background color based on the base theme key
            const bgColor =
              baseThemeKey === 'default-dark' ? 'midnightblue' : 'aliceblue';

            source.postMessage(
              {
                id: data.id,
                payload: {
                  name: 'IFrame External Theme',
                  baseThemeKey,
                  // Override --dh-color-xxx theme variables
                  cssVars: {
                    '--dh-color-grid-header-bg': bgColor,
                    '--dh-color-grid-bg': `color-mix(in srgb, ${bgColor} 70%, black 30%)`,
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

### Send a theme at any time

To set or change the theme at any time, send the Deephaven page inside the IFrame an `io.deephaven.message.ThemeModel.requestSetTheme` message that holds the theme settings. Deephaven accepts this message only when the IFrame URL includes the `theme=external-theme` query parameter.

Your page must still answer the initial `io.deephaven.message.ThemeModel.requestExternalTheme` message, as shown in [Respond to the theme request](#respond-to-the-theme-request). Until the IFrame receives a theme, it shows nothing. If your page never answers the request, it times out after 30 seconds and the IFrame switches to the default dark theme, even if your page already sent a theme with `requestSetTheme`.

The following page extends the [theme request page](#respond-to-the-theme-request) with a **Toggle Theme** button. It answers the initial theme request with the dark base theme, and each click then sends `requestSetTheme`, switching between the light and dark base themes:

<details>
<summary> Expand for the full HTML. </summary>

```html
<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Deephaven IFrame Tester</title>
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
      #btnUpdateTheme {
        position: absolute;
        bottom: 0;
        padding: 0.5rem 1rem;
      }
    </style>
  </head>
  <body>
    <iframe
      src="http://localhost:10000/iframe/widget/?name=sinTable&theme=external-theme&preloadTransparentTheme=true"
    ></iframe>
    <button id="btnUpdateTheme" onclick="onUpdateTheme()">Toggle Theme</button>

    <script>
      // Get the origin of the Deephaven server
      const dhServerOrigin = new URL(document.querySelector('iframe').src)
        .origin;

      let baseThemeKey = 'default-dark';

      /** Build the theme settings for the current base theme */
      function getThemePayload() {
        const bgColor =
          baseThemeKey === 'default-dark' ? 'midnightblue' : 'aliceblue';
        return {
          name: 'IFrame External Theme',
          baseThemeKey,
          cssVars: {
            '--dh-color-grid-header-bg': bgColor,
            '--dh-color-grid-bg': `color-mix(in srgb, ${bgColor} 70%, black 30%)`,
          },
        };
      }

      /** Answer the initial theme request from Deephaven in the IFrame */
      window.addEventListener('message', ({ data, origin, source }) => {
        if (origin !== dhServerOrigin) {
          return;
        }
        if (
          data.message === 'io.deephaven.message.ThemeModel.requestExternalTheme'
        ) {
          source.postMessage({ id: data.id, payload: getThemePayload() }, origin);
        }
      });

      /** Handle button click */
      function onUpdateTheme() {
        const childWindow = document.querySelector('iframe').contentWindow;

        // Switch between the dark and light base themes
        baseThemeKey =
          baseThemeKey === 'default-dark' ? 'default-light' : 'default-dark';

        // Initiate setting the theme from the parent window
        childWindow?.postMessage(
          {
            message: 'io.deephaven.message.ThemeModel.requestSetTheme',
            payload: getThemePayload(),
          },
          dhServerOrigin
        );
      }
    </script>
  </body>
</html>
```

</details>

![The parent page toggles the embedded table between the dark and light base themes](../assets/how-to/iframe-color-theme.gif)

## Related documentation

- [Application Mode](./application-mode.md)
- [Built-in plotting API](./plotting/api-plotting.md)
- [Configure and use pre-shared key authentication](./authentication/auth-psk.md)
- [Create a time table](./time-table.md)
- [Create custom themes](./custom-themes.md)
- [`plot`](../reference/plot/plot.md)
- [`timeTable`](../reference/table-operations/create/timeTable.md)
- [`update`](../reference/table-operations/select/update.md)
- [`plot` Javadoc](/core/javadoc/io/deephaven/plot/Figure.html#plot(java.lang.Comparable,io.deephaven.engine.table.Table,java.lang.String,java.lang.String))
- [`timeTable` Javadoc](/core/javadoc/io/deephaven/engine/util/TableTools.html#timeTable(java.lang.String))
- [`update` Javadoc](/core/javadoc/io/deephaven/api/TableOperations.html#update(java.lang.String...))
