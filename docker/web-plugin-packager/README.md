# web-plugin-packager

web-plugin-packager is a docker image used to package JS plugins for the Deephaven Web UI with your own custom docker container.

## Usage

First, follow the instructions to [Launch Deephaven from pre-built images](https://deephaven.io/core/docs/tutorials/quickstart/#set-up-your-deephaven-deployment).

Once you have the docker-compose.yml file pulled down, you must create your own web image that includes the web plugins you would like to use. Create a `Dockerfile` in the same folder as your `docker-compose.yml`, and fill it with the following:

```
# Pull the web-plugin-packager image
FROM ghcr.io/deephaven/web-plugin-packager:latest as build

# Specify the plugins you wish to use. You can specify multiple plugins separated by a space, and optionally include the version number, e.g.
# RUN ./pack-plugins.sh <js-plugin-name>[@version] ...
# For a list of published plugins, see https://www.npmjs.com/search?q=%22%40deephaven%2Fjs-plugin%22
# Here is how you would install the matplotlib and table-example plugins
RUN ./pack-plugins.sh @deephaven/js-plugin-matplotlib @deephaven/js-plugin-table-example

# Copy the packaged plugins over
FROM ghcr.io/deephaven/web:latest
COPY --from=build js-plugins/ /usr/share/nginx/html/js-plugins/
```

Save the file. Next, build and tag your custom image:

```commandline
docker build . -t my-deephaven-web:local-build
```

After building, you need to specify using that build in your `docker-compose`. Do this by creating a `docker-compose.override.yml` file with the following:
```yaml
services:
  web:
    # Use the image you just created/tagged in the previous step
    image: my-deephaven-web:local-build
```

Everything's ready to go! Now you just need to run `docker-compose up` as normal, and you will be using your custom image with your JS plugins installed. See what you can create!

## JS Plugin Development

For instructions on developing your own JS plugin, look at the [deephaven-js-plugin-template repository](https://github.com/deephaven/deephaven-js-plugin-template/).