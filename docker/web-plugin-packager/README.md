# web-plugin-packager

web-plugin-packager is a docker image used to package JS plugins for the Deephaven Web UI with your own custom docker container.

## Usage

First, follow the instructions to [Launch Deephaven from pre-built images](https://deephaven.io/core/docs/tutorials/quickstart/#set-up-your-deephaven-deployment).

Once you have the `docker-compose.yml` file pulled down, define your own web docker image in `web/Dockerfile` that includes the plugins you would like to use. 

1. Create subdirectory `web` in the same folder as your `docker-compose.yml`: `mkdir web`
2. Create the `Dockerfile` for web and open for editing: `vi web/Dockerfile`
3. Paste the following into the `web/Dockerfile` and save:
```
# Pull the web-plugin-packager image
FROM ghcr.io/deephaven/web-plugin-packager:latest as build

# Specify the plugins you wish to use. You can specify multiple plugins separated by a space, and optionally include the version number, e.g.
# RUN ./pack-plugins.sh <js-plugin-name>[@version] ...
# For a list of published plugins, see https://www.npmjs.com/search?q=keywords%3Adeephaven-js-plugin
# Here is how you would install the matplotlib and table-example plugins
RUN ./pack-plugins.sh @deephaven/js-plugin-matplotlib @deephaven/js-plugin-table-example

# Copy the packaged plugins over
FROM ghcr.io/deephaven/web:latest
COPY --from=build js-plugins/ /usr/share/nginx/html/js-plugins/
```

Many plugins will require a server side component as well. To define the plugins used on the server, create a `server/Dockerfile` similar to above:

1. Create subdirectory `server` in the same folder as your `docker-compose.yml`: `mkdir server`
2. Create the `Dockerfile` for server and open for editing: `vi server/Dockerfile`
3. Paste the following into the `server/Dockerfile` and save:
```
FROM ghcr.io/deephaven/server:latest

# pip install any of the plugins required on the server
RUN pip install deephaven-plugin-matplotlib
```

After building, you need to specify using that build in your `docker-compose`. Do this by modifying the existing a `docker-compose.yml` file and replace the web and server definitions with the following:
```yaml
services:
  server:
    # Comment out the image name
    # image: ghcr.io/deephaven/server:${VERSION:-latest}
    # Build from your local Dockerfile you just created
    build: ./server
    
    ...
    
  web:
    # Comment out the image name
    # image: ghcr.io/deephaven/web:${VERSION:-latest}
    # Build from your local Dockerfile you just created
    build: ./web
```

When you're done, your directory structure should look like:
```commandline
.
├── docker-compose.yml
├── server
│   └── Dockerfile
└── web
    └── Dockerfile
```

Everything's ready to go! Now you just need to run `docker-compose up` as normal, and you will be using your custom image with your JS plugins installed. See what you can create!

## JS Plugin Development

For instructions on developing your own JS plugin, look at the [deephaven-js-plugin-template repository](https://github.com/deephaven/deephaven-js-plugin-template/).