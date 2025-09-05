---
title: Install and use Java packages
---

This guide discusses how to install Java packages and use them in Deephaven queries.

Java packages are installed for use with Deephaven by adding them to the Deephaven Docker images. This way, they are available every time Deephaven is launched.

Once a package is installed, it can be imported and used like any other Java package.

> [!CAUTION]
> If a Java package has dependencies that Deephaven does not have, those packages will not work unless the required dependencies are installed as well. Java packages can also have dependencies that conflict with Deephaven's, so it's important to be careful when adding Java packages.

## Add packages to a custom Docker image

In order to use Java packages within Deephaven, you must put the associated `.jar` files in the `/apps/libs` directory of your `server` image. (The `/apps/libs` directory is the default `EXTRA_CLASSPATH` location in the [Docker images](./configuration/docker-application.md).)

In this section, we add the [Java CryptoCompare API client](https://mvnrepository.com/artifact/com.github.jeffreytai/cryptocompare-api-wrapper) to a custom Dockerfile and reference it from Deephaven so that we can use it in more than one session.

In order to use packages more than once, you can create a custom Docker image, and then use that image in Deephaven. The steps for accomplishing this differ slightly depending on how you launch Deephaven. Let's start with the steps that are common between both.

### Prerequisites

Before a custom Docker image can be built, you must acquire the necessary base images. This process differs based upon how you launch Deephaven:

- If you [launch from pre-built images](../getting-started/docker-install.md), ensure you have run the following command to download the necessary base images:

```shell
docker compose pull
```

- If you [launch from source code](../getting-started/launch-build.md), ensure you have built the project so that you have the necessary base images.

### Create a custom Dockerfile

To begin, create a new directory. This directory should not be in a Deephaven deployment directory. You can name it whatever you'd like. For this guide, we'll name ours `deephaven-custom`.

```shell
mkdir deephaven-custom
cd deephaven-custom
```

Now, in this directory, create a file called `Dockerfile`. `Dockerfile` should use `ghcr.io/deephaven/server` as the base image and should contain a recipe for installing the new package. When adding the [Java CryptoCompare API client](https://mvnrepository.com/artifact/com.github.jeffreytai/cryptocompare-api-wrapper), it looks like this:

```
FROM ghcr.io/deephaven/server
RUN curl --output /apps/libs
https://repo1.maven.org/maven2/com/github/jeffreytai/cryptocompare-api-wrapper/1.0.0/cryptocompare-api-wrapper-1.0.0.jar
```

### Create a custom Docker image

Now that we have the `Dockerfile` in place, we need to create the custom Docker image. To do so, run a command from the directory with `Dockerfile` that looks like:

```shell
docker build --tag <user>/server-<custom> .
```

This creates a new Docker image named `<user>/server-<custom>`. For this guide, we will call the image `guide/server-cryptocompare`:

```shell
docker build --tag guide/server-cryptocompare .
```

When the command finishes running, you can see the new image in your system:

```shell
docker image ls
```

### Reference the new image

To put it all together, we now need to reference this new image in the `docker-compose` file we use to launch Deephaven. The particular file depends on how you build and launch Deephaven:

- If you [launch from pre-built images](../getting-started/docker-install.md), the file is `docker-compose.yml` and can be found in your `deephaven-deployment` directory.

- If you [launch from source code](../getting-started/launch-build.md), the file is `docker-compose-common.yml` and can be found in your `deephaven-core` directory.

In the Docker Compose file, there are three lines of text that look like:

```
services:
  server:
    image: <IMAGE_NAME>
```

The image used by default depends on how you build and launch Deephaven. Regardless, this line is where you need to insert your custom image name. Modify the `image` line to use your new image:

```
services:
  server:
    image: guide/server-cryptocompare:latest
```

Now, when you launch Deephaven again, you can use the package!

> [!CAUTION]
> When base images are updated by rebuilding source code or redownloading pre-built images, custom images must be rebuilt to incorporate the base image changes.

## List all available Java packages

You can check what Java packages are available to Deephaven by running the following command from your Deephaven installation while Deephaven is running.

```bash
docker compose exec server ls /apps/libs
```

## Use Java packages in query strings

Installed Java packages can be used in query strings. We'll use the [Apache Commons Lang](https://commons.apache.org/proper/commons-lang/) [`RandomUtils`](https://commons.apache.org/proper/commons-lang/apidocs/org/apache/commons/lang3/RandomUtils.html) class to create a table with ten rows of random integers in a range.

```python
from deephaven import empty_table

t = empty_table(10).update(
    formulas=["X = org.apache.commons.lang3.RandomUtils.nextInt(1, 99)"]
)
```

## Related documentation

- [Create an empty table](./new-and-empty-table.md#empty_table)
- [How to install packages](./install-packages.md)
- [How to configure the Deephaven Docker application](./configuration/docker-application.md)
- [Launch Deephaven from pre-built images](../getting-started/docker-install.md)
- [Build and launch Deephaven from source code](../getting-started/launch-build.md)
- [How to use Java packages in query strings](./install-and-use-java-packages.md#use-java-packages-in-query-strings)
- [How to use jpy](./use-jpy.md)
- [Access your file system with Docker data volumes](../conceptual/docker-data-volumes.md)
