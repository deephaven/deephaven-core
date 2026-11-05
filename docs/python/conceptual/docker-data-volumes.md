---
title: Access your file system with Docker bind mounts
---

Deephaven reads and writes files on your computer through [Docker bind mounts](https://docs.docker.com/storage/bind-mounts/). A bind mount maps a directory on your computer to a directory inside the Deephaven container. A mount point is the directory inside the container where a directory from your local file system appears.

This guide covers only bind mounts. The [Docker Compose](https://docs.docker.com/compose/) files that Deephaven publishes use `volumes` entries that map local directories, which are bind mounts.

Docker also has a separate volume type, which Docker manages. This guide does not cover it. See [Docker volumes](https://docs.docker.com/engine/storage/volumes/) if you want one, for example a named volume for data that doesn't need to appear on your computer.

## Why mounts are needed

The Deephaven server runs in its own Docker container. You can start it with [Docker Compose](https://docs.docker.com/compose/), which reads its configuration from a YAML (`.yml`) file, or with `docker create` and `docker start`.

Docker containers do not store persistent data by design. To persist data, you mount a directory from your local file system into the container. For example, you can mount directory `./abc` on your local file system at `/xyz` when you start a container. For Deephaven, the Docker Compose YAML file configures these mounts.

## The `/data` mount point

Your deployment directory is the local directory that holds your Compose YAML file and that you launch Deephaven from. Deephaven maps the `./data` directory in your deployment directory to `/data` in the container by default.

If you [launch Deephaven from pre-built images](../getting-started/docker-install.md), that file is named `docker-compose.yml`.

The [Docker Compose](https://docs.docker.com/compose/) files that Deephaven publishes set this mapping with a `volumes` entry in the Compose YAML file:

```yaml
volumes:
  - ./data:/data
```

If you write data to `/data/abc/file.csv` from the Deephaven console, that file appears at `./data/abc/file.csv` on your computer.

If you copy the local file `abc.parquet` to `./data/abc.parquet`, you can read it at `/data/abc.parquet` on the Deephaven server.

## Add mount points

You can also create other mount points. A service is a container definition in the [Docker Compose](https://docs.docker.com/compose/) file. To create a mount point, add an entry to the `volumes` list of the Deephaven service in your Compose YAML file. The pre-built Compose files name this service `deephaven`.

Say we're working on a machine learning application and want separate mount points to store training, testing, and validation data sets. Add these mount points by modifying the `volumes` section of the Docker Compose YAML file for the Deephaven service. Keep the `- ./data:/data` entry and add the three new ones:

```yaml
volumes:
  - ./data:/data
  - ./ML_Train:/ML_Train
  - ./ML_Test:/ML_Test
  - ./ML_Validate:/ML_Validate
```

After you edit the file, run `docker compose up` again so Docker recreates the container with the new mount points. Running `docker compose restart` doesn't apply changes to the `volumes` list.

If a local directory in the `volumes` list doesn't exist when Deephaven starts, Docker creates it.

## Using `docker create`

If you start Deephaven without [Docker Compose](https://docs.docker.com/compose/), add a `-v` flag to `docker create` for each mount point, then start the container with `docker start`. To map `/data` to your local `./data` directory, add `-v "$(pwd)/data:/data"`. Without that flag, the image's `/data` directory is a Docker-managed anonymous volume, and files written there don't appear in your local `./data` directory. The image's `/cache` directory is also an anonymous volume unless you map it. Docker-managed volumes are outside this guide.

For an example of `docker create` with an extra mount point, see [Add a second volume](../getting-started/docker-install.md#add-a-second-volume). That example omits the `/data` flag, so add `-v "$(pwd)/data:/data"` to it as well to keep the `/data` mapping.

## Related documentation

- [Read Parquet files into Deephaven tables](../how-to-guides/data-import-export/parquet-import.md)
- [Launch Deephaven from pre-built images](../getting-started/docker-install.md)
