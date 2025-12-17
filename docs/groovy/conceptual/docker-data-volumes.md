---
title: Access your file system with Docker data volumes
sidebar_label: Docker data volumes
---

This guide discusses accessing your file system within your Deephaven Docker container using [data volumes](https://docs.docker.com/storage/volumes/). Before we dive into the mount points, let's cover the basics of Docker and how Deephaven uses it.

## What is Docker?

Docker is an open platform for developing, shipping, and running applications. In Docker, applications are packaged into containers -- standardized executable components containing application code, operating system libraries, and dependencies required to run the application. Docker containers can be run on any operating system that supports Docker.

Deephaven is one of many applications that leverages Docker for build and deployment. Each Deephaven deployment is composed of multiple containers. These containers are managed by [Docker Compose](https://docs.docker.com/compose/). A YAML (`.yml`) file configures a Docker Compose deployment.

By design, Docker containers do not store persistent data. To persist data, storage volumes are mounted into the file system of a running container. For example, directory `./abc` on your local file system may be mounted as `/xyz` volume in a running container. For a Deephaven deployment, these volumes are configured in the Docker Compose YAML file.

## The `/data` mount point

By default, all Deephaven deployments mount `./data` in the local deployment directory to the `/data` volume in the running Deephaven container. This means that if the Deephaven console is used to write data to `/data/abc/file.csv`, that file will be visible at `./data/abc/file.csv` on the local file system of your computer. Similarly, if the local file `abc.parquet` is copied to `./data/abc.parquet`, then the file can be accessed at `/data/abc.parquet` on the Deephaven server.

If the `./data` directory does not exist when Deephaven is launched, it will be created.

## Modify mount points

Your Docker-Compose YAML file is located in your Deephaven deployment directory and contains all of the information needed to launch Deephaven.

The Docker-Compose YAML file your installation uses depends on how you launch Deephaven:

- If you [launch Deephaven from pre-built images](../tutorials/docker-install.md), your Docker Compose file is called `docker-compose.yml`.

- If you [build and launch Deephaven from source code](../how-to-guides/launch-build.md), your Docker Compose file is called `docker-compose-common.yml`.

The `/data` mount point is configured using the `volumes` keyword in the Docker Compose YAML file.

```
volumes:
  - ./data:/data
```

This mounts the `./data` directory on your file system as `/data` in Deephaven. If you decide to change the mount point, make the change consistently for all services in the deployment.

## Add mount points

While `/data` is the default place for external data to reside, you have the option of creating other mount points. To create a mount point, you must add the volumes to the `grpc-api` and `web` services in your Docker-Compose YAML file.

Say we're working on a machine learning application and want separate volumes to store training, testing, and validation data sets. These volumes can be added by modifying the `volumes` section of the Docker Compose YAML file for both the `grpc-api` and `web` services:

```
volumes:
  - ./data:/data
  - ./ML_Train:/ML_Train
  - ./ML_Test:/ML_Test
  - ./ML_Validate:/ML_Validate
```

When Deephaven is re-launched, the new volumes will be available.

## Related documentation

- [Launch Deephaven from pre-built images](../tutorials/docker-install.md)
- [Launch Deephaven from source code](../how-to-guides/launch-build.md)
