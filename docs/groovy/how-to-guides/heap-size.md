---
title: Configure and adjust heap size
sidebar_label: Adjust heap size
---

When launching Deephaven, you may have very specific and perhaps large data needs that require fine-tuned resource management, such as adjusting your heap size (memory).

If you [launch Deephaven from pre-built images](../tutorials/docker-install.md) or [build and launch Deephaven from source code](../how-to-guides/launch-build.md), you use [Docker](https://www.docker.com/) to facilitate your containers and resource management. Depending on the applications and data you run, it may be necessary to change the amount of heap available to each of the containers in the system.

## See memory settings

To see your Docker configuration, run in a terminal:

```bash
docker info | grep Memory
```

By default, Docker on Mac is configured with 2 GB of RAM. If you need to increase the available memory, open Docker and navigate to **Preferences->Resources->Memory**.

> [!NOTE]
> Docker on Windows and Linux does not have this configuration option.

![Docker's Configuration panel](../assets/tutorials/launch/DockerConfigMac.png)

To get a better understanding of your resources, you can see the container statistics in a terminal by running the [Docker stats](https://docs.docker.com/engine/reference/commandline/stats/) built-in command:

```bash
docker stats
```

This shows the live memory usage of all active containers, as well as the limit specified on each container.

The column `MEM USAGE / LIMIT` includes information like `20.00MiB / 5.804GiB`, which means that container is using `20 MiB` of RAM out of the alloted `5.8 GiB` of RAM. When running your session, you can monitor this live feed to see which containers might need more memory.

## Change default heap size

The `JAVA_TOOL_OPTIONS=-Xmx4g` parameter in the `grpc-api` container `environment` controls the amount of memory available to Deephaven. By default, this is set to 4GB of Docker RAM.

To change this setting, nagivate to your Docker-Compose YAML file located in your Deephaven deployment directory. See our guide [Docker data volumes](../conceptual/docker-data-volumes.md) for more information.

The Docker-Compose YAML file your installation uses depends on how you launch Deephaven:

- If you [launch Deephaven from pre-built images](../tutorials/docker-install.md), your Docker Compose file is called `docker-compose.yml`.

- If you [build and launch Deephaven from source code](../how-to-guides/launch-build.md), your Docker Compose file is called `docker-compose-common.yml`.

Open the Docker-Compose YAML file with the text editor of your choice. You will see several containers listed separately under `services`. Each container can be fine-tuned.

![A docker-compose file](../assets/how-to/heap.png)

For instance, to use up to 8GB of RAM, change the first parameter to `JAVA_TOOL_OPTIONS=-Xmx8g`. When the `-Xmx` parameters are not set, the JVM sizes the heap based on the system specifications.

> [!NOTE]
> If you are using Docker Desktop, this value is also limited by what is specified in your Docker `Preferences->Resources->Memory` setting.

## Control individual containers

The Docker-Compose YAML file allows you to control each container. Its format depends based on your Docker version.

For example, in version 3.\*, you can gain more memory control under each service by supplying specific memory values:

```bash
deploy:
  resources:
    limits:
      cpus: '4.0'
      memory: 6000M
    reservations:
      memory: 1000M
```

In version 2.\*, the same changes to your `.yml` file would look like:

```bash
mem_limit: 6000m
mem_reservation: 1000M
cpus: 4.0
```

## Related documentation

- [Quick start](../tutorials/docker-install.md)
- [Build and launch Deephaven from source code](../how-to-guides/launch-build.md)
- [Access your file system with Docker data volumes](../conceptual/docker-data-volumes.md)
