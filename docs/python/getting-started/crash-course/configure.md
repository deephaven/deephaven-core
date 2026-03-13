---
title: Configure your Deephaven Instance
sidebar_label: Configure your Instance
---

This last section covers configuration details needed to take your Deephaven instance beyond the defaults.

## Authentication

Keeping your real-time data secure is one of Deephaven's top priorities. As such, the default installation of Deephaven is equipped with [pre-shared key (PSK) authentication](../../how-to-guides/authentication/auth-psk.md). By default, this key is randomly generated and available in the Docker logs (if using Docker) on startup:

![Docker logs showing the PSK](../../assets/how-to/9-steps/docker-logs.png)

It is advised that you change the password from a randomly generated string to your own password for enhanced security. For the [Docker one-liner installation](../../getting-started/docker-install.md), you can set your password with the `-Dauthentication.psk` flag:

```bash skip-test
docker run --rm --name deephaven -p 10000:10000 -v data:/data  --env START_OPTS=-Dauthentication.psk=YOUR_PASSWORD_HERE ghcr.io/deephaven/server:latest
```

If you're using pip-installed Deephaven, you can set the pre-shared key with the `jvm_args` parameter when starting a Deephaven server:

```python skip-test
from deephaven_server import Server

s = Server(jvm_args=["-Dauthentication.psk=YOUR_PASSWORD_HERE"])
s.start()
```

To learn more about PSK authentication, see the [user guide](../../how-to-guides/authentication/auth-psk.md) on the topic.

In addition to PSK authentication, Deephaven supports the following types of authentication:

- [Anonymous](../../how-to-guides/authentication/auth-anon.md)
- [Keycloak](../../how-to-guides/authentication/auth-keycloak.md)
- [mTLS](../../how-to-guides/authentication/auth-mtls.md)
- [Username / Password](../../how-to-guides/authentication/auth-uname-pw.md)

## Deployments

Deephaven provides [multiple docker images](../../getting-started/docker-install.md#choose-a-deployment). Each of these images, called deployments, comes pre-installed with different Python packages. You also have the option of including [Deephaven's example data](https://github.com/deephaven/examples) in any of these images. Many of these images integrate with popular Python libraries. For example, the [`server-all-ai`](https://github.com/deephaven/deephaven-core/pkgs/container/server-all-ai) image comes pre-installed with [PyTorch](https://pytorch.org), [Tensorflow](https://www.tensorflow.org), [scikit-learn](https://scikit-learn.org/stable/), and [nltk](https://www.nltk.org). You can use this image with this Docker command:

```bash skip-test
docker run --rm --name deephaven -p 10000:10000 ghcr.io/deephaven/server-all-ai:latest
```

To learn more about deployments, check out the guide for [installing Deephaven with Docker](../../getting-started/docker-install.md).

## Installing Python packages

Even with a standard deployment, you'll likely need to install new Python packages at some point. The process for installing a package is different for Docker-run Deephaven and pip-installed Deephaven.

If you're using Docker-installed Deephaven, you can install packages from within the Deephaven IDE:

```python skip-test
import os

os.system("pip install gpytorch")
```

Such installations will not persist after the Deephaven server has shut down. To learn about persistent package installations, see the [user guide on installing Python packages](../../how-to-guides/install-and-use-python-packages.md).

Package persistence will not be a problem if you're using pip-installed Deephaven. All packages present in the environment where the [`deephaven-server`](https://pypi.org/project/deephaven-server/) package is installed will be available in Deephaven. New packages are installed in the usual way:

```bash skip-test
(env) pip install gpytorch
```

## RAM

Large datasets require significant memory — often much more than the 4G that Deephaven allocates by default. Fortunately, it's easy to give Deephaven more memory, whether you have a Docker or a pip installation.

If you're using Docker-installed Deephaven, Docker itself imposes memory constraints on processes it runs - you can raise this ceiling in [Docker Desktop](https://docs.docker.com/desktop/settings-and-maintenance/settings/#resources) by going to `Settings > Resources` and raising the memory parameter. Then, you can specify the memory allocated to Deephaven with the `-Xmx` flag. Here's the command to pull and run the latest version of the Deephaven server with 16G of RAM:

```bash skip-test
docker run --rm --name deephaven -p 10000:10000 --env START_OPTS=-Xmx16g ghcr.io/deephaven/server:latest
```

The same argument can be used to raise the memory ceiling for pip-installed Deephaven:

```python skip-test
from deephaven_server import Server

s = Server(jvm_args=["-Xmx16g"])
s.start()
```

The [memory guide](../../how-to-guides/heap-size.md) explains more about allocating memory in Deephaven.

## Keep your instance up-to-date

The data world is ever-evolving, and so is Deephaven. It's easy to keep your instance up-to-date, taking advantage of the latest features and bug fixes.

If you're running Deephaven with Docker, simply use `docker pull` to pull the latest version of the image from the repository:

```bash
docker pull ghcr.io/deephaven/server:latest
```

Users with custom Docker installations should check out [this guide](../../how-to-guides/configuration/updating-deephaven.md#update-docker-installed-deephaven) for information on updating custom instances.

In the pip-installed Deephaven world, you can update the Deephaven Python package the same way you'd update any Python package:

```bash skip-test
pip install --upgrade deephaven-server
```
