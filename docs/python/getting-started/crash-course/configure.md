---
title: Configure your Deephaven instance
---

This page shows how to take your Deephaven instance beyond the defaults: secure it, choose a different image, add Python packages, give it more memory, and keep it up to date. The Docker examples build on the command you ran in [Get started](./get-started.md). If you installed with pip, the last section shows how to do the same tasks.

## Secure your instance

Keeping your real-time data secure is one of Deephaven's top priorities. By default, Deephaven uses [pre-shared key (PSK) authentication](../../how-to-guides/authentication/auth-psk.md). In [Get started](./get-started.md), you set the key with the `-Dauthentication.psk` flag in the `docker run` command.

- With the flag, your key stays the same across server restarts.
- Without the flag, Deephaven generates a new random key each time the server starts.

Choose a long, hard-to-guess key rather than the placeholder `YOUR_PASSWORD_HERE`.

In both cases, Deephaven prints the key in the server logs on startup, so you can find it there if you forget it. With the Get started command, the logs print in the terminal where you ran `docker run`. The key appears in a line like this one from a Docker Compose log:

![Docker Compose logs showing the PSK](../../assets/how-to/9-steps/docker-logs.png)

To learn more about PSK authentication, see the [PSK authentication guide](../../how-to-guides/authentication/auth-psk.md).

In addition to PSK authentication, Deephaven supports the following types of authentication:

- [Anonymous](../../how-to-guides/authentication/auth-anon.md)
- [Keycloak](../../how-to-guides/authentication/auth-keycloak.md)
- [mTLS](../../how-to-guides/authentication/auth-mtls.md)
- [Username/password](../../how-to-guides/authentication/auth-uname-pw.md)

## Choose an image

Deephaven provides [multiple Docker images](../../getting-started/docker-install.md#choose-a-deployment), called deployments. Each image is available in several [versions](../../getting-started/docker-install.md#image-versions), such as `latest` or a specific release.

The images that start a Python console, such as the `server` image you ran in [Get started](./get-started.md), come pre-installed with different Python packages. For example, the [`server-all-ai`](https://github.com/deephaven/deephaven-core/pkgs/container/server-all-ai) image comes pre-installed with [PyTorch](https://pytorch.org), [TensorFlow](https://www.tensorflow.org), [scikit-learn](https://scikit-learn.org/stable/), and [NLTK](https://www.nltk.org). To use `server-all-ai`, swap the image name in your Get started command:

```bash skip-test
docker run --rm --name deephaven -p 10000:10000 -v "$(pwd)/data:/data" --env START_OPTS=-Dauthentication.psk=YOUR_PASSWORD_HERE ghcr.io/deephaven/server-all-ai:latest
```

Unlike `server`, `server-all-ai` doesn't include the [Deephaven Express](/core/plotly/docs/) and [`deephaven.ui`](/core/ui/docs/) plugins that the [Real-time Plots](./plots.md) and [deephaven.ui](./deephaven-ui.md) pages of this crash course use. Run `server` if you want to run those examples. Deephaven loads plugins when the server starts, so installing them from the IDE doesn't make them available. To add them to `server-all-ai`, build a [custom Docker image](../../how-to-guides/install-and-use-python-packages.md#permanently-add-packages-to-a-custom-docker-image) that starts from `server-all-ai`:

```docker skip-test
FROM ghcr.io/deephaven/server-all-ai:latest
RUN pip install deephaven-plugin-plotly-express deephaven-plugin-ui
```

You can also run Deephaven with [Deephaven's example data](https://github.com/deephaven/examples), [Redpanda](https://redpanda.com/), or both. Deephaven publishes pre-built `docker-compose.yml` files that do this. To pick one and start it, see [Choose a deployment](../../getting-started/docker-install.md#choose-a-deployment).

To learn more about deployments, check out the guide for [installing Deephaven with Docker](../../getting-started/docker-install.md).

## Install Python packages

Whichever image you run, you'll likely need to install Python packages it doesn't include at some point. You can install packages from within the Deephaven IDE:

```python skip-test
import os

os.system("pip install gpytorch")
```

The commands on this page start the container with `--rm`, so when you stop the container, Docker removes it along with any packages you installed. To learn about persistent package installations, see the [user guide on installing Python packages](../../how-to-guides/install-and-use-python-packages.md).

## Give Deephaven more memory

Large datasets require significant memory. Deephaven runs on the Java Virtual Machine (JVM), and the JVM's maximum heap size caps how much memory Deephaven can use for data. You set it with the `-Xmx` flag. If you don't, the JVM picks a size based on the memory available, which is often too small for large data.

Fortunately, it's easy to give Deephaven more memory:

1. On Docker Desktop, raise the memory limit under **Settings > Resources > Advanced**, as described in the [Docker Desktop settings documentation](https://docs.docker.com/desktop/settings-and-maintenance/settings/#resources). Containers can use only as much memory as Docker Desktop allows, which by default is half of your machine's memory.
2. Add the `-Xmx` flag to the `START_OPTS` environment variable you used in [Get started](./get-started.md). Separate multiple JVM options with spaces. The new `START_OPTS` value replaces the old one, so include your pre-shared key in it too.

Here's the Get started command with a 16 GB maximum heap added:

```bash skip-test
docker run --rm --name deephaven -p 10000:10000 -v "$(pwd)/data:/data" --env START_OPTS="-Xmx16g -Dauthentication.psk=YOUR_PASSWORD_HERE" ghcr.io/deephaven/server:latest
```

The [memory guide](../../how-to-guides/heap-size.md) explains more about allocating memory in Deephaven.

## Keep your instance up to date

The data world is ever-evolving, and so is Deephaven. It's easy to keep your instance up to date and get the latest features and bug fixes.

To update, pull the latest version of the image you run. For the [Get started](./get-started.md) image, that's:

```bash
docker pull ghcr.io/deephaven/server:latest
```

Then stop your running container and start a new one with the same `docker run` command you used in [Get started](./get-started.md). To stop the container, press <kbd>Ctrl+C</kbd> in its terminal or run `docker stop deephaven`. Because the Get started command mounts `./data`, your saved files carry over.

If you run Deephaven from a `docker-compose.yml` file or your own `Dockerfile`, see the [guide to updating Docker-installed Deephaven](../../how-to-guides/configuration/updating-deephaven.md#update-docker-installed-deephaven) to set the image version there.

## If you installed Deephaven with pip

This crash course runs Deephaven with Docker. If you [installed Deephaven with pip](../../getting-started/pip-install.md) instead, the steps above work differently.

Set the pre-shared key and the maximum heap size with the `jvm_args` parameter when you start the Deephaven server:

```python skip-test
from deephaven_server import Server

s = Server(jvm_args=["-Dauthentication.psk=YOUR_PASSWORD_HERE", "-Xmx16g"])
s.start()
```

Deephaven can import every package in the Python environment where you installed [`deephaven-server`](https://pypi.org/project/deephaven-server/), so packages you install there persist across server restarts. To add a package, run `pip install` in that environment:

```bash skip-test
pip install gpytorch
```

Update the Deephaven Python package the same way you'd update any Python package:

```bash skip-test
pip install --upgrade deephaven-server
```
