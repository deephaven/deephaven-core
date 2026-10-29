---
title: Use Deephaven in a GCP Linux instance
---

This guide shows you how to use the [Google Cloud Platform (GCP)](https://cloud.google.com/) to run [deephaven-core](https://github.com/deephaven/deephaven-core/) from [Docker](https://docker.com/). You launch a single VM instance, deploy the Deephaven server container to it, and connect to it from your local machine.

GCP is one of the most popular cloud computing services currently available. Its popularity isn't just for external users: it powers Google's own services such as [Gmail](https://mail.google.com/), [Google Drive](https://drive.google.com/), and even [YouTube](https://youtube.com/). So why not take advantage of it yourself?

## Get started

You need a Google Cloud account with billing enabled and a project to create the VM in. Note the project ID, because you use it in the SSH command later.

GCP has its own command line interface, the [gcloud CLI](https://cloud.google.com/cli). This guide uses it to SSH into the VM and open the tunnel, so install and configure it. Follow the instructions for your OS in Google's [installation guide](https://cloud.google.com/sdk/docs/install).

After you install the gcloud CLI, run the following command to give it authorized access to manage cloud services:

```bash
gcloud auth login
```

This opens your browser and prompts you to log in to your Google account.

## Create a VM instance

This guide creates the VM in the Google Cloud console. You can also create it with the gcloud CLI, as [Google's documentation](https://cloud.google.com/compute/docs/instances/create-start-instance) describes.

In the Google Cloud console, go to **Compute Engine** > **VM instances** and click **Create instance**.

### Name, region, and zone

Name the VM `dhc-how-to`, set the **Region** to `us-central1 (Iowa)`, and set the **Zone** to `us-central1-a`. See Google's [regions and zones documentation](https://cloud.google.com/compute/docs/regions-zones).

### Machine configuration

The **Machine configuration** options specify the hardware your cloud instance uses. Choices include the number of processor cores, the amount of memory, and an optional GPU. It's important to consider your needs, as the hardware configurations are critical to the success of your endeavors in the cloud.

This guide uses:

- Machine type: [`e2-standard-2`](https://cloud.google.com/blog/products/compute/google-compute-engine-gets-new-e2-vm-machine-types) (2 vCPUs, 8 GB memory)
- CPU platform: default
- GPU: none
- Display device: none

![The Machine configuration window](../assets/how-to/machine-config.png)

Another option is [confidential computing](https://cloud.google.com/confidential-computing), which encrypts your data while it's being processed. If you work with sensitive data, consider enabling it. This guide does _not_ enable confidential computing.

### OS and storage

Next, navigate to **OS and storage** on the left of the page, then click **Change**.

![The OS and storage window](../assets/how-to/os-and-storage.png)

Ignore the **Container** section on this screen. It is deprecated, and this guide runs the container from the startup script instead.

Container-Optimized OS is a Linux distribution Google built for running containers, and it comes with [Docker](https://docker.com/) pre-installed. To select it:

1. Choose **Container-Optimized OS** from the operating system list.
2. Select the newest stable version listed.
3. Click **Select** to confirm.

If you keep the default Debian image or choose Ubuntu, the startup script in the next section installs Docker on first boot.

### Choose a Docker image

Deephaven can be [launched from pre-built Docker images](../getting-started/quickstart.md). Images include:

- Python: `ghcr.io/deephaven/server:{VERSION}` (also published as `ghcr.io/deephaven/server-ui:{VERSION}`, an alias for the same image)
- Python with [NLTK](https://www.nltk.org): `ghcr.io/deephaven/server-nltk:{VERSION}`
- Python with [PyTorch](https://pytorch.org): `ghcr.io/deephaven/server-pytorch:{VERSION}`
- Python with [scikit-learn](https://scikit-learn.org): `ghcr.io/deephaven/server-sklearn:{VERSION}`
- Python with [TensorFlow](https://tensorflow.org): `ghcr.io/deephaven/server-tensorflow:{VERSION}`
- Python with all of the above: `ghcr.io/deephaven/server-all-ai:{VERSION}`
- Basic Groovy: `ghcr.io/deephaven/server-slim:{VERSION}`

> [!NOTE]
> `{VERSION}` in the list above is the Deephaven Core version number. See the [deephaven-core releases page](https://github.com/deephaven/deephaven-core/releases) for version numbers. Use the number without a leading `v`, for example `42.7`. `{VERSION}` can also be `latest`, which pulls the most recent release. Because the script pulls on every boot, `latest` can upgrade Deephaven when the VM restarts. Pin a specific version for reproducible deployments.

This guide uses the basic Groovy image `ghcr.io/deephaven/server-slim:latest`. If you choose a different image, set it as `CONTAINER_IMAGE` in the startup script in the next section.

### Startup script for Docker container

You can configure a Google Cloud VM to run Docker containers on startup with a script. This guide creates a startup script that pulls and runs the Deephaven container when the VM starts.

To configure the startup script:

1. Expand the **Advanced options** section at the bottom of the VM creation page.
2. Expand the **Management** section.
3. In the **Automation** section, find the **Startup script** text box.
4. Paste the following script, replacing `YOUR_PASSWORD_HERE` with your own password. You use this password to log in to the IDE. The password must contain only letters and digits, because the startup script's launcher parses `START_OPTS` with `xargs`, which strips quotes and backslashes and splits on spaces. Use a long, random password.

```bash
#!/bin/bash

# Install Docker if not already installed
if ! command -v docker &> /dev/null; then
    echo "Installing Docker..."
    curl -fsSL https://get.docker.com -o get-docker.sh
    sh get-docker.sh
    rm get-docker.sh
fi

# Container configuration
CONTAINER_NAME="deephaven-server"
CONTAINER_IMAGE="ghcr.io/deephaven/server-slim:latest"
START_OPTS="-Xmx4g -Dauthentication.psk=YOUR_PASSWORD_HERE"

# Stop and remove the container if it exists
docker stop $CONTAINER_NAME || true
docker rm $CONTAINER_NAME || true

# Pull the latest version of the container image
docker pull $CONTAINER_IMAGE

# Run the Deephaven container
docker run \
  --name=$CONTAINER_NAME \
  --restart=always \
  --detach \
  --publish 10000:10000 \
  --env START_OPTS="$START_OPTS" \
  $CONTAINER_IMAGE
```

This startup script does the following:

- Installs Docker if it isn't already present, using Docker's official installation script.
- Stops and removes an existing container named `deephaven-server`, which matters when the VM restarts.
- Pulls the latest Deephaven image.
- Runs the container with these options:
  - `--restart=always` restarts the container if it exits or when the Docker daemon starts.
  - `--publish 10000:10000` exposes the port the Deephaven IDE listens on.
  - `--env START_OPTS=...` passes JVM options to the server.
    - `-Xmx4g` sets the maximum JVM heap to 4 GB.
    - `-Dauthentication.psk` sets the password you use to log in. See [pre-shared key authentication](../how-to-guides/authentication/auth-psk.md).

> [!IMPORTANT]
> If you omit `-Dauthentication.psk`, the server generates a random key and prints it to the container log (`docker logs deephaven-server`).
>
> GCP stores the startup script in the VM's metadata, so anyone who can view the instance in your project can read the password in it.

> [!NOTE]
> If you choose a VM with more memory, increase the `-Xmx4g` value in the `START_OPTS` variable, leaving headroom for the operating system and non-heap memory. For example, use `-Xmx12g` on a 16 GB VM.

> [!NOTE]
> This guide does not cover persistent storage in the cloud. There are several options, including [Docker data volumes](../conceptual/docker-data-volumes.md) and [gcloud storage](https://cloud.google.com/sdk/gcloud/reference/storage), for workflows that require storage of large datasets.

### Remaining options

For the remaining options, this guide uses the defaults, including the access scopes and the firewall. You do not open port 10000 in the firewall, because you reach Deephaven through the SSH tunnel.

### Create the VM

After you've set the name, machine configuration, OS, and startup script, you can create the VM in either of two ways:

1. Click the **Create** button at the bottom of the page.
2. Click **Equivalent code** at the bottom of the page, then do one of the following:
   - Click **Copy** and run the command in your local terminal.
   - Click **Run in Cloud Shell** to run it in [Cloud Shell](https://cloud.google.com/shell/docs), a browser-based terminal with the gcloud CLI installed.

Google Cloud creates the VM and then runs the startup script, which pulls the Deephaven image. The container is not available until the startup script finishes pulling the image, which can take a few minutes after the VM shows as running. The next section shows how to check.

## Check that Deephaven is running

Connect to the VM over SSH to confirm the Deephaven container started. When the VM is ready, SSH into it with [`gcloud compute ssh`](https://cloud.google.com/sdk/gcloud/reference/compute/ssh), which uses the same syntax as [`ssh`](https://linuxcommand.org/lc3_man_pages/ssh1.html). The Google Cloud console shows the exact command for your VM.

First, head to your list of VMs.

![VM Instances highlighted in the Google Cloud Compute Engine menu](../assets/how-to/gcloud-vm-instances.jpg)

Then, find the VM you created in the list of VMs. Click the downward-facing arrow on the right-hand side.

![The VM `dhc-how-to` in the list of VMs, with the downward-facing arrow highlighted](../assets/how-to/gcloud-dhc-howto.jpg)

From the drop-down menu, click **View gcloud command**.

![dropdown menu with View gcloud command option selected](../assets/how-to/gcloud-ssh-dropdown.jpg)

This brings up a pop-up window where you can copy the command to SSH into your VM.

Copy that into your terminal to connect.

Run `docker container ls` to confirm the container is running. If the list is empty, wait a minute and run it again.

![terminal displaying docker container ls output showing running Deephaven container](../assets/how-to/gcloud-docker-ls-gr.png)

## Connect to Deephaven

Deephaven's IDE runs in a browser, so reach it from your local machine through an SSH tunnel. The tunnel forwards a port on the VM to a port on your machine. The **View gcloud command** pop-up shows the zone, instance name, and project ID the command needs. Type `exit` to leave the VM session, then run the tunnel command in a terminal on your local machine.

The command looks like this:

```bash
gcloud compute ssh --zone <ZONE> <INSTANCE> --project <PROJECT> -- -NL <LOCAL_PORT>:localhost:<HOST_PORT> &
```

- `<ZONE>` is the zone you created your VM in. This guide creates it in `us-central1-a`.
- `<INSTANCE>` is the name of the VM. This guide names it `dhc-how-to`.
- `<PROJECT>` is your Google Cloud project ID. It appears in the console URL as `?project=<PROJECT_ID>` and in the project selector.
- `-NL` does two things.
  - The first, `-N`, tells SSH not to run a remote command, so the session only forwards ports.
  - The second, `-L`, specifies the port forwarding between the local and remote hosts.
- `<LOCAL_PORT>` is the port on your local machine that you use to reach Deephaven.
  - Use a different port if a local Deephaven server already uses `10000`.
  - This guide uses `10000`.
- `<HOST_PORT>` is the port that Deephaven listens on in the VM. This guide uses `10000`.
- `&` runs the tunnel in the background so you can keep using the same terminal.

With the zone and instance name from this guide and `my-project` standing in for your own project ID, the command looks like this:

```bash
gcloud compute ssh --zone us-central1-a dhc-how-to --project my-project -- -NL 10000:localhost:10000 &
```

To keep the tunnel in its own terminal window or tab instead, omit the `&`.

With the tunnel running, open `localhost:10000/ide/` in your browser and enter the password you set in the startup script. The tunnel connects your browser to the Deephaven server on the Google Cloud VM, not to a local server.

![Deephaven IDE loaded in browser via Google Cloud VM tunnel](../assets/how-to/dhc-on-gcloud-gr.png)

## Clean up

1. Stop the background tunnel. Run `jobs` to find it, then run `kill %<JOB_NUMBER>`, replacing `<JOB_NUMBER>` with the number `jobs` shows. If you ran it in its own terminal window, press Ctrl+C.
2. Delete the VM so you are not billed for it. In the Google Cloud console, go to **Compute Engine** > **VM instances** and delete `dhc-how-to`. Or run:

   ```bash
   gcloud compute instances delete dhc-how-to --zone us-central1-a --project my-project
   ```

## Related documentation

- [Docker data volumes](../conceptual/docker-data-volumes.md)
- [Learn Deephaven](../getting-started/quickstart.md)
- [How to use Deephaven with AWS EC2](../how-to-guides/aws-ec2.md)
