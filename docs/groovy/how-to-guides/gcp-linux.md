---
id: gcp-linux
title: Use Deephaven in a GCP Linux instance
sidebar_label: GCP
---

This guide will show you how to use the [Google Cloud Platform (GCP)](https://cloud.google.com/) to run [deephaven-core](https://github.com/deephaven/deephaven-core/) from [Docker](https://docker.com/). It will show you how to launch a single VM instance, deploy the Deephaven server container to it, and then connect via your local machine.

GCP is one of the most popular cloud computing services currently available. Its popularity isn't just for external users - it powers Google's own services such as [Gmail](https://mail.google.com/), [Google Drive](https://drive.google.com/), and even [YouTube](https://youtube.com/). So why not take advantage of it yourself?

## Get started

GCP has its own [command line interface](https://cloud.google.com/cli) that is recommended for interacting with instances. For this guide, it is necessary to install and configure the CLI. Follow the instructions for your OS in Google's [installation guide](https://cloud.google.com/sdk/docs/install) to get started.

Once you've installed the gcloud CLI, run the following command to give it authorized access to manage cloud services:

```bash
gcloud auth login
```

This will bring up your browser and have you log in to your Google account.

## Create a VM instance

There are two ways you can create a GCP VM instance. The first is via your web browser. The second is via the gcloud CLI. Both are described [in Google's documentation](https://cloud.google.com/compute/docs/instances/create-start-instance). We will use the web interface.

### Name, region, and zone

We're going to name our VM `dhc-how-to`, set the `region` to `us-central1 (Iowa)`, and the `zone` to `us-central1-a`. More information on regions and zones can be found [here](https://cloud.google.com/compute/docs/regions-zones).

### Machine configuration

The `Machine configuration` options specify the hardware your cloud instance will use. Choices include processor cores, amount of memory, GPU, etc. It's important to consider your needs, as the hardware configurations are critical to the success of your endeavors in the cloud.

For this demo, we will be using:

- [`e2-standard-2`](https://cloud.google.com/blog/products/compute/google-compute-engine-gets-new-e2-vm-machine-types) machine type
  - 2 vCPUs
  - 8 GB memory
  - The default CPU type
  - No GPU
  - No display device

![The **Machine configuration** window](../assets/how-to/machine-config.png)

Another option is [confidential computing](https://cloud.google.com/confidential-computing), which encrypts your data while it's being processed. If you are working with sensitive data, this option is likely important to enable. For this guide, we will _not_ be enabling confidential computing.

### OS and storage

Next, navigate to **OS and storage** on the left of the page, then click **Change**.

![The **OS and storage** window](../assets/how-to/os-and-storage.png)

For the operating system, select **Container-Optimized OS**, which is a Linux distribution created by Google that comes with Docker pre-installed and is optimized for running containers:

- Look for the **Boot disk** section and click **CHANGE**
- Select **Container-Optimized OS** from the operating system list
- Select the latest stable version
- Click **SELECT** to confirm

Alternatively, you can use other Linux distributions like Ubuntu or Debian. The startup script will automatically install Docker if it's not already present.

### Startup script for Docker container

A Google Cloud VM can be configured to run Docker containers on startup using a startup script. Since Deephaven can be [launched from pre-built Docker images](../tutorials/quickstart.md), we'll create a startup script that automatically pulls and runs the Deephaven container when the VM starts.

Deephaven has several pre-built Docker images to choose from. Your choice should depend on your needs.

:::note

`{VERSION}` in the list below is the Deephaven Core version number. Version numbers can be found [here](https://github.com/deephaven/deephaven-core/releases). Additionally, `{VERSION}` can be `latest`, which will always pull the latest version number.

:::

- Basic Python: `ghcr.io/deephaven/server:{VERSION}`
- Python with [NLTK](https://www.nltk.org): `ghcr.io/deephaven/server-nltk:{VERSION}`
- Python with [PyTorch](https://pytorch.org): `ghcr.io/deephaven/server-pytorch:{VERSION}`
- Python with [SciKit-Learn](https://scikit-learn.org): `ghcr.io/deephaven/server-sklearn:{VERSION}`
- Python with [TensorFlow](https://tensorflow.org): `ghcr.io/deephaven/server-tensorflow:{VERSION}`
- Python with all of the above: `ghcr.io/deephaven/server-all-ai:{VERSION}`
- Basic Groovy: `ghcr.io/deephaven/server-slim:{VERSION}`

For this guide, we'll use the basic Groovy image `ghcr.io/deephaven/server-slim:latest`.

To configure the startup script:

1. Expand the **Advanced options** section at the bottom of the VM creation page
2. Expand the **Management** section
3. In the **Automation** section, find the **Startup script** text box
4. Paste the following script:

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
START_OPTS="-Xmx4g"

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

This startup script will:

- Install Docker if it's not already present (using Docker's official installation script)
- Stop and remove any existing Deephaven container (useful for VM restarts)
- Pull the latest Deephaven image
- Run the container with:
  - `--restart=always`: Automatically restart if the container crashes
  - `--publish 10000:10000`: Expose Deephaven's web UI port
  - `--env START_OPTS="-Xmx4g"`: Configure Deephaven to use 4GB of memory (adjust based on your VM's memory)

:::note

If you choose a VM with more memory, increase the `-Xmx4g` value in the `START_OPTS` variable to whatever amount suits your needs. For example, use `-Xmx8g` for 8GB of memory.

:::

### Remaining options

For the remaining options, we will use the defaults. These include `Allow default access` and `Firewall`. Take some time to review each option and ensure the default options (or otherwise) are right for your needs.

:::note

This guide will not cover persistent storage in the cloud. There are several options, including [Docker data volumes](../conceptual/docker-data-volumes.md) and [gcloud storage](https://cloud.google.com/sdk/gcloud/reference/storage), for workflows that require storage of large datasets.

:::

### Create the VM

Once that's done, there are two ways to create the VM:

1. Click the **CREATE** button at the bottom of the page.
2. Click the **EQUIVALENT CODE** button at the bottom of the page, then either click **COPY** and run it in your local terminal, or click **RUN IN CLOUD SHELL**.

With all that done, you've got a Google VM running Deephaven!

## SSH into your VM

Google Cloud takes a bit of time to create the VM. Once that's done, you can SSH into it via [`gcloud compute ssh`](https://cloud.google.com/sdk/gcloud/reference/compute/ssh). It uses very similar syntax to [`ssh`](https://linuxcommand.org/lc3_man_pages/ssh1.html). One of the nice things about the gcloud CLI is that you can find relevant commands in the Google Cloud web interface.

First, head to your list of VMs.

![**VM Instances** highlighted in the Google Cloud Compute Engine menu](../assets/how-to/gcloud-vm-instances.jpg)

Then, find the VM you created in the list of VMs. Click the downward-facing arrow on the right-hand side.

![The newly created VM in the list of VMs, with the downward-facing arrow highlighted](../assets/how-to/gcloud-dhc-howto.jpg)

From the drop-down menu, click **View gcloud command**.

![dropdown menu with View gcloud command option selected](../assets/how-to/gcloud-ssh-dropdown.jpg)

This brings up a pop-up window where you can copy the command to SSH into your VM.

Copy that into your terminal, and you'll be there!

It's always a good idea to ensure your container is up and running via `docker container ls`.

![terminal displaying docker container ls output showing running Deephaven container](../assets/how-to/gcloud-docker-ls-gr.png)

## Connect to Deephaven

In the previous section, you connected to your VM to ensure that Deephaven is running, but didn't actually do anything with it. Deephaven uses a GUI, and you'll need access to the GUI in the cloud on your local machine. Thus, we need to take the previous section one step further and enable that GUI. The [gcloud CLI](https://cloud.google.com/sdk/gcloud) has you covered.

This time around, we will create an SSH tunnel with port forwarding. Deephaven runs on a specific port, so if we forward that port on the VM to the local machine, we'll have access to the GUI. The command looks like this:

```bash
gcloud compute ssh --zone <ZONE> <INSTANCE> --project <PROJECT> -- -NL <LOCAL_PORT>:localhost:<HOST_PORT> &
```

- `<ZONE>` is the zone you created your VM in. For this guide, we created it in `us-central1-a`.
- `<INSTANCE>` is the name of the VM. For this guide, we called it `dhc-how-to`.
- `<PROJECT>` is the name of your gcloud project. This can be found in the URL when you go to the web interface at the end. It looks like `?project=<PROJECT_NAME>`.
- `-NL` does two things.
  - The first, `-N`, enables port forwarding. It's consistent with `ssh`.
  - The second, `-L`, specifies the port forwarding between the local and remote hosts.
- `<LOCAL_PORT>` is whatever port you'd like to connect to Deephaven on locally. Deephaven is typically run on `10000`, although it may be useful to specify a different port here if you're running a local Deephaven server on that port. For this case, we use `10000`, since we're not running any local Deephaven servers on that port.
- `<HOST_PORT>` is the port on the VM which you can connect to Deephaven from. In this case, it's `10000`.
- `&` prevents the terminal from blocking. It allows you to continue using your terminal. It's not necessary unless you don't want to create any new terminal windows or tabs for your work. You should manually kill this process when you're done with it.

With all of the configuration options we chose, the command looks like this:

```bash
gcloud compute ssh --zone us-central1-a dhc-how-to --project deephaven-oss -- -NL 10000:localhost:10000 &
```

With that run, head to your web browser of choice and go to `localhost:10000/ide/`. The Deephaven running isn't running locally, but on a Google Cloud VM! Pretty cool.

![Deephaven IDE loaded in browser via Google Cloud VM tunnel](../assets/how-to/dhc-on-gcloud-gr.png)

## Related documentation

- [Docker install guide](../tutorials/docker-install.md)
- [Learn Deephaven](../tutorials/quickstart.md)
- [How to use Deephaven with AWS EC2](../how-to-guides/aws-ec2.md)
- [Docker data volumes](../conceptual/docker-data-volumes.md)
