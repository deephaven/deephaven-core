---
title: Use Deephaven with AWS EC2
---

[Amazon Web Services](https://aws.amazon.com/) (AWS) is the world's most popular cloud computing service. It offers a wide variety of cloud solutions, and these tend to pair well with Deephaven Community Core.

This guide shows you how to run Deephaven on an [AWS EC2](https://aws.amazon.com/ec2/) (Elastic Compute Cloud) instance, then connect to the user interface (UI) from your local machine and run queries.

## Create an AWS EC2 instance

This guide uses an EC2 instance running [Amazon Linux](https://aws.amazon.com/linux/amazon-linux-2023/). AWS offers a variety of other [operating systems](https://docs.aws.amazon.com/systems-manager/latest/userguide/prereqs-operating-systems.html) for cloud instances.

The instance type you need depends on your data and your calculations. There are many different [AWS EC2 instance types](https://aws.amazon.com/ec2/instance-types/) to choose from. Tables you create in Deephaven live on the JVM heap, so choose an instance with enough memory for the in-memory tables and results you expect. Tables read from Parquet files load on demand. You set the server's heap size when you install Deephaven. See [Install and run Deephaven](#install-and-run-deephaven).

Create an instance with the following steps.

1. Follow the steps in Amazon's [Set up to use Amazon EC2](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/get-set-up-for-amazon-ec2.html) guide.
2. Follow the steps in Amazon's [Get started with Amazon EC2 Linux instances](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/EC2_GetStarted.html) guide.

For more options, see Amazon's [EC2 tutorials](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-tutorials.html).

## Install and run Deephaven

We recommend [installing Deephaven with Docker](../getting-started/docker-install.md) on your EC2 instance.

Install and start Docker on the instance first. On Amazon Linux 2023, run:

```shell
sudo dnf install -y docker
sudo systemctl enable --now docker
sudo usermod -aG docker ec2-user
```

Log out and back in so the `docker` group applies to your user.

Docker needs no Java on the host. To set the heap size, see [Add more memory](../getting-started/docker-install.md#add-more-memory).

### Install Java to build from source

Install Java only if you plan to build Deephaven from source. [Building from source](../getting-started/launch-build.md) requires JDK 21. Install the Java Development Kit (JDK) as that guide describes. Amazon provides [Corretto](https://aws.amazon.com/corretto/), a free OpenJDK distribution. On Amazon Linux 2023, install Corretto 21 with:

```shell
sudo dnf install -y java-21-amazon-corretto-devel
```

## Connect to Deephaven

With Deephaven running on your AWS instance, all that's left to do is to connect to the user interface (UI) from your local machine:

1. In the EC2 console, add an inbound rule to the instance's security group that allows TCP traffic on port `10000` from your own IP address only. Port `10000` is Deephaven's default. If you changed the server's port, allow that port instead.
2. Find the instance's public IPv4 address or public DNS name in the EC2 console.
3. In your web browser, go to `http://<public-ip>:10000/ide`, replacing `<public-ip>` with that address or name.
4. Enter the [pre-shared key](./authentication/auth-psk.md) when prompted. Unless you set your own key, Deephaven generates one and prints it in the server log.

The `http://` address sends the key and your data over the internet unencrypted. To keep the connection private, close port `10000` in the security group and open an SSH tunnel from your local machine instead:

```shell
ssh -L 10000:localhost:10000 -i <key-file> ec2-user@<public-ip>
```

With the tunnel open, go to `http://localhost:10000/ide` in your browser.

Once logged in, you'll see your Deephaven UI, where you can create and run queries.

## Related documentation

- [Pre-shared key authentication](./authentication/auth-psk.md)
- [Docker installation guide](../getting-started/docker-install.md)
- [How to build from source](../getting-started/launch-build.md)
- [Deephaven production application](../getting-started/production-application.md)
