---
title: Use Deephaven with AWS EC2
sidebar_label: AWS EC2
---

[Amazon Web Services](https://aws.amazon.com/) (AWS) is the world's most popular cloud computing service. It offers a wide variety of cloud solutions - these tend to pair well with Deephaven Community Core.

This guide will show you how to run Deephaven on an [AWS EC2](https://aws.amazon.com/ec2/) (Elastic Compute Cloud) instance, then connect to the UI from your local machine and run queries.

## Create an AWS EC2 instance

The type of AWS EC2 instance you will need to make will depend entirely on what operating system you prefer, what kind of data you want to work with, and the nature of the calculations you wish to execute. There are many different [AWS EC2 instance types](https://aws.amazon.com/ec2/instance-types/) to choose from.

AWS offers a variety of [operating systems](https://docs.aws.amazon.com/systems-manager/latest/userguide/prereqs-operating-systems.html) for cloud instances. For this blog, we will create an AWS EC2 instance with [AWS-Linux](https://aws.amazon.com/amazon-linux-2/?amazon-linux-whats-new.sort-by=item.additionalFields.postDateTime&amazon-linux-whats-new.sort-order=desc) as the OS with the following steps.

1. Follow the steps in Amazon's [Set up to use Amazon EC2](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/get-set-up-for-amazon-ec2.html) guide.
2. Follow the steps in Amazon's [Get started with Amazon EC2 Linux instances](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/EC2_GetStarted.html) guide.
3. Read any of Amazon's other [tutorials](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-tutorials.html) that fit your needs.

## Install and run Deephaven

We recommend either of the following to install and run Deephaven from the cloud:

- [From pre-built Docker images](../getting-started/quickstart.md)
- [From pip](../getting-started/pip-install.md)

Follow the instructions in the linked guides above to install Deephaven on your AWS EC2 instance.

AWS EC2 2022 Linux instances come with [amazon-linux-extras](https://aws.amazon.com/premiumsupport/knowledge-center/ec2-install-extras-library-software/) installed. Java 11 (required by pip and source builds) can be easily installed on an AWS Linux instance with this command.

```shell
sudo amazon-linux-extras install java-openjdk11
```

If `amazon-linux-extras` is not installed on your instance, you can install it with `yum`.

```shell
sudo yum install -y amazon-linux-extras
```

A full list of software that can be easily installed with `amazon-linux-extras` can be found [here](https://aws.amazon.com/premiumsupport/knowledge-center/ec2-install-extras-library-software/).

> [!NOTE]
> Amazon also offers their own OpenJDK distribution, [Corretto](https://aws.amazon.com/corretto/?filtered-posts.sort-by=item.additionalFields.createdDate&filtered-posts.sort-order=desc). It works well on AWS, and can be used in place of Oracle's OpenJDK. Corretto user guides can be found [here](https://docs.aws.amazon.com/corretto/index.html). For this guide, we'll [install Corretto 11 on AWS Linux 2](https://docs.aws.amazon.com/corretto/latest/corretto-11-ug/amazon-linux-install.html).

```shell
sudo yum install java-11-amazon-corretto
```

## Connect to Deephaven

With Deephaven running on your AWS instance, all that's left to do is to connect to the user interface (UI) via your local machine. For that, you'll need the IP or hostname of your AWS EC2 instance. For example, let's say that the IP of your instance is `172.16.1.1`, and that you're running Deephaven on the standard port `10000`. In your web browser, go to `http://172.16.1.1:10000/ide`, and you'll see your Deephaven UI!

## Related documentation

- [Docker quickstart](../getting-started/docker-install.md)
- [Pip quickstart](../getting-started/pip-install.md)
- [How to build from source](../getting-started/launch-build.md)
- [Deephaven production application](../getting-started/production-application.md)

<!-- - [How to use AWS ECS with Deephaven] -->

<!-- TODO: address network security for AWS -->
