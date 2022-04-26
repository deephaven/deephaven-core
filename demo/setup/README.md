# Deephaven Cluster Setup

Currently the Deephaven demo system runs on Google Kubernetes Engine and gcloud.

We use kubernetes to manage our certificates, and eventually will provide kubernetes-based controller/workers, but the current implementation uses gcloud service accounts to directly manage a cluster of machines.

This setup document will capture all of the steps necessary to properly configure a google project in order to run the Deephaven demo controller.

If you are just trying to deploy a worker or conroller to an existing demo systems, DO NOT FOLLOW THIS DOCUMENT; instead see [../deploy/README.md].

## Creating GKE Cluster

Our current cluster uses the google project name `deephaven-oss` and a cluster name of `dhce-auto`.  This cluster is an autopilot cluster, to simplify setup and improve security (no root allowed in containers!).

If you have not already done so, enable gcloud and setup a project via [https://console.cloud.google.com/projectcreate].

For the purposes of this guide, we will use the name (and ID) of `deephaven-community`. Make sure to use "advanced" gui settings so you don't get a project ID ending in random numbers.

Make sure to enable the kubernetes API by visiting [https://console.cloud.google.com/kubernetes]. This will require enabling billing for your project (you must supply payment information). For small deployments like a single worker, the total cost aside from the hardware you give to the worker will be negligible.



...

## Setting up Service Accounts

We use a number of gcloud service accounts, all with varying permissions.

Workers run with the least amount of privileges, using the `dh-worker` service account.  
Controllers run with enough privileges to create workers, using the `dh-control` service account.  
Administrator users deploying workers and clusters require more extensive privileges, and utilize both the dh-control and `dh-admin` service accounts.  
Automated certificate provisioning (optional) requires a semi-privileged `cert-issuer` service account, which is linked to a kubernetes service account.

These service accounts are all backed by gcloud IAM roles with (nearly*) minimal permissions granted. * There may be _some_ excessive permissions that could be removed with careful trial and error, but the amount of permission granted is far, far less than simply granting Owner/Editor status to users.

...

## Create a Certificate

...