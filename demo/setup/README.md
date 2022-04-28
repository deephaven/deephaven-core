# Deephaven Cluster Setup

Currently the Deephaven demo system runs on Google Kubernetes Engine and gcloud.

We use kubernetes to manage our certificates,  
and eventually will provide kubernetes-based controller/workers,  
but currently provision machines directly using gcloud.

This setup document will capture all of the necessary steps  
to properly configure your google project  
in order to run the Deephaven demo controller.

If you are already have a running demo system  
and are just trying to **deploy a worker or controller**  
DO NOT FOLLOW THIS DOCUMENT;  
instead see the [Deployment Guide](../deploy/README.md).

To simplify example scripts, make sure to set up your terminal environment with the following variables (customized to your target cluster / google project).

```shell
PROJECT_ID="${PROJECT_ID:-deephaven-community}"
REGION="${ZONE:-us-central1}"
CLUSTER_NAME="${CLUSTER_NAME:-dh-demo}"
```

## Creating Google Project

The first step is to create a google project  
and setup the [gcloud](https://cloud.google.com/sdk/docs/install) program.

If you have not already done so, setup a google project: [https://console.cloud.google.com/projectcreate](https://console.cloud.google.com/projectcreate).

For the purposes of this guide, we will use the name (and ID) of `deephaven-community`.  
Make sure to use "advanced" gui settings so you don't get a project ID ending in random numbers.

Once your project is created,  
obtain credentials to operate on your project:  

```shell
PROJECT_ID="${PROJECT_ID:-deephaven-community}"
gcloud auth login --project "$PROJECT_ID"
```


## Creating GKE Cluster

Our cluster uses the google project ID `deephaven-community`  
and a kubernetes cluster name of `dh-demo`.  
This cluster is an autopilot cluster,  
to simplify setup and improve security  
(no root allowed in containers!).


Make sure to enable the kubernetes API by visiting [https://console.cloud.google.com/kubernetes]. This will require enabling billing for your project (you must supply payment information). For small deployments like a single worker, the total cost aside from the hardware you give to the worker will be negligible.



...

## Setting up Service Accounts

We use a number of gcloud service accounts, all with varying permissions.

* Workers run with the least amount of privileges, using the `dh-worker` service account.  
* Controllers run with enough privileges to create workers, using the `dh-control` service account.  
* Administrator users deploying workers and clusters require more extensive privileges, and utilize both the dh-control and `dh-admin` service accounts.  
* Automated certificate provisioning (optional) requires a semi-privileged `cert-issuer` service account, which is linked to a kubernetes service account.

These service accounts are all backed by gcloud IAM roles with (nearly*) minimal permissions granted. * There may be _some_ excessive permissions that could be removed with careful trial and error, but the amount of permission granted is far, far less than simply granting Owner/Editor status to users.

...

## Create a Certificate

...