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
PROJECT_ID="${PROJECT_ID:-deephaven-oss}"
REGION="${ZONE:-us-central1}"
CLUSTER_NAME="${CLUSTER_NAME:-dhce-auto}"
```

## Creating Google Project

The first step is to create a google project  
and setup the [gcloud](https://cloud.google.com/sdk/docs/install) program.

If you have not already done so, setup a google project: [https://console.cloud.google.com/projectcreate](https://console.cloud.google.com/projectcreate).

For the purposes of this guide, we will use the name (and ID) of `deephaven-oss`.  
Make sure to use "advanced" gui settings so you don't get a project ID ending in random numbers.

Once your project is created,  
obtain credentials to operate on your project:  

```shell
PROJECT_ID="${PROJECT_ID:-deephaven-oss}"
gcloud auth login --project "$PROJECT_ID"
```


## Creating GKE Cluster

Our cluster uses the google project ID `deephaven-oss`  
and a kubernetes cluster name of `dhce-auto`.  
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

The following list is NOT the ideal, short list of permissions,  
and we _should_ reduce this down to a smaller subset with more testing  
but, for now, we will simply list our custom roles and service accounts. 

## Custom Roles

### Demo Worker Role
* artifactregistry.repositories.downloadArtifacts

### Demo Controller Role
* artifactregistry.repositories.downloadArtifacts
* compute.addresses.create
* compute.addresses.createInternal
* compute.addresses.delete
* compute.addresses.deleteInternal
* compute.addresses.get
* compute.addresses.list
* compute.addresses.setLabels
* compute.addresses.use
* compute.addresses.useInternal
* compute.disks.create
* compute.disks.createSnapshot
* compute.disks.delete
* compute.disks.get
* compute.disks.list
* compute.disks.resize
* compute.disks.setLabels
* compute.disks.update
* compute.disks.use
* compute.disks.useReadOnly
* compute.globalAddresses.create
* compute.globalAddresses.createInternal
* compute.globalAddresses.delete
* compute.globalAddresses.deleteInternal
* compute.globalAddresses.get
* compute.globalAddresses.list
* compute.globalAddresses.setLabels
* compute.globalAddresses.use
* compute.images.create
* compute.images.delete
* compute.images.get
* compute.images.list
* compute.images.update
* compute.instances.attachDisk
* compute.instances.create
* compute.instances.delete
* compute.instances.deleteAccessConfig
* compute.instances.detachDisk
* compute.instances.get
* compute.instances.getIamPolicy
* compute.instances.list
* compute.instances.reset
* compute.instances.resume
* compute.instances.setLabels
* compute.instances.setMachineType
* compute.instances.setMetadata
* compute.instances.setTags
* compute.instances.start
* compute.instances.stop
* compute.instances.suspend
* compute.instances.update
* compute.instances.use
* compute.instances.useReadOnly
* compute.machineTypes.get
* compute.machineTypes.list
* compute.networks.access
* compute.networks.get
* compute.networks.getEffectiveFirewalls
* compute.networks.list
* compute.networks.update
* compute.networks.use
* compute.projects.get
* compute.regionOperations.get
* compute.regions.get
* compute.regions.list
* compute.securityPolicies.get
* compute.securityPolicies.getIamPolicy
* compute.securityPolicies.list
* compute.snapshots.create
* compute.snapshots.delete
* compute.snapshots.get
* compute.snapshots.getIamPolicy
* compute.snapshots.list
* compute.snapshots.setLabels
* compute.snapshots.useReadOnly
* container.clusters.get
* container.clusters.getCredentials
* container.secrets.get
* container.secrets.update
* dns.changes.create
* dns.changes.get
* dns.managedZones.list
* dns.resourceRecordSets.create
* dns.resourceRecordSets.delete
* dns.resourceRecordSets.list
* dns.resourceRecordSets.update
* iam.serviceAccounts.actAs
* iam.serviceAccounts.get
* iam.serviceAccounts.list

### Demo Admin Role
* artifactregistry.dockerimages.get
* artifactregistry.dockerimages.list
* artifactregistry.files.get
* artifactregistry.files.list
* artifactregistry.packages.get
* artifactregistry.packages.list
* artifactregistry.repositories.downloadArtifacts
* artifactregistry.repositories.get
* artifactregistry.repositories.getIamPolicy
* artifactregistry.repositories.list
* artifactregistry.repositories.update
* artifactregistry.repositories.uploadArtifacts
* artifactregistry.tags.get
* artifactregistry.tags.list
* artifactregistry.versions.get
* artifactregistry.versions.list
* compute.globalOperations.get
* compute.images.create
* compute.images.delete
* compute.images.get
* compute.images.list
* compute.images.setLabels
* compute.images.useReadOnly
* compute.instances.setServiceAccount
* compute.subnetworks.get
* compute.subnetworks.list
* compute.subnetworks.use
* compute.subnetworks.useExternalIp
* compute.zones.get
* compute.zones.list
* dns.changes.create
* dns.changes.get
* dns.managedZoneOperations.get
* dns.managedZoneOperations.list
* dns.managedZones.get
* dns.managedZones.list
* dns.resourceRecordSets.create
* dns.resourceRecordSets.delete
* dns.resourceRecordSets.get
* dns.resourceRecordSets.list
* dns.resourceRecordSets.update

## Service Accounts

### dh-worker
* Custom Roles
  * Demo Worker Role

### dh-controller
* Stock Roles 
  * Compute Admin
  * Compute Viewer
  * DNS Administrator
* Custom Roles
  * Demo Admin Role
  * Demo Controller Role

### dhadmin
* Stock Roles
  * Compute Security Admin
  * DNS Administrator

### dhce-demo@deephaven-oss.iam.gserviceaccount.com
* Stock Roles
  * Kubernetes Engine Host Service Agent User

In the future, we will provide automated scripts  
for creating the necessary roles and service accounts.

## Create a Certificate

To update the certificates, we must do two things:  
1) ./gradlew updateCerts
2) Create a new base image
   1) Can be done in two ways:
   2) Performing a minor version upgrade, like 0.10.0 -> 0.11.0 will make a new base image
   3) Deleting the existing image and rerunning ./gradlew deployDemo
      1) The images can be found at [https://console.cloud.google.com/compute/images?tab=images&project=deephaven-oss](https://console.cloud.google.com/compute/images?tab=images&project=deephaven-oss)
      
Once a new certificate is created,  
you will need to run ./gradlew deployDemo to update production.

See the [Deployment Guide](../deploy/README.md) for details.

TODO: fill in with certificate setup instructions

## Maintenance

Some resources will need to be manually, periodically pruned.

* VM Instances
  * [https://console.cloud.google.com/compute/instances?project=deephaven-oss](https://console.cloud.google.com/compute/instances?project=deephaven-oss)
* VM Images
  * [https://console.cloud.google.com/compute/images?tab=images&project=deephaven-oss](https://console.cloud.google.com/compute/images?tab=images&project=deephaven-oss)
* Deployed Docker Images
  * [https://console.cloud.google.com/artifacts/docker/deephaven-oss/us-central1/deephaven?project=deephaven-oss](https://console.cloud.google.com/artifacts/docker/deephaven-oss/us-central1/deephaven?project=deephaven-oss)