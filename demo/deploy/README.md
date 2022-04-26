# Deploying Deephaven Clusters

This guide will help you understand how to create Deephaven workers (long-lived machines) and controllers (one long-lived machine which supplies short-lived machines to users).

Creating both types of machines requires you to have already followed the complete [setup guide](../setup/README.md).

To simplify example scripts, make sure to set up your terminal environment with the following variables (customized to your target cluster / google project).

```shell
PROJECT_ID="${PROJECT_ID:-deephaven-community}"
REGION="${ZONE:-us-central1}"
CLUSTER_NAME="${CLUSTER_NAME:-dh-ctrl}"
K8NS="${K8NS:-dh}"
K8_SRV_ACT="${K8_SRV_ACT:-dhadmin}"
GCE_SRV_ACT="${GCE_SRV_ACT:-dhadmin}"
```

You _may_ also wish to change your default gcloud project or region,  
so if you forget to add --project or --region flags,  
you will still be operating on your deephaven cluster.

```shell
gcloud config set compute/region "$REGION"
gcloud config set project "$PROJECT_ID"
```



## User Requirements

In order to allow a user to deploy code to Google VMs,  
that user will need to have the [gcloud program installed](https://cloud.google.com/sdk/docs/install),  
upload a public ssh key to your google project's metadata,  
and have IAM permissions / credentials created in the setup guide.

Assuming a project ID of deephaven-community,  
the URL to add public SSH keys is  
[https://console.cloud.google.com/compute/metadata?tab=sshkeys&project=deephaven-community](https://console.cloud.google.com/compute/metadata?tab=sshkeys&project=deephaven-community)

Ensure the user has the necessary IAM permissions:  
Go to [https://console.cloud.google.com/iam-admin/iam?project=deephaven-community](https://console.cloud.google.com/iam-admin/iam?project=deephaven-community).  
Edit/create a principal for your user's email address.  
Grant them the following roles:  
`Demo Admin Role`  
`Demo Controller Role`

Make sure to click save.  
If the roles do not exist, follow the [setup guide](../setup/README.md).


Next, have user run `gcloud auth login` from their shell,  
to download the necessary credentials to invoke gcloud.  

```shell
gcloud auth login --project "$PROJECT_ID"
```

## Deploying Machines

To create a single, long-lived worker machine:  
`./gradlew deployMachine -Phostname=any-machine-name`

To create a new controller instance:  
`./gradlew deployDemo`

For additional information about these tasks,  
`./gradlew :demo-deploy:tasks`

Note that long-running workers use a mangled version  
which will include your git branch name.  
This lets controllers know to ignore your machine,  
plus it helps to figure out where machines came from.

# Deploying production

Before you deploy production, you must bump versions.

Edit [io.deephaven.java-common-conventions.gradle](../../buildSrc/src/main/groovy/io.deephaven.java-common-conventions.gradle).

Changing just this one file is enough for all code to be correct  
but we currently find/replace this version string using IDE/grep.  
It can be dangerous to blindly change versions, however,  
as many npm packages may randomly have same semver as us.

Once you bump versions, you are ready to create a new controller.

Run `./gradlew deployDemo` until it succeeds.  
You will test the new controller by visiting its url,  
which will be printed to your console logs and look like:  
`controller-1-2-3.demo.deephaven.app`.

The new controller should send you to a worker within a minute.

Test that worker functions (run some notebook commands). 

Finally, update the DNS record for the main demo site.  
You should point your domain to the new controller's IP.     
The console _will_ print the new controller's IP address,  
or you can find it using the dig command:  
`dig +short controller-1-2-3.demo.deephaven.app`.

The DNS record is updated using the url:  
[https://console.cloud.google.com/net-services/dns/zones/deephaven-app/rrsets/demo.deephaven.app./A/view?project=deephaven-community](https://console.cloud.google.com/net-services/dns/zones/deephaven-app/rrsets/demo.deephaven.app./A/view?project=deephaven-community)

Once the new controller notices the change in DNS records,  
it will take over modifying the cluster.

Once the old controller realizes it is no longer the leader,  
It will stop modifying the cluster, and you should shut it down.

Turn off the old controller in the web console:  
[https://console.cloud.google.com/compute/instances?project=deephaven-community](https://console.cloud.google.com/compute/instances?project=deephaven-community).  

It is recommended to keep 1-2 old controller instances  
and delete anything you know you won't need again.


If a promoted controller fails to clean up old nodes properly,  
(give it at least 10 minutes for a ~20 node cluster)  
and open a ticket, with attached logs from the controller machine  
(see Troubleshooting section for grabbing logs)  
and manually delete the old instances.

To manually delete old controller instance, or old workers   
visit your gcloud instance list (currently at url):  
[https://console.cloud.google.com/compute/instances?project=deephaven-community](https://console.cloud.google.com/compute/instances?project=deephaven-community).  
You can use the filter bar, searching for `Labels.dh-version:<oldversion>`,  
then select all matching machines and click Delete.

If you don't type any version, (just `Labels.dh-version:`),  
autocomplete will show you a list of all live versions.

## Troubleshooting

If gcloud operations fail with any strange errors,    
you may want to try running `gcloud components update`,  
or use your system package manager to update your gcloud version.

### Grabbing logs

To extract logs from your machine:

```
ssh "${DOMAIN:-demo.deephaven.app}" '
  cd /dh &&
    sudo docker compose logs
  ' > /tmp/dh.log
# upload / read log in /tmp/dh.log.
 ```

The log size may be very large.  
Most interesting information will be at the head/tail of logs,  
or found using grep, less or your favorite text search tool  
(in less, `q` to exit, `/searchString<enter>` to search).