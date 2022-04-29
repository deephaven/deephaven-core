# Deploying Deephaven Clusters

This guide will help you understand how to create Deephaven workers (long-lived machines)  
and controllers (one long-lived machine which supplies short-lived machines to users).

Creating both types of machines requires you to have already followed the complete [setup guide](../setup/README.md).

To simplify example scripts, make sure to set up your terminal environment with the following variables  
(you may wish to customize to your target cluster / google project in your .profile).

```shell
PROJECT_ID="${PROJECT_ID:-deephaven-oss}"
REGION="${REGION:-us-central1}"
ZONE="${ZONE:-us-central1-f}"
CLUSTER_NAME="${CLUSTER_NAME:-dhce-auto}"
```

You _may_ also wish to change your default gcloud project or region,  
so if you forget to add --project, --region or --zone flags,  
you will still be operating on your deephaven cluster.

```shell
gcloud config set compute/region "$REGION"
gcloud config set compute/zone "$ZONE"
gcloud config set project "$PROJECT_ID"
```


## User Requirements

In order to allow a user to deploy code to Google VMs,  
that user will need to have the [gcloud program installed](https://cloud.google.com/sdk/docs/install),  
upload a public ssh key to your google project's metadata,  
and have IAM permissions / credentials created (from the setup guide).

Assuming a project ID of deephaven-oss,  
the URL to add public SSH keys is  
[https://console.cloud.google.com/compute/metadata?tab=sshkeys&project=deephaven-oss](https://console.cloud.google.com/compute/metadata?tab=sshkeys&project=deephaven-oss)

Ensure the user has the necessary IAM permissions:  
Go to [https://console.cloud.google.com/iam-admin/iam?project=deephaven-oss](https://console.cloud.google.com/iam-admin/iam?project=deephaven-oss).  
Edit/create a principal for your user's email address.  
Grant them the following roles:  
`Demo Admin Role`  
`Demo Controller Role`

Make sure to click save.  
If the roles do not exist, follow the [Setup Guide](../setup/README.md).


Next, have user run `gcloud auth login` from their shell,  
to download the necessary credentials to invoke gcloud.  

```shell
gcloud auth login --project "$PROJECT_ID"
```

## Deploying Machines

To create a single, long-lived worker machine:  
`./gradlew deployMachine -Phostname=any-machine-name`
* The default invocation will give you a server-all-ai (very large and slow)
* Pass -PserverType= to build a smaller image. Empty value == basic deephaven instance
* Valid values for serverType are any server-prefixed directories in [docker/src/main](../../docker/src/main)
  * all-ai, nltk, pytorch, sklearn, tensorflow or empty string
* Pass -PrebuildWorker=true to delete and rebuild the worker from scratch

To create a new controller (production server) instance:  
`./gradlew deployDemo`
* If you are redeploying without a version bump, pass -Pforce=true
* If you just want to rebuild the worker image, pass -PworkerOnly=true
* Do NOT redeploy production controller without a version bump
  * ssh demo.deephaven.app and check the `hostname` command in terminal

To quickly look up these tasks' flags:  
`./gradlew :demo-deploy:tasks`

Note that long-running workers use a mangled version  
which will include your git branch name.  
This lets controllers know to ignore your machine,  
plus it helps to figure out where machines came from,  
so we can ask the owner to delete them.

In python, checking `deephaven.__version__` will display this mangled version.  
You can also read env variables `DEEPHAVEN_VERSION` or just `VERSION`.

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
[https://console.cloud.google.com/net-services/dns/zones/deephaven-app/rrsets/demo.deephaven.app./A/view?project=deephaven-oss](https://console.cloud.google.com/net-services/dns/zones/deephaven-app/rrsets/demo.deephaven.app./A/view?project=deephaven-oss)

Once the new controller notices the change in DNS records,  
it will take over modifying the cluster.

Once the old controller realizes it is no longer the leader,  
It will stop modifying the cluster, and you should shut it down.

Turn off the old controller in the web console:  
[https://console.cloud.google.com/compute/instances?project=deephaven-oss](https://console.cloud.google.com/compute/instances?project=deephaven-oss).  

It is recommended to keep 1-2 old controller instances  
and delete anything you know you won't need again.


If a promoted controller fails to clean up old nodes properly,  
(give it at least 10 minutes for a ~20 node cluster)  
first, open a ticket, with attached logs from the controller machine  
(see Troubleshooting section for grabbing logs)  
and manually delete any old instances.

To manually delete old controller instances, or old workers   
visit your gcloud instance list (currently at url):  
[https://console.cloud.google.com/compute/instances?project=deephaven-oss](https://console.cloud.google.com/compute/instances?project=deephaven-oss).  
You can use the filter bar, searching for `dh-version:<oldversion>`,  
but BE CAREFUL DELETING FROM GOOGLE'S UI:  
it will show you prefixed matches, so you might wipe out a developer machine.  
Click the "Labels" drop down and verify the dh-version is what you expect.

I only use the UI to discover what versions are available.  
Only the command line can give an accurate list for a version:  
```shell
version=1-2-3
gcloud compute instances list --project deephaven-oss \
    --filter "Labels.dh-version<=$version AND Labels.dh-version>=$version"
```

Once you are satisfied with the list of machines to delete, kill them:
```shell
version=1-2-3
gcloud compute instances list --project deephaven-oss \
    --filter "Labels.dh-version<=$version AND Labels.dh-version>=$version" \
    --format 'value(NAME,ZONE)' |
    xargs -P 8 -I{} bash -c '
    set -e
    name="$(echo "$1" | awk "{print \$1}")"
    zone="$(echo "$1" | awk "{print \$2}")"
    echo "Deleting $name"
    echo gcloud compute instances delete --project deephaven-oss --quiet \
       --zone "$zone" "$name"
    echo "Deleted $name"
    ' -- {}
```

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

You may also older logs on the machine in /dh/logs directory.  
This is particularly true on the production controller instance.