# Deephaven Demo

The Deephaven demo system  
consists of a controller machine  
running at [https://demo.deephaven.app](https://demo.deephaven.app)  
which hands out pre-provisioned workers  
to each client who visits the demo URL.  

Each worker is tagged with lease metadata  
and is decommissioned after 45 minutes.

These workers use a pre-baked "server-all" image  
which contains many popular python frameworks  
and multiple example notebooks detailing their use.

This demo uses gcloud to manage a fleet of workers.  

If you do not have a Deephaven demo cluster yet,  
follow the [Setup Guide](setup/README.md).

If you have a demo cluster already,  
and want to create a worker or controller,  
follow the [Deployment Guide](deploy/README.md).

If you are a contributor looking for interesting code,  
have a look at the [controller code](controller),  
in particular the [ClusterController](src/main/java/io/deephaven/demo/ClusterController.java) class.  

For [deployment code](deploy),  
start with the [ImageDeployer] class  
and see how it is used in the [gradle script].