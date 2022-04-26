# Deephaven Demo

The Deephaven demo system  
uses a pre-baked "server-all" image  
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