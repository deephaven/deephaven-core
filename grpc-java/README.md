# grpc-java 

This directory contains code licensed under the Apache License, Version 2.0.

## grpc-servlet-jakarta
The grpc-servlet-jakarta directory is from https://github.com/grpc/grpc-java/pull/8596, 
a pull request to add a Java Servlet based transport. This version only supports 
Jakarta Servlets, and is copied from the generated sources created by that pull
request, to best support the newest versions of Jetty. There is also one small
change to allow the ServletAdapter to expose internal details to other transports,
like grpc-servlet-web or grpc-servlet-websocket, to allow avoiding using an
external proxy. 

## grpc-servlet-websocket-jakarta
The grpc-servlet-websocket-jakarta project is new, not yet submitted to grpc-java for
discussion or review. In short, it is a grpc-java transport, which uses one websocket
per stream based on the https://github.com/improbable-eng/grpc-web/ client/proxy
implementation. This enables a browser-based client to connect to a bidirectional
binary stream without SSL or an intermediate proxy.

## grpc-servlet-web-jakarta
There will be another project here soon, for a grpc-web implementation, allowing
a browser-based client to connect to server-streaming or unary calls with text or binary
payloads, without an intermediate proxy. This will require SSL to use http2, and, until
browsers support it, will not be able to handle any form of client streaming.
