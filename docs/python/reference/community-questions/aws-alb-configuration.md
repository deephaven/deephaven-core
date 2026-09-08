---
title: How do I run Deephaven behind an AWS Application Load Balancer?
sidebar_label: How do I run Deephaven behind AWS ALB?
---

_I'm running Deephaven behind an AWS Application Load Balancer (ALB) on Kubernetes. When I try to access the IDE, I get an HTTP 500 error with a `NullPointerException` mentioning "host" is null. How do I fix this?_

This error typically occurs when the ALB does not forward the `X-Forwarded-Host` header, which Jetty's `ForwardedRequestCustomizer` expects. The error looks like:

```
java.lang.NullPointerException: Cannot invoke "String.equalsIgnoreCase(String)" because "host" is null
    at org.eclipse.jetty.server.ForwardedRequestCustomizer.customize(ForwardedRequestCustomizer.java:543)
```

## Root cause

AWS ALB sends `X-Forwarded-For` but historically has not sent a spec-compliant `X-Forwarded-Host` header. Deephaven's embedded Jetty server uses `ForwardedRequestCustomizer` to process these headers, and it expects either a valid `X-Forwarded-Host` or a complete, spec-compliant `X-Forwarded-For` value.

Additionally, [ALB defaults to HTTP/1.1](https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-target-groups.html#target-group-protocol-version) for backend connections. Deephaven requires HTTP/2 for gRPC communication.

## Solution: Use an nginx sidecar

The recommended solution is to deploy an [nginx](https://nginx.org/en/docs/http/ngx_http_proxy_module.html) sidecar container that sits between ALB and Deephaven. The nginx proxy handles the forwarded headers and protocol translation properly.

### Why this works

- **Header normalization**: nginx can add the missing `X-Forwarded-Host` header.
- **Protocol handling**: nginx can terminate HTTP/1.1 from ALB and forward HTTP/2 to Deephaven.
- **Proven pattern**: This mirrors how Deephaven Enterprise uses Envoy in production with ALB.

## ALB configuration notes

If you're configuring ALB directly (without a sidecar), ensure you're using the correct backend protocol. For gRPC-based applications like Deephaven, you need:

```yaml
alb.ingress.kubernetes.io/backend-protocol-version: "GRPC"
```

Not:

```yaml
# This will not work with Deephaven
alb.ingress.kubernetes.io/backend-protocol-version: "HTTP1"
```

See the [AWS documentation on gRPC with ALB](https://docs.aws.amazon.com/prescriptive-guidance/latest/patterns/deploy-a-grpc-based-application-on-an-amazon-eks-cluster-and-access-it-with-an-application-load-balancer.html) for more details.

## Related configuration

The `proxy.hint` property in `deephaven.prop` controls some internal configuration choices:

- Whether HTTP/1.1 should ever be supported.
- Whether WebSocket support should be enabled.

When a proxy is present, `proxy.hint=true` tells Deephaven to disable HTTP/1.1 and WebSocket support, since the proxy handles TLS and forwards h2 or h2c connections.

## Related documentation

- [Configure Deephaven to use another port](configure-dh-to-use-another-port.md)
- [Why can't I read from S3?](aws-timeout.md)

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
