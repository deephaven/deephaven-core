# deephaven-uri

The deephaven-uri package canonicalizes a [URI](https://en.wikipedia.org/wiki/Uniform_Resource_Identifier) format for
[Deephaven target](src/main/java/io/deephaven/uri/DeephavenTarget.java) and
[resource](src/main/java/io/deephaven/uri/StructuredUri.java) identification.
The goal is to provide a string format that can be easy referenced and shared across client and server contexts.

### Deephaven target

The Deephaven target format allows for a concise representation of connection information:

| URI                           | Secure | Host            | Port          |
|------------------------------ | ------ | --------------- | ------------- |
| `dh://example.com`            | yes    | `example.com`   | (unspecified) |
| `dh://example.com:8888`       | yes    | `example.com`   | 8888          |
| `dh+plain://example.com`      | no     | `example.com`   | (unspecified) |
| `dh+plain://example.com:8888` | no     | `example.com`   | 8888          |

When no port is given, it's up to the client implementation to choose the correct port.

External clients are encouraged to use the Deephaven target URI format when applicable for consistency:

```shell
$ ./example-third-party-app-list-tables --target dh://example.com
```

### Resource

The resource format allows for a concise representation of resource identification, both local and remote.

| URI                                               | Deephaven target                               | Type                    | Type-specific information                        |
|-------------------------------------------------- | ---------------------------------------------- | ----------------------- | ------------------------------------------------ |
| `dh:///scope/my_table`                            | (none)                                         | scope                   | variableName=`my_table`                          |
| `dh://example.com/scope/my_table`                 | `dh://example.com`                             | remote + scope          | variableName=`my_table`                          |
| `dh:///app/my_app/field/my_field`                 | (none)                                         | app                     | applicationId=`my_app` fieldName=`my_field`      |
| `dh://example.com/app/my_app/field/my_field`      | `dh://example.com`                             | remote + app            | applicationId=`my_app` fieldName=`my_field`      |
| `dh://example.com/field/my_field`                 | `dh://example.com`                             | remote + field          | applicationId=`example.com` fieldName=`my_field` |
| `dh://gateway?uri=dh://host/scope/my_table`       | `dh://gateway` (local) + `dh://host` (gateway) | remote + remote + scope | variableName=`my_table`                          |
| `custom:///foo?bar=baz`                           | (none)                                         | custom                  | (custom)                                         |
| `dh://host?uri=custom%3A%2F%2F%2Ffoo%3Fbar%3Dbaz` | `dh://host`                                    | remote + custom         | (custom)                                         |

[Deephaven URIs](src/main/java/io/deephaven/uri/DeephavenUri.java) are those that have the scheme `dh` and no host,
or `dh`/`dh+plain` and a host; and represent Deephaven-specific concepts. Unless configured otherwise, Deephaven servers
should be able to resolve Deephaven URIs into appropriate resources.

[Custom URIs](src/main/java/io/deephaven/uri/CustomUri.java) are any other URI, whereby the server may or may not be
able to resolve them into resources. This allows for third parties to define and configure URI resolution for their own
needs. It is also a place where Deephaven may experiment with other schemes and URIs that aren't necessarily safe in a
general context, or as a staging ground for later incorporation into a Deephaven URI.
