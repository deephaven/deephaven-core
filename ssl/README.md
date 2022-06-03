# ssl

The projects under [ssl/](.) are meant to service the needs of clients and servers with respect to Deephaven SSL.

### config

[ssl-config](config) serves as the configuration layer. The configuration objects are
[immutable](https://immutables.github.io/) and can be constructed via builders or can be parsed via
[jackson](https://github.com/FasterXML/jackson).

### kickstart

[ssl-kickstart](kickstart) serves as an implementation/adapter role, guided by
[sslcontext-kickstart](https://github.com/Hakky54/sslcontext-kickstart). This handles the translation from the 
configuration objects into JDK-native SSL objects, which can then be used in the construction of specific client or
server libraries. 
