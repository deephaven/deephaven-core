/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/**
 * SSL configuration can range from relatively straightforward to relatively complex. In an effort to provide a common
 * configuration layer among multiple server and client implementations, {@link io.deephaven.ssl.config.SSLConfig} is
 * (hopefully) easier to use than the Java native SSL configuration layers, while also providing deserialization from
 * JSON. It is meant to service the majority of server and client SSL configuration use-cases.
 *
 * <p>
 * While not exposed to the end-user, the overall configuration structure is guided by
 * <a href="https://github.com/Hakky54/sslcontext-kickstart">sslcontext-kickstart</a>.
 */
package io.deephaven.ssl.config;
